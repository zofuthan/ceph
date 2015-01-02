// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "common/ceph_argparse.h"
#include "common/errno.h"

#include "TableTool.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

void TableTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-table-tool [options] reset <session>"
    << "\n"
    << "Options:\n"
    << "  --rank=<int>      MDS rank (default: operate on all ranks)\n";

  generic_client_usage();
}


int TableTool::main(std::vector<const char*> &argv)
{
  int r;

  dout(10) << __func__ << dendl;

  // Parse --rank
  std::vector<const char*>::iterator arg = argv.begin();
  std::string rank_str;
  if(ceph_argparse_witharg(argv, arg, &rank_str, "--rank", (char*)NULL)) {
    std::string rank_err;
    rank = strict_strtol(rank_str.c_str(), 10, &rank_err);
    if (!rank_err.empty()) {
        derr << "Bad rank '" << rank_str << "'" << dendl;
        usage();
    }
  }

  // Require 2 args <action> <table>
  if (argv.size() < 2) {
    usage();
    return -EINVAL;
  }

  // RADOS init
  // ==========
  r = rados.init_with_context(g_ceph_context);
  if (r < 0) {
    derr << "RADOS unavailable, cannot scan filesystem journal" << dendl;
    return r;
  }

  dout(4) << "connecting to RADOS..." << dendl;
  rados.connect();
 
  int const pool_id = mdsmap->get_metadata_pool();
  dout(4) << "resolving pool " << pool_id << dendl;
  std::string pool_name;
  r = rados.pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    derr << "Pool " << pool_id << " named in MDS map not found in RADOS!" << dendl;
    return r;
  }

  dout(4) << "creating IoCtx.." << dendl;
  r = rados.ioctx_create(pool_name.c_str(), io);
  assert(r == 0);

  const std::string mode = std::string(*arg);
  arg = argv.erase(arg);

  if (mode == "reset") {
    const std::string table = std::string(*arg);
    if (table == "session") {
      return reset_session_table();
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
      return -EINVAL;
    }
  } else {
    derr << "Invalid mode '" << mode << "'" << dendl;
    usage();
    return -EINVAL;
  }
}



/**
 * Clear a specific MDS rank's session table
 */
int TableTool::_reset_session_table(mds_rank_t rank)
{
  int r = 0;

  // Compose object ID
  bufferlist sessiontable_bl;
  std::ostringstream oss;
  oss << "mds" << rank << "_sessionmap";
  const std::string sessiontable_oid = oss.str();

  // Attempt to read the sessiontable version and set new_version
  // to one ahead of it.
  r = io.read(sessiontable_oid, sessiontable_bl, (1<<22), 0);
  version_t new_version;
  if (r == -ENOENT) {
    new_version = 1;
    dout(4) << "missing sessionmap object " << sessiontable_oid
      << ", rewriting at version " << new_version << dendl;
  } else if (r >= 0) {
    bufferlist::iterator q = sessiontable_bl.begin();
    uint64_t pre;
    try {
      ::decode(pre, q);
      if (pre == (uint64_t)-1) {
        // 'new' format
        DECODE_START_LEGACY_COMPAT_LEN(3, 3, 3, q);
        ::decode(new_version, q);
        new_version++;
        DECODE_FINISH(q);
      } else {
        // 'old' format
        new_version = pre + 1;
      }
    } catch (buffer::error &e) {
      derr << "sessionmap " << sessiontable_oid << " is corrupt: '" << e.what()
        << "' and will be rewritten at version 1" << dendl;
      new_version = 1;
    }
  } else {
    dout(0) << "error reading sessionmap object " << sessiontable_oid
      << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  // Compose new sessiontable
  dout(4) << "writing blank sessiontable with version " << new_version << dendl;
  bufferlist new_bl;
  ::encode((uint64_t)-1, new_bl);
  ENCODE_START(3, 3, new_bl);
  ::encode(new_version, new_bl);
  ENCODE_FINISH(new_bl);

  // Write out new sessiontable
  r = io.write_full(sessiontable_oid, new_bl);
  if (r != 0) {
    derr << "error writing sessionmap object " << sessiontable_oid
      << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  return r;
}

/**
 * Clear one or all MDSs' session tables depending
 * on TableTool::rank
 */
int TableTool::reset_session_table()
{
  if (rank == MDS_RANK_NONE) {
    int r = 0;
    std::set<mds_rank_t> in_ranks;
    mdsmap->get_mds_set(in_ranks);

    for (std::set<mds_rank_t>::iterator rank_i = in_ranks.begin();
        rank_i != in_ranks.end(); ++rank_i)
    {
      int rank_r = _reset_session_table(*rank_i);
      r = r ? r : rank_r;
    }

    return r;
  } else {
    return _reset_session_table(rank);
  }
}

