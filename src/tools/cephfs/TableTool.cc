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

#include "mds/SessionMap.h"
#include "mds/InoTable.h"
#include "mds/SnapServer.h"

#include "TableTool.h"


#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << __func__ << ": "

void TableTool::usage()
{
  std::cout << "Usage: \n"
    << "  cephfs-table-tool [options] <reset|show> <session|snap|inode>"
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

  // Require 2 args <action> <table>
  if (argv.size() < 2) {
    usage();
    return -EINVAL;
  }
  const std::string mode = std::string(argv[0]);
  const std::string table = std::string(argv[1]);

  if (mode == "reset") {
    if (table == "session") {
      return apply_rank_fn(&TableTool::_reset_session_table, NULL);
    } else if (table == "inode") {
      return apply_rank_fn(&TableTool::_reset_ino_table, NULL);
    } else if (table == "snap") {
      return _reset_snap_table();
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
      return -EINVAL;
    }
  } else if (mode == "show") {
    JSONFormatter jf(true);
    jf.open_object_section("ranks");
    if (table == "session") {
      r = apply_rank_fn(&TableTool::_show_session_table, &jf);
    } else if (table == "inode") {
      r = apply_rank_fn(&TableTool::_show_ino_table, &jf);
    } else if (table == "snap") {
      r = _show_snap_table(&jf);
    } else {
      derr << "Invalid table '" << table << "'" << dendl;
      usage();
      return -EINVAL;
    }
    jf.close_section();

    // Subcommand should have written to formatter, flush it
    jf.flush(std::cout);
    std::cout << std::endl;
    return r;
  } else {
    derr << "Invalid mode '" << mode << "'" << dendl;
    usage();
    return -EINVAL;
  }
}



/**
 * Clear a specific MDS rank's session table
 */
int TableTool::_reset_session_table(mds_rank_t rank, Formatter *f)
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
 * For a function that takes an MDS rank as an argument and
 * returns an error code, execute it either on all ranks (if
 * this->rank is MDS_RANK_NONE), or on the rank specified
 * by this->rank.
 */
int TableTool::apply_rank_fn(int (TableTool::*fptr) (mds_rank_t, Formatter*), Formatter *f)
{
  int r = 0;
  if (rank == MDS_RANK_NONE) {
    std::set<mds_rank_t> in_ranks;
    mdsmap->get_mds_set(in_ranks);

    for (std::set<mds_rank_t>::iterator rank_i = in_ranks.begin();
        rank_i != in_ranks.end(); ++rank_i)
    {
      if (f) {
        std::ostringstream rank_str;
        rank_str << *rank_i;
        f->open_object_section(rank_str.str().c_str());
      }
      int rank_r = (this->*fptr)(*rank_i, f);
      r = r ? r : rank_r;
      if (f) {
        f->close_section();
      }
    }

    return r;
  } else {
    if (f) {
      std::ostringstream rank_str;
      rank_str << rank;
      f->open_object_section(rank_str.str().c_str());
    }
    r = (this->*fptr)(rank, f);
    if (f) {
      f->close_section();
    }
    return r;
  }
}


template <typename A>
class LoadAndDump
{
private:
  std::string object_name;
  bool mds_table;

public:
  LoadAndDump(mds_rank_t rank, std::string const &name, bool mds_table_)
  {
    // Compose object name of the table we will dump
    std::ostringstream oss;
    oss << "mds";
    if (rank != MDS_RANK_NONE) {
      oss << rank;
    }
    oss << "_" << name;
    object_name = oss.str();
    mds_table = mds_table_;
  }

  int load_and_dump(librados::IoCtx *io, Formatter *f)
  {
    assert(io != NULL);
    assert(f != NULL);

    // Attempt read
    bufferlist table_bl;
    int read_r = io->read(object_name, table_bl, (1<<22), 0);
    if (read_r >= 0) {
      bufferlist::iterator q = table_bl.begin();
      try {
        if (mds_table) {
          version_t version;
          ::decode(version, q);
          f->dump_int("version", version);
        }
        A table_inst;
        table_inst.decode(q);
        table_inst.dump(f);

        return 0;
      } catch (buffer::error &e) {
        derr << "table " << object_name << " is corrupt" << dendl;
        return -EIO;
      }
    } else {
      derr << "error reading table object " << object_name
        << ": " << cpp_strerror(read_r) << dendl;
      return read_r;
    }
  }

  int reset(librados::IoCtx *io)
  {
    A table_inst;
    table_inst.reset_state();
    
    // Compose new (blank) table
    bufferlist new_bl;
    table_inst.encode(new_bl);

    // Write out new table
    int r = io->write_full(object_name, new_bl);
    if (r != 0) {
      derr << "error writing table object " << object_name
        << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    return r;
  }
};


int TableTool::_show_session_table(mds_rank_t rank, Formatter *f)
{
  return LoadAndDump<SessionMapStore>(rank, "sessionmap", false).load_and_dump(&io, f);
}

int TableTool::_show_ino_table(mds_rank_t rank, Formatter *f)
{
  LoadAndDump<InoTable> dumper(rank, "inotable", true);
  return dumper.load_and_dump(&io, f);
}

int TableTool::_reset_ino_table(mds_rank_t rank, Formatter *f)
{
  LoadAndDump<InoTable> dumper(rank, "inotable", true);
  return dumper.reset(&io);
}

int TableTool::_show_snap_table(Formatter *f)
{
  LoadAndDump<SnapServer> dumper(MDS_RANK_NONE, "snaptable", true);
  return dumper.load_and_dump(&io, f);
}

int TableTool::_reset_snap_table()
{
  LoadAndDump<SnapServer> dumper(MDS_RANK_NONE, "snaptable", true);
  return dumper.reset(&io);
}


