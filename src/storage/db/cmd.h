#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

struct Cmd {
  CmdHeader();

  Cmd no_cmd;
  LogicTime start_logic_time;
  ErrNo g_errno;
  union {
    CmdCreateTable cmd_create_table;
    CmdDropTable cmd_drop_table;
    CmdModify cmd_modify;
  } body;

  bool endOfWriteCmd;
};

struct CmdCreateTable {
  static const Cmd::TypeCmd kCmd = Cmd::kCreateTable;

  NoTable no_table;
};

struct CmdDropTable {
  static const Cmd::TypeCmd kCmd = Cmd::kDropTable;

  NoTable no_table;
};

struct CmdModify {
  static const Cmd::TypeCmd kCmd = Cmd::kModify;

  enum Category {
    kAdd,
    kRemove,
    kUpdate,
  } category;

  struct Req {
    NoTable no_table;
    const KVBatch* kv_batch;
  } req;

  struct Resp {
    ErrNo errno[Limits::kMaxNumBatch]; 
  } resp;
};

}}
