#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

struct DBMsg {
  DBMsgHeader();

  uint32_t no_cmd;
  LogicTime logic_time;

  ErrNo g_errno;
  union {
    AddKVMsg* add_kv_msg;
    RemoveKVMsg* remove_kv_msg;
  } body;
};

struct AddKVMsg {
  static const size_t kCmd = 1;

  struct Request {
    const KVBatch* kv_batch;
  };

  struct Response {
    ErrNo errno[Limits::kMaxNumBatch]; 
  };
};

struct RemoveKVMsg {
  static const size_t kCmd = 2;

  struct Request {
    const KeyBatch* key_batch;
  };

  struct Response {
    ErrNo errno[Limits::kMaxNumBatch]; 
  };
};

}}
