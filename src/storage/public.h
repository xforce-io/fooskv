#pragma once

#include "public/public.h"
#include "limits_local.h"

namespace xforce { namespace fooskv {

typedef JsonType Config;
typedef LogicTime time_t;
typedef NoTable uint16_t;

struct Cmd {
  typedef uint16_t TypeCmd;

  static const TypeCmd kCreateTable = 1;
  static const TypeCmd kDropTable = 2;
  static const TypeCmd kModify = 3;

  TypeCmd code;
};

struct ErrNo {
  typedef uint16_t TypeErrNo;

  static const TypeErrNo kOK = 0;
  static const TypeErrNo kOther = 100;

  TypeErrNo code;
}

struct KV {
  Slice Key;
  Slice val;
};

struct KeyBatch {
 public: 
  Slice* keys;
  size_t num;

 public:
  inline KeyBatch();
  ~KeyBatch();
};

struct KVBatch {
 public:
  KV* kvs;
  size_t num;

 public:
  inline KVBatch();
  ~KVBatch();
};

KeyBatch::KeyBatch() {
  XFC_NEW(keys, Slice [Limits::kMaxNumKeyBatch])
}

KeyBatch::~KeyBatch() {
  XFC_DELETE_ARRAY(keys)
}

KVBatch::KVBatch() {
  XFC_NEW(kvs, KV [Limits::kMaxNumKVBatch])
}

KVBatch::~KVBatch() {
  XFC_DELETE_ARRAY(kvs)
}

}}
