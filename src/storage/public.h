#pragma once

#include "public/public.h"
#include "limits_local.h"

namespace xforce { namespace fooskv {

typedef JsonType Config;
typedef LogicTime time_t;
typedef NoTable uint16_t;

struct Cmd {
  uint16_t code;
};

static const Cmd kCreateTable = {1};
static const Cmd kDropTable = {2};
static const Cmd kModify = {3};

struct ErrNo {
  uint16_t code;
};

static const ErrNo kOK = {0};
static const ErrNo kTableNotExist = {1};
static const ErrNo kKeyNotExist = {2};
static const ErrNo kOther = {100};

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
