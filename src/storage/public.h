#pragma once

#include "public/public.h"
#include "limits_local.h"

namespace xforce { namespace fooskv {

typedef JsonType Config;
typedef LogicTime time_t;

enum ErrNo {
  kOK = 0,
  kOther = 100,
};

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
