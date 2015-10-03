#pragma once

#include "../public.h"

namespace xforce { namespace fooskv {

struct DeviceCmd {
  enum Cmd {
    kUninit,
    kCreateTable,
    kDropTable,
    kAdd,
    kRemove,
    kNumCmd,
  };
};

typedef size_t KeyHash;

inline KeyHash GenKeyHash(NoTable no_table, const Slice& key) {
  return Hash::MurmurHash64(key.Data(), key.Size()) ^ (no_table << 48);
}

typedef int Index;
typedef off_t Offset;

struct DevicePos {
  Index index;
  Offset offset;
};

}}
