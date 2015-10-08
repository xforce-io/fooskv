#pragma once

#include "../public.h"

namespace xforce { namespace fooskv {

struct DeviceCmd {
  uint16_t code;
};

static const DeviceCmd kUninit = {0};
static const DeviceCmd kCreateTable = {1};
static const DeviceCmd kDropTable = {2};
static const DeviceCmd kAdd = {3};
static const DeviceCmd kRemove = {4};

typedef size_t KeyHash;

inline KeyHash GenKeyHash(const Slice& key) {
  return Hash::MurmurHash64(key.Data(), key.Size());
}

typedef int Index;
typedef int Offset;

struct DevicePos {
 public:
  Index index;
  Offset offset;

 public:
  inline bool Serialize(FILE* fp) const;
  inline bool Deserialize(FILE* fp);
};

bool DevicePos::Serialize(FILE* fp) const {
  return 0 != fprintf(fp, "%d\t%d", index, offset);
}

bool DevicePos::Deserialize(FILE* fp) {
  return 0 != fscanf(fp, "%d\n%d", &index, &offset);
}

}}
