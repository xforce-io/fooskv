#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class TableIndex {
 public:
  TableIndex();

  bool Init(
      const Config& config, 
      NoTable no_table, 
      const std::string& name_table,
      size_t num_buckets); 

  inline ErrNo Add(KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(KeyHash key_hash);

  bool Dump();

  virtual ~TableIndex();
 
 private:
  static size_t GetBucket_(KeyHash key_hash);

 private: 
  size_t num_buckets_;
  TableIndexBucket** table_index_buckets_;
};

}}

namespace xforce { namespace fooskv {

ErrNo TableIndex::Add(KeyHash key_hash, DevicePos device_pos) {
  table_index_buckets_[GetBucket_(key_hash)]->Add(key_hash, device_pos);
}

ErrNo TableIndex::Remove(KeyHash key_hash) {
  table_index_buckets_[GetBucket_(key_hash)]->Remove(key_hash);
}

size_t TableIndex::GetBucket_(KeyHash key_hash) {
  return key_hash % num_buckets_;
}

}}
