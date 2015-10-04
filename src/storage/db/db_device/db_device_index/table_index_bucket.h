#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class TableIndexBucket {
 private:
  typedef SpinLock Lock;
  typedef MultiCowBtreeMap<KeyHash, DevicePos> Container;
  
 public:
  TableIndexBucket(const Config& config, NoTable no_table, size_t no_bucket);

  inline ErrNo Add(KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(KeyHash key_hash);

  bool Dump();

  virtual ~TableIndexBucket();

 private:
  const Config& config_;
  NoTable no_table_;
  size_t no_bucket_; 

  Lock lock_;
  MultiCowBtreeMap* index_;
};

}}

namespace xforce { namespace fooskv {

ErrNo TableIndexBucket::Add(KeyHash key_hash, DevicePos device_pos) {
  WAIT_UNTIL_TRUE(lock_.Lock(), "table_index_lock", )

  bool ret = index_->Insert(key_hash, device_pos).first;
  lock_.Unlock();

  if (ret) {
    return kOK;
  } else {
    FATAL("fail_insert_into_table_index_bucket");
    return kOther;
  }
}

ErrNo TableIndexBucket::Remove(KeyHash key_hash) {
  WAIT_UNTIL_TRUE(lock_.Lock(), "table_index_lock", )
  bool ret = index_->Erase(key_hash);
  lock_.Unlock();

  if (ret) {
    return kOK;
  } else {
    FATAL("fail_erase_from_table_index_bucket")
    return kOther;
  }
}

}}
