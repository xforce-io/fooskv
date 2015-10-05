#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class BucketDumper;

class TableIndexBucket {
 private:
  typedef SpinLock Lock;
  typedef MultiCowBtreeMap<KeyHash, DevicePos> Index;
  
 public:
  TableIndexBucket();

  bool Init(
      const Config& config, 
      NoTable no_table, 
      const std::string& name_table,
      size_t no_bucket,
      bool* end);

  inline ErrNo Add(KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(KeyHash key_hash);

  bool Dump();

  virtual ~TableIndexBucket();
 
 private:
  const Config* config_;
  NoTable no_table_;
  const std::string* name_table_;
  size_t no_bucket_; 

  Lock lock_;
  Index* index_;

  BucketDumper* bucket_dumper_;
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
