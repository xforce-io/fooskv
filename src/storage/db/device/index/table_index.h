#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class TableIndex {
 private:
  typedef TableIndex Self;
  
 public:
  TableIndex();

  bool Init(
      const Config& config, 
      NoTable no_table, 
      const std::string& name_table,
      size_t num_buckets,
      bool* end,
      bool newly_created); 

  const DevicePos* GetReplayPos() const;

  inline ErrNo Add(KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(KeyHash key_hash);

  virtual ~TableIndex();
 
 private:
  void ActivateBucket_(size_t i);
  /*
   * @return :
   *    >  0 : end of recovery
   *    == 0 : ok
   *    <  0 : error happens
   */
  int RecoverBucket_();

  static void* Recovery_(void* arg);
  static size_t GetBucket_(KeyHash key_hash);

 private: 
  const Config* config_;
  NoTable no_table_;
  const std::string* name_table_;
  bool* end_;

  pthread_t tid_recovery_;

  size_t num_buckets_;
  TableIndexBucket** table_index_buckets_;

  std::queue<size_t> recovery_queue_;
  SpinLock lock_;
};

}}

namespace xforce { namespace fooskv {

ErrNo TableIndex::Add(KeyHash key_hash, DevicePos device_pos) {
  size_t bucket = GetBucket_(key_hash);
  if (!table_index_buckets_[bucket]->IsInit()) {
    ActivateBucket_(bucket);
  }
  table_index_buckets_[bucket]->Add(key_hash, device_pos);
}

ErrNo TableIndex::Remove(KeyHash key_hash) {
  size_t bucket = GetBucket_(key_hash);
  if (!table_index_buckets_[bucket]->IsInit()) {
    ActivateBucket_(bucket);
  }
  table_index_buckets_[bucket]->Remove(key_hash);
}

size_t TableIndex::GetBucket_(KeyHash key_hash) {
  return key_hash % num_buckets_;
}

}}
