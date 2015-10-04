#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class DBDeviceIndex {
 private:
  typedef std::unordered_map<NoTable, TableIndex*> TableIndexes;
  
 public: 
  DBDeviceIndex();

  bool Init(const Config& config);

  ErrNo CreateTable(NoTable no_table, size_t num_buckets);
  ErrNo DropTable(NoTable no_table);
  inline ErrNo Add(NoTable no_table, KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(NoTable no_table, KeyHash key_hash);

  virtual ~DBDeviceIndex();
 
 private: 
  inline ErrNo Modify_(bool is_add, NoTable no_table, KeyHash key_hash, DevicePos device_pos);

 private:
  TableIndexes table_indexes_; 

  //cache
  NoTable cache_no_table_;
  TableIndex* cache_table_index_;
};

ErrNo DBDeviceIndex::Add(NoTable no_table, KeyHash key_hash, DevicePos device_pos) {
  return Modify_(true, no_table, key_hash, device_pos);
}

ErrNo DBDeviceIndex::Remove(NoTable no_table, KeyHash key_hash) {
  static const DevicePos tmp_device_pos = (struct DevicePos){0, 0};
  return Modify_(false, no_table, key_hash, tmp_device_pos);
}

ErrNo DBDeviceIndex::Modify_(
    bool is_add, 
    NoTable no_table, 
    KeyHash key_hash, 
    DevicePos device_pos) {
  if (cache_no_table_ == no_table) {
    return is_add ? cache_table_index_->Add(key_hash, device_pos)
        : cache_table_index_->Remove(key_hash);
  }

  TableIndex::iterator iter = table_indexes_->find(key_hash);
  if (iter != table_indexes_.end()) {
    cache_no_table_ = no_table;
    cache_table_index_ = *iter;
    return is_add ? iter->Add(key_hash, device_pos) : iter->Remove(key_hash);
  } else {
    return kTableNotExist;
  }
}

}}
