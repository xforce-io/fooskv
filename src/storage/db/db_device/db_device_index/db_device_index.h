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
  ErrNo Add(KeyHash key_hash, DevicePos device_pos);
  ErrNo Remove(KeyHash key_hash);

  virtual ~DBDeviceIndex();

 private:
  TableIndexes table_indexes_; 
};

}}
