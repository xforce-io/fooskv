#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class DBDevice {
 public:
  DBDevice();

  bool Init(const Config& config);

  ErrNo Add(NoTable no_table, const KV& kv, LogicTime logic_time);
  ErrNo Remove(NoTable no_table, const KVB& kv, LogicTime logic_time);

  virtual ~DBDevice();
 
 private: 
  DBDeviceDir* db_device_dir_;
  DBDeviceWriteHandle* write_handle_;
};

}}
