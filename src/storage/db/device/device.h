#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class Device {
 public:
  Device();

  bool Init(const Config& config);

  ErrNo Add(NoTable no_table, const KV& kv, LogicTime logic_time);
  ErrNo Remove(NoTable no_table, const KVB& kv, LogicTime logic_time);

  virtual ~Device();
 
 private: 
  DeviceDir* db_device_dir_;
  DeviceWriteHandle* write_handle_;
};

}}
