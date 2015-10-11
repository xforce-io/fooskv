#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class Device {
 public:
  Device();

  bool Init(const Config& config, bool* end);

  ErrNo Add(NoTable no_table, const KV& kv, LogicTime logic_time);
  ErrNo Remove(NoTable no_table, const KVB& kv, LogicTime logic_time);

  virtual ~Device();
 
 private:
  DeviceDir* InitDeviceDir_();
  DeviceReadHandle* InitDeviceReadHandle_(DeviceDir& device_dir);
  Index* InitIndex_();

  bool ReplayLogs_();  
 
 private:
  const Config* config_;
  bool* end_;

  DeviceDir* device_dir_;
  DeviceReadHandle* read_handle_;
  Index* index_;
};

}}
