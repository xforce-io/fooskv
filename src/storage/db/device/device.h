#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class Device {
 public:
  Device();

  bool Init(const Config& config, bool* end);

  ErrNo CreateTable(NoTable no_table, const std::string& name_table);
  ErrNo DropTable(NoTable, const std::string& name_table);

  ErrNo Add(
      NoTable no_table, 
      const std::string& name_table, 
      const KV& kv, 
      LogicTime logic_time);

  ErrNo Remove(
      NoTable no_table, 
      const std::string& name_table,
      const KVB& kv, 
      LogicTime logic_time);

  virtual ~Device();
 
 private:
  bool InitDeviceDir_();
  bool InitDeviceReadHandle_(DeviceDir& device_dir);
  bool InitIndex_();
  bool InitDeviceWriteHandle_(DeviceDir& device_dir, DeviceDirIterator& iter);
  

  DeviceDirIterator* ReplayLogs_();  
  bool ReplayDeviceLog_(const DeviceLog& device_log);
 
 private:
  const Config* config_;
  bool* end_;

  DeviceDir* device_dir_;
  DeviceReadHandle* read_handle_;
  DeviceWriteHandle* write_handle_;
  Index* index_;

  mutable DeviceLog* tmp_device_log_;
};

}}
