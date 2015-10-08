#include "../db_device.h"

namespace xforce { namespace fooskv {

DBDevice::DBDevice() :
  db_device_dir_(NULL),
  write_handle_(NULL) {}

bool DBDevice::Init(const Config& config) {
  XFC_NEW(db_device_dir_, DBDeviceDir)
  XFC_NEW(write_handle_, DBDeviceWriteHandle)

  bool ret = write_handle_->Reset(-1);
  XFC_FAIL_HANDLE(!ret)

  return true;

  ERROR_HANDLE:
  return false;
}

ErrNo DBDevice::Add(NoTable no_table, const KV& kv, LogicTime logic_time) {
}

ErrNo DBDevice::Remove(NoTable no_table, const KV& kv, LogicTime logic_time) {
}

DBDevice::~DBDevice() {
}

}}
