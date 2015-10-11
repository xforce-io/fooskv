#include "../db_device.h"

namespace xforce { namespace fooskv {

Device::Device() :
  db_device_dir_(NULL),
  read_handle_(NULL),
  index_(NULL) {}

bool Device::Init(const Config& config) {
  XFC_FAIL_HANDLE(!InitDeviceDir_())
  XFC_FAIL_HANDLE(!InitDeviceReadHandle_(*device_dir_))
  XFC_FAIL_HANDLE(!InitIndex_())

  return true;

  ERROR_HANDLE:
  return false;
}

ErrNo Device::Add(NoTable no_table, const KV& kv, LogicTime logic_time) {
}

ErrNo Device::Remove(NoTable no_table, const KV& kv, LogicTime logic_time) {
}

Device::~Device() {
  XFC_DELETE(index_)
  XFC_DELETE(read_handle_)
  XFC_DELETE(device_dir_)
}

bool Device::InitDeviceDir_() {
  bool ret;
  const std::vector<std::string> dirs;
  const ListType& conf_dirs = (*config_)["dirs"].AsList();
  ListType::const_iterator iter;

  XFC_FAIL_HANDLE_WARN(
      !(*config_)["dirs"].IsList(),
      "fail_init_device dirs_should_be_list")

  for (iter = conf_dirs.begin(); iter != conf_dirs.end(); ++iter) {
    XFC_FAIL_HANDLE_WARN(
        !iter->IsStr(),
        "fail_init_device_ dirs_should_be_strings")

    dirs.push_back(iter->AsStr());
  }

  XFC_FAIL_HANDLE_WARN(
      !(*config)["max_size_device_block"].IsInt(), 
      "max_size_device_block_should_be_int")
  
  XFC_NEW(device_dir_, DeviceDir)
  ret = device_dir_->Init(dirs, (*config)["max_size_device_block"].AsInt());
  XFC_FAIL_HANDLE_WARN(!ret, "fail_init_device_dir")
  return true;

  ERROR_HANDLE:
  return false;
}

bool Device::InitDeviceReadHandle_(DeviceDir& device_dir) {
  XFC_NEW(read_handle_, DeviceReadHandle(device_dir))
  return true;
}

bool Device::InitIndex_() {
  XFC_NEW(index_, Index)
  bool ret = index_->Init(*config_, end_);
  if (ret) {
    return true;
  } else {
    WARN("fail_init_index");
    return false;
  }
}

bool Device::ReplayLogs_() {
  const DevicePos& device_pos = index_->GetReplayPos();
  if (-1 == device_pos.index) {
    return true;
  }

  DeviceDirIterator* iter = new DeviceDirIterator(*device_dir_, device_pos.index);
  do {
    int ret = read_handle_->Reset(*iter, device_pos.offset);
    XFC_FAIL_HANDLE_WARN(true!=ret, "fail_reset_read_handle_when_replay_logs")

    ret = read_handle_->ReadLog();
  } while (iter->GetIndex() != -1);
  return true;

  ERROR_HANDLE:
  return false;
}

}}
