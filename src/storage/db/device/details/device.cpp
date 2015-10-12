#include "../db_device.h"

namespace xforce { namespace fooskv {

Device::Device() :
db_device_dir_(NULL),
  read_handle_(NULL),
  write_handle_(NULL),
  index_(NULL) {}

bool Device::Init(const Config& config) {
  DeviceDirIterator* iter;

  XFC_FAIL_HANDLE(!InitDeviceDir_())
  XFC_FAIL_HANDLE(!InitDeviceReadHandle_(*device_dir_))
  XFC_FAIL_HANDLE(!)
  XFC_FAIL_HANDLE(!InitIndex_())
  
  tmp_device_log_ = DeviceLog::Produce();
  XFC_ASSERT(NULL!=tmp_device_log_)

  iter = ReplayLogs_();
  XFC_FAIL_HANDLE(NULL==iter)

  XFC_FAIL_HANDLE(!InitDeviceWriteHandle_(*device_dir_, *iter))

  return true;

  ERROR_HANDLE:
  FATAL("fail_init_device");
  return false;
}

ErrNo Device::CreateTable(NoTable no_table, const std::string& name_table) {
}

ErrNo Device::DropTable(NoTable no_table, const std::string& name_table) {
}

ErrNo Device::Add(
    NoTable no_table, 
    const std::string& name_table,
    const KV& kv, 
    LogicTime logic_time) {
}

ErrNo Device::Remove(
    NoTable no_table, 
    const std::string& name_table,
    const KV& kv, 
    LogicTime logic_time) {
}

Device::~Device() {
  XFC_DELETE(index_)
  XFC_DELETE(read_handle_)
  XFC_DELETE(device_dir_)
  XFC_DELETE(tmp_device_log_)
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

bool Device::InitDeviceWriteHandle_(DeviceDir& device_dir, DeviceDirIterator& iter) {
  XFC_NEW(write_handle_, DeviceWriteHandle)
  bool ret = write_handle_->Init(device_dir, (*config)["flush_sync"].AsBool());
  if (ret) {
    return true;
  } else {
    WARN("fail_init_write_handle");
    return false;
  }

  bool 
}

DeviceDirIterator* Device::ReplayLogs_() {
  const DevicePos& device_pos = index_->GetReplayPos();
  DeviceDirIterator* iter = new DeviceDirIterator(*device_dir_, device_pos.index);
  if (-1 == device_pos.index) {
    return iter;
  }

  do {
    int ret = read_handle_->Reset(*iter, device_pos.offset);
    XFC_FAIL_HANDLE_WARN(true!=ret, "fail_reset_read_handle_when_replay_logs")

    ret = read_handle_->ReadLog(*tmp_device_log_);
    if (0==ret) {
      ret = ReplayDeviceLog_(*tmp_device_log_);
      XFC_FAIL_HANDLE_WARN(true!=ret, "fail_replay_log_at[" << *iter << "]")
    } else if (ret>0) {
      ret = iter->MoveToNext();
      if (0==ret) {
        continue;
      } else if (ret>0) {
        break;
      } else {
        XFC_FAIL_HANDLE_WARN(true, "fail_move_to_next_index");
      }
    } else {
      XFC_FAIL_HANDLE_WARN(true, "fail_read_log")
    }
  } while (iter->GetIndex() != -1);
  return iter;

  ERROR_HANDLE:
  WARN("fail_replay_logs");
  return NULL;
}

}}
