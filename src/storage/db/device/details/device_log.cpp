#include "../device_log.h"

namespace xforce { namespace fooskv {

DeviceLog* DeviceLog::Produce() {
  return RCAST<Self*>(operator new (kMaxSize))
}

void DeviceLog::Assign(const FlushMsg& msg) {
  logic_time_ = msg.logic_time;
  cmd_ = msg.cmd;
  if (DeviceCmd::kAddRecords == cmd_) {
    key_ = msg.cmd_extra.add_rec.key;
  }
  len_ = msg.msg.Size();
  memcpy(content_, msg.msg.Data(), len_);
  GenChecksum_();
}

int DeviceLog::ReadFrom(int fd) {
  ssize_t ret = read(fd, &checksum_, sizeof(checksum_));
  if (sizeof(checksum_) != ret) return -1;

  if (IsEnd()) return 1;

  ssize_t count = GetContentOffset() - sizeof(checksum_);
  ret = read(fd, &cmd_, count);
  if (count!=ret) return -2;

  if (len_<0 || SCAST<size_t>(len_) > Limits::kMaxSizeRecord) {
    return -3;
  }

  ret = read(fd, content_, len_);
  if (len_!=ret) {
    return -4;
  } else if (!CheckChecksum()) {
    return -5;
  }
  return 0;
}

const std::string DeviceLog::Str() const {
  std::stringstream ss;
  ss << "DeviceLog[cmd:" 
      << cmd_.Str() 
      << "|no_table:" 
      << no_table_
      << "|logic_time:"
      << logic_time_
      << "|key_hash_:"
      << key_hash_
      << "|len_all_:"
      << len_all_
      << "|len_key_:"
      << len_key_
      << "]";
  return ss.str();
}

}}
