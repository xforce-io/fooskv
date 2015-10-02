#include "../device_log.h"

namespace xforce { namespace fooskv {

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

}}
