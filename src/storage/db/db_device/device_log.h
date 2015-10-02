#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class DeviceLog {
 public: 
  typedef DeviceLog Self;
  
 public:
  /*
   * @note: magic number indicates end of a db file, set in checksum
   */
  static const uint32_t kEndMagic = 0x9ED0A676;
   
 public:
  inline DeviceLog& operator=(const Self& other);
  void Assign(const FlushMsg& msg);
  inline bool CheckChecksum() const;

  /*
   * @return:
   *    ==0 : ok
   *    > 0 : end
   *    < = : error
   */
  int ReadFrom(int fd);

  inline void MarkLastLog() { checksum_=kEndMagic; }

  uint32_t GetChecksum() const { return checksum_; }
  DeviceCmd::EnumDeviceCmd GetDeviceCmd() const { return cmd_; }
  time_t GetLogicTime() const { return logic_time_; }
  Key GetKey() const { return key_; }
  int GetContentLen() const { return len_; }
  const char* GetContent() const { return content_; }
  inline const char* GetContentStr() const;
  inline static size_t GetContentOffset();
  size_t GetSize() const { return GetContentOffset() + len_; }
  inline size_t DeviceSpaceAquired() const;

  bool IsEnd() const { return checksum_ == kEndMagic; }
 
 private:
  inline void GenChecksum_();

 private:
  uint32_t checksum_;
  DeviceCmd::EnumDeviceCmd cmd_;
  time_t logic_time_;
  Key key_;
  int len_;
  char content_[];
};

DeviceLog& DeviceLog::operator=(const Self& other) {
  memcpy(&checksum_, &(other.checksum_), GetContentOffset() + other.len_);
  return *this;
}

bool DeviceLog::CheckChecksum() const {
  return Crc32Checker::Check(RCAST<const char*>(&checksum_), 
      DeviceLog::GetContentOffset() + len_);
}

void DeviceLog::GenChecksum_() {
  checksum_ = Crc32Checker::Gen(RCAST<const char*>(&cmd_), 
      DeviceLog::GetContentOffset() - sizeof(checksum_) + len_);
}

const char* DeviceLog::GetContentStr() const {
  return ( len_>0 && '\0' == *(content_+len_-1) ) ? content_ : NULL;
}

size_t DeviceLog::GetContentOffset() { 
  return offsetof(DeviceLog, content_); 
}

size_t DeviceLog::DeviceSpaceAquired() const {
  return GetContentOffset() + len_ + sizeof(checksum_);
}

}}
