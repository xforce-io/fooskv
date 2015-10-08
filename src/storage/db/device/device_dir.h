#pragma once

#include "public.h"
#include "device_log.h"

namespace zmt { namespace foosdb {

class DeviceDirIterator;

class DeviceDir {
 public:
  typedef std::vector<std::string> DeviceDirs;
  typedef std::unordered_map<int, int> IndexToDeviceDir;
 
 public:
  static const int kMaxNoIndex=(1<<30); 
   
 public: 
  bool Init(const std::vector<std::string>& dirs, size_t max_size_device_block);
 
  bool NewFile(size_t index = (size_t)-1);
  size_t GetMaxSizeDeviceBlock() const { return max_size_device_block_; }
  inline int GetLastIndex() const { return last_index_; }
  int GetSpaceLeftRatio() const;
  std::string GetPathWithGreatestSpace(size_t index) const;

  inline DeviceDirIterator Last() const;
  inline DeviceDirIterator End() const;

 protected:
  const std::string* GetDeviceDir(int index) const;
  const std::vector<std::string>& GetDeviceDirs() const { return dirs_; }
  std::string GetPath(size_t dir_index, uint32_t index) const;
  int GetDeviceDirIndexWithGreatestSpace() const;

 private: 
  //const
  DeviceDirs dirs_;
  size_t max_size_device_block_;
  ///

  int last_index_;
  IndexToDeviceDir index_to_dir_;

  friend DeviceDirIterator;
};

class DeviceDirIterator {
 public:
  DeviceDirIterator(const DeviceDir& db_device_dir, int index=-1);

  void SetIndex(int index) { index_=index; }
  int MoveToNextFile();
  int MoveToPrevFile();
  std::string operator*() const;
  inline bool operator==(const DeviceDirIterator& other) const;
  inline bool operator!=(const DeviceDirIterator& other) const;

  int GetIndex() const { return index_; }
  const std::string* GetDeviceDir() const { return db_device_dir_->GetDeviceDir(index_); }

 private:
  //const
  const DeviceDir* db_device_dir_;
  ///

  int index_;

  friend std::ostream& operator<<(std::ostream& os, const DeviceDirIterator& dir_iter);
};

DeviceDirIterator DeviceDir::Last() const {
  return DeviceDirIterator(CCAST<const DeviceDir&>(*this), last_index_);
}

DeviceDirIterator DeviceDir::End() const {
  return DeviceDirIterator(CCAST<const DeviceDir&>(*this), -1);
}

bool DeviceDirIterator::operator==(const DeviceDirIterator& other) const {
  return index_ == other.index_;
}

bool DeviceDirIterator::operator!=(const DeviceDirIterator& other) const {
  return index_ != other.index_;
}

inline std::ostream& operator<<(std::ostream& os, const DeviceDirIterator& dir_iter) {
  os << dir_iter.index_;
  return os;
}

}}
