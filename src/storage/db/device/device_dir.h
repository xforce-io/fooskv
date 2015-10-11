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
  DeviceDir();

  bool Init(const std::vector<std::string>& dirs, size_t max_size_device_block);
 
  /*
   * @parameters :
   *    index : create a new file with this no, increase current max if is -1
   */
  bool NewFile(int index=-1);
  size_t GetMaxSizeDeviceBlock() const { return max_size_device_block_; }
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

  IndexToDeviceDir index_to_dir_;

  mutable DeviceDirIterator* tmp_iter_;

  friend DeviceDirIterator;
};

class DeviceDirIterator {
 public:
  /*
   * @parameters :
   *    index : index of this device file, -1 represents the end
   */
  DeviceDirIterator(const DeviceDir& device_dir, int index=-1);

  void SetIndex(int index) { index_=index; }

  /*
   * @return :
   *    1 : no more index available
   *    0 : succ
   *   -1 : error happens 
   */
  int MoveToFirst();
  int MoveToLast();
  int MoveToNext();
  int MoveToPrev();

  std::string operator*() const;
  inline bool operator==(const DeviceDirIterator& other) const;
  inline bool operator!=(const DeviceDirIterator& other) const;

  size_t GetDir() const { return dir_; }
  int GetIndex() const { return index_; }
  const std::string* GetDeviceDir() const { return device_dir_->GetDeviceDir(index_); }

 private:
  int MoveToSuitableIndex_(int left_bound, int right_bound, bool is_first /* or is last */);

 private:
  //const
  const DeviceDir* device_dir_;
  ///

  size_t dir_;
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
