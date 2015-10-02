#pragma once

#include "public.h"
#include "device_log.h"

namespace zmt { namespace foosdb {

class DBDeviceDirIterator;

class DBDeviceDir {
 public:
  typedef std::vector<std::string> Dirs;
  typedef std::unordered_map<int, int> IndexToDir;
 
 public:
  static const int kMaxNoIndex=(1<<30); 
   
 public: 
  bool Init(const std::vector<std::string>& dirs, size_t max_size_device_block);
 
  bool NewFile(size_t index = (size_t)-1);
  size_t GetMaxSizeDeviceBlock() const { return max_size_device_block_; }
  inline int GetLastIndex() const { return last_index_; }
  int GetSpaceLeftRatio() const;
  std::string GetPathWithGreatestSpace(size_t index) const;

  inline DBDeviceDirIterator Last() const;
  inline DBDeviceDirIterator End() const;

 protected:
  const std::string* GetDir(int index) const;
  const std::vector<std::string>& GetDirs() const { return dirs_; }
  std::string GetPath(size_t dir_index, uint32_t index) const;
  int GetDirIndexWithGreatestSpace() const;

 private: 
  //const
  Dirs dirs_;
  size_t max_size_device_block_;
  ///

  int last_index_;
  IndexToDir index_to_dir_;

  friend DBDeviceDirIterator;
};

class DBDeviceDirIterator {
 public:
  DBDeviceDirIterator(const DBDeviceDir& db_device_dir, int index=-1);

  void SetIndex(int index) { index_=index; }
  int MoveToNextFile();
  int MoveToPrevFile();
  std::string operator*() const;
  inline bool operator==(const DBDeviceDirIterator& other) const;
  inline bool operator!=(const DBDeviceDirIterator& other) const;

  int GetIndex() const { return index_; }
  const std::string* GetDir() const { return db_device_dir_->GetDir(index_); }

 private:
  //const
  const DBDeviceDir* db_device_dir_;
  ///

  int index_;

  friend std::ostream& operator<<(std::ostream& os, const DBDeviceDirIterator& dir_iter);
};

DBDeviceDirIterator DBDeviceDir::Last() const {
  return DBDeviceDirIterator(CCAST<const DBDeviceDir&>(*this), last_index_);
}

DBDeviceDirIterator DBDeviceDir::End() const {
  return DBDeviceDirIterator(CCAST<const DBDeviceDir&>(*this), -1);
}

bool DBDeviceDirIterator::operator==(const DBDeviceDirIterator& other) const {
  return index_ == other.index_;
}

bool DBDeviceDirIterator::operator!=(const DBDeviceDirIterator& other) const {
  return index_ != other.index_;
}

inline std::ostream& operator<<(std::ostream& os, const DBDeviceDirIterator& dir_iter) {
  os << dir_iter.index_;
  return os;
}

}}
