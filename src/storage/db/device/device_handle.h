#include "public.h"
#include "device_dir.h"

namespace zmt { namespace foosdb {

class DeviceWriteHandle {
 public:
  struct FlushUnit {
    char* addr;
    size_t len;
  };

 public:
  static const size_t kTimesFlushRetry=3;
  static char* const kInvalidAddr;

 public:
  DeviceWriteHandle(DeviceDir& device_dir, bool flush_sync);

  /*
   * @parameters:
   *    index: index of the db file, if -1, seek to the last file
   *    offset: offset of the pointer, if -1, seek to the end
   */
  bool Reset(DeviceDirIterator dir_iter, Offset offset=0);
  inline bool Reset(Index index, Offset offset=0);
  bool Reset(const std::string& filepath, Offset offset=0);
  bool SeekToTheEnd();

  /*
   * @return: 
   *    ==0: valid log here
   *    >0 : uninit log here
   *    <0 : log corrupt
   */
  int HasValidLogHere();

  inline bool HasSpaceForNewLog(const FlushMsg& msg);
  void WriteCurrentLog(const FlushMsg& msg);
  inline bool HasSpaceForNewLog(const DeviceLog& device_log);
  void WriteCurrentLog(const DeviceLog& device_log);

  /*
   * @note: can be called only when HasValidLogHere return 0
   */
  inline void PassLog();
  inline bool MapReadLock();
  inline void MapUnlock();

  const std::string& GetFilePath() const { return filepath_; }
  int GetIndex() const { return dir_iter_.GetIndex(); }
  inline Offset GetOffset() const;
  DeviceLog* GetCurrentLog() { return cur_log_; }
  inline DeviceLog* GetLog(Offset offset);
  ssize_t GetRoomLeft() const { return db_device_dir_->GetMaxSizeDeviceBlock() - GetOffset(); }
  bool IsValid() const { return db_device_dir_->End() != dir_iter_; }
  void Flush(bool at_close=false);
  virtual ~DeviceWriteHandle();

 private:
  ssize_t EnsureFilesize_(int fd);
  void Close_(); 
 
 private:
  //const
  DeviceDir* db_device_dir_;
  int sync_flag_;
  ///

  DeviceDirIterator dir_iter_;
  std::string filepath_;
  int fd_;

  char* start_addr_;
  char* tobe_flushed_;
  DeviceLog* cur_log_;

  pthread_rwlock_t rwlock_;
};

class DeviceReadHandle {
 private:
  struct BackupItem {
    int fd;
    time_t timestamp_in_sec;
  };

  typedef std::unordered_map<int, BackupItem> Backups;

 private:
  static const size_t kBackupItemExpireTimeInSec=60; 
  
 public:
  DeviceReadHandle(const DeviceDir& db_device_dir);
  bool Reset(DeviceDirIterator dir_iter, Offset offset=0);
  inline bool Reset(Index index, Offset offset=0);

  /*
   * @return: 
   *   ==0: valid log here
   *    >0 : uninit log here
   *    <0 : log corrupt
   */
  inline int ReadLog(DeviceLog& device_log);

  const DeviceDirIterator& GetDirIter() const { return dir_iter_; }

  int GetIndex() const { return dir_iter_.GetIndex(); }
  Offset GetOffset() const { return lseek(fd_, 0, SEEK_CUR); }
  bool IsValid() const { return db_device_dir_->End() != dir_iter_; }
  virtual ~DeviceReadHandle();

 private:
  void ClearExpiredBackupItems_(Index index_except);

 private:
  //const
  const DeviceDir* db_device_dir_;
  ///

  DeviceDirIterator dir_iter_;
  int fd_;

  Backups backups_;
};

bool DeviceWriteHandle::Reset(Index index, Offset offset) {
  return index>=0 ?
    ( db_device_dir_->NewFile(index) ? 
        Reset(DeviceDirIterator(*db_device_dir_, index), offset) : 
        false ) :
    ( -1 != db_device_dir_->GetLastIndex() ? 
        Reset(db_device_dir_->Last(), offset) : 
        ( db_device_dir_->NewFile() ? 
            Reset(DeviceDirIterator(*db_device_dir_, 0), offset) :
            false ) );
}

bool DeviceWriteHandle::HasSpaceForNewLog(
    const FlushMsg& msg) {
  return GetRoomLeft() >= SCAST<ssize_t>(DeviceLog::GetContentOffset() + 
      msg.msg.Size() + sizeof(DeviceLog().GetChecksum()));
}

bool DeviceWriteHandle::HasSpaceForNewLog(
    const DeviceLog& device_log) {
  return GetRoomLeft() >= SCAST<ssize_t>(DeviceLog::GetContentOffset() + 
      device_log.GetContentLen() + sizeof(DeviceLog().GetChecksum()));
}

void DeviceWriteHandle::PassLog() {
  cur_log_ = RCAST<DeviceLog*>(
      RCAST<char*>(cur_log_) + cur_log_->GetSize());
}

bool DeviceWriteHandle::MapReadLock() {
  return 0 == pthread_rwlock_rdlock(&rwlock_);
}

void DeviceWriteHandle::MapUnlock() {
  pthread_rwlock_unlock(&rwlock_);
}

Offset DeviceWriteHandle::GetOffset() const {
  return RCAST<char*>(cur_log_) - start_addr_;
}

DeviceLog* DeviceWriteHandle::GetLog(Offset offset) { 
  return RCAST<DeviceLog*>(start_addr_+offset); 
}

bool DeviceReadHandle::Reset(Index index, Offset offset) {
  return index>=0 ? 
    Reset(DeviceDirIterator(*db_device_dir_, index), offset) :
    Reset(db_device_dir_->Last(), offset);
}

int DeviceReadHandle::ReadLog(DeviceLog& device_log) {
  int ret = device_log.ReadFrom(fd_);
  if (ret<0) {
    FATAL("fail_read_device_log ret[" 
        << ret 
        << "] index[" 
        << dir_iter_.GetIndex() 
        << "] offset[" 
        << GetOffset() 
        << "] filepath["
        << *dir_iter_ 
        << "]");
  }
  return ret;
}

}}
