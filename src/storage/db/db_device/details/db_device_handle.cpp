#include "../db_device_handle.h"

#include <fcntl.h>
#include <sys/mman.h>

namespace zmt { namespace foosdb {

char* const DBDeviceWriteHandle::kInvalidAddr = RCAST<char* const>(-1);

DBDeviceWriteHandle::DBDeviceWriteHandle(
    DBDeviceDir& db_device_dir,
    bool flush_sync) :
  db_device_dir_(&db_device_dir),
  sync_flag_(flush_sync ? MS_SYNC : MS_ASYNC),
  dir_iter_(db_device_dir),
  fd_(-1),
  start_addr_(kInvalidAddr),
  tobe_flushed_(kInvalidAddr),
  cur_log_(NULL) {
  MEGA_ASSERT(0 == pthread_rwlock_init(&rwlock_, NULL));
}

bool DBDeviceWriteHandle::Reset(DBDeviceDirIterator dir_iter, off_t offset) {
  int ret;
  int new_fd;
  ssize_t filesize;
  char* new_addr;
  struct stat state_file;

  MEGA_FAIL_HANDLE_WARN(offset > SCAST<off_t>(db_device_dir_->GetMaxSizeDeviceBlock()),
      "offset_too_big_for_write_handle[" << offset << "]");

  if (dir_iter!=dir_iter_) {
    std::string new_filepath = *dir_iter;
    new_fd = open(new_filepath.c_str(), O_RDWR|O_CREAT, 0664);
    MEGA_FAIL_HANDLE_WARN(new_fd<0,
        "fail_open_file_for_write[" << new_filepath << "]")

    filesize = EnsureFilesize_(new_fd);
    MEGA_FAIL_HANDLE(filesize<0)

    new_addr = RCAST<char*>(
      mmap(0, db_device_dir_->GetMaxSizeDeviceBlock(), PROT_READ|PROT_WRITE, 
          MAP_SHARED, new_fd, 0));
    MEGA_FAIL_HANDLE_WARN(kInvalidAddr==new_addr,
        "fail_mmap_new_dbfile[" << new_filepath << "]")

    Close_();
    filepath_=new_filepath;
    fd_=new_fd;
    start_addr_=new_addr;
    tobe_flushed_=new_addr;
    cur_log_ = RCAST<DeviceLog*>(start_addr_);
    if (0==filesize) cur_log_->MarkLastLog();
    dir_iter_=dir_iter;
  } else {
    MEGA_FAIL_HANDLE_WARN(
        0 != stat(filepath_.c_str(), &state_file),
        "file[" << filepath_ << "] no_longer_exists")
  }

  if (offset>=0) {
    ret = lseek(fd_, offset, SEEK_SET);
    MEGA_FAIL_HANDLE_WARN(ret<0, "fail_seek_to_offset[" 
        << offset << "] in_path[" << filepath_ << "]");

    cur_log_ = RCAST<DeviceLog*>(start_addr_+offset);
  } else {
    ret = SeekToTheEnd();
    MEGA_FAIL_HANDLE(ret<0);
  }
  return true;

  ERROR_HANDLE:
  Close_();
  return false;
}

bool DBDeviceWriteHandle::Reset(const std::string& filepath, off_t offset) {
  int ret;
  int new_fd;
  ssize_t filesize;
  char* new_addr;
  struct stat state_file;

  MEGA_FAIL_HANDLE_WARN(offset > SCAST<off_t>(db_device_dir_->GetMaxSizeDeviceBlock()),
      "offset_too_big_for_write_handle[" << offset << "]");

  if (filepath!=filepath_) {
    new_fd = open(filepath.c_str(), O_RDWR|O_CREAT, 0664);
    MEGA_FAIL_HANDLE_WARN(new_fd<0,
        "fail_open_file_for_write[" << filepath << "]")

    filesize = EnsureFilesize_(new_fd);
    MEGA_FAIL_HANDLE(filesize<0)

    new_addr = RCAST<char*>(
      mmap(0, db_device_dir_->GetMaxSizeDeviceBlock(), PROT_READ|PROT_WRITE, 
          MAP_SHARED, new_fd, 0));
    MEGA_FAIL_HANDLE_WARN(kInvalidAddr==new_addr,
        "fail_mmap_new_dbfile[" << filepath << "]")

    Close_();
    filepath_=filepath;
    fd_=new_fd;
    start_addr_=new_addr;
    tobe_flushed_=new_addr;
    cur_log_ = RCAST<DeviceLog*>(start_addr_);
    if (0==filesize) cur_log_->MarkLastLog();
    dir_iter_ = db_device_dir_->End();
  } else {
    MEGA_FAIL_HANDLE_WARN(
        0 != stat(filepath_.c_str(), &state_file),
        "file[" << filepath_ << "] no_longer_exists")
  }

  if (offset>=0) {
    ret = lseek(fd_, offset, SEEK_SET);
    MEGA_FAIL_HANDLE_WARN(ret<0, "fail_seek_to_offset[" 
        << offset << "] in_path[" << filepath_ << "]");

    cur_log_ = RCAST<DeviceLog*>(start_addr_+offset);
  } else {
    ret = SeekToTheEnd();
    MEGA_FAIL_HANDLE(ret<0);
  }
  return true;

  ERROR_HANDLE:
  Close_();
  return false;
}

bool DBDeviceWriteHandle::SeekToTheEnd() {
  int ret=0;
  while (true) {
    ret = HasValidLogHere();
    if (0!=ret) break;
    PassLog();
  }

  if (ret<0) {
    WARN("fail_seek_to_end dir[" << db_device_dir_ << "] iter[" << dir_iter_ << "]");
    return false;
  }
  return true;
}

int DBDeviceWriteHandle::HasValidLogHere() {
  size_t content_offset = DeviceLog::GetContentOffset();
  if ( GetRoomLeft() < SCAST<ssize_t>(content_offset)
      || cur_log_->IsEnd() ) {
    return 1;
  }

  if ( GetRoomLeft() < SCAST<ssize_t>(cur_log_->DeviceSpaceAquired() )
      || !cur_log_->CheckChecksum() ) {
    WARN("device_log_corrupt_detected index["
        << dir_iter_
        << "] offset["
        << GetOffset()
        << "] check_checksum["
        << cur_log_->CheckChecksum()
        << "]");
    return -1;
  }
  return 0;
}

void DBDeviceWriteHandle::WriteCurrentLog(const FlushMsg& msg) {
  cur_log_->Assign(msg);
  size_t bytes_written = cur_log_->GetSize();
  cur_log_ = RCAST<DeviceLog*>(RCAST<char*>(cur_log_) + bytes_written);
  cur_log_->MarkLastLog();
}

void DBDeviceWriteHandle::WriteCurrentLog(const DeviceLog& device_log) {
  *cur_log_=device_log;
  size_t bytes_written = cur_log_->GetSize();
  cur_log_ = RCAST<DeviceLog*>(RCAST<char*>(cur_log_) + bytes_written);
  cur_log_->MarkLastLog();
}

DBDeviceWriteHandle::~DBDeviceWriteHandle() {
  Close_();
  pthread_rwlock_destroy(&rwlock_);
}

ssize_t DBDeviceWriteHandle::EnsureFilesize_(int fd) {
  struct stat st;
  char boundary=0;
  ssize_t ret = fstat(fd, &st);
  MEGA_FAIL_HANDLE_WARN(0!=ret,
      "fail_fstat error[" << strerror(errno) << "]")

  if (st.st_size >= SCAST<ssize_t>(db_device_dir_->GetMaxSizeDeviceBlock())) {
    return true;
  }

  ret = lseek(fd, db_device_dir_->GetMaxSizeDeviceBlock(), SEEK_SET);
  MEGA_FAIL_HANDLE_WARN(-1==ret, 
      "fail_lseek error[" << strerror(errno) << "]")

  ret = write(fd, &boundary, sizeof(boundary));
  MEGA_FAIL_HANDLE_WARN(ret != sizeof(boundary), 
      "fail_write error[" << strerror(errno) << "]")
  return st.st_size;

  ERROR_HANDLE:
  return -1;
}

void DBDeviceWriteHandle::Close_() {
  dir_iter_ = db_device_dir_->End();
  Flush(true);
  if (kInvalidAddr!=start_addr_) {
    pthread_rwlock_wrlock(&rwlock_);
    munmap(start_addr_, db_device_dir_->GetMaxSizeDeviceBlock());
    start_addr_=kInvalidAddr;
    pthread_rwlock_unlock(&rwlock_);
  }

  if (-1!=fd_) {
    close(fd_);
    fd_=-1;
  }
}

void DBDeviceWriteHandle::Flush(bool at_close) {
  if (unlikely(kInvalidAddr==tobe_flushed_)) return;

  static const size_t kPageSize = getpagesize();
  size_t size_to_flush=0;
  size_t size_not_flushed = RCAST<char*>(cur_log_) - tobe_flushed_;
  if (!at_close) {
    size_t num_pages = size_not_flushed/kPageSize;
    if (0==num_pages) return;

    size_to_flush = num_pages*kPageSize;
  } else {
    size_to_flush = std::min(
        db_device_dir_->GetMaxSizeDeviceBlock() - (tobe_flushed_-start_addr_),
        size_not_flushed + sizeof(DeviceLog::checksum_));
  }

  int ret = msync(RCAST<void*>(tobe_flushed_), size_to_flush, sync_flag_);
  if (unlikely(0!=ret)) {
    size_t i;
    for (i=0; i<kTimesFlushRetry-1; ++i) {
      ret = msync( RCAST<void*>(tobe_flushed_), size_to_flush, sync_flag_);
      if (0==ret) break;
    }

    if (kTimesFlushRetry-1 == i) {
      WARN("fail_msync current_index[" 
        << dir_iter_
        << "] error["
        << strerror(errno)
        << "]");
      return;
    }
  }
  tobe_flushed_+=size_to_flush;
}

DBDeviceReadHandle::DBDeviceReadHandle(
    const DBDeviceDir& db_device_dir) :
  db_device_dir_(&db_device_dir),
  dir_iter_(db_device_dir),
  fd_(-1) {}

bool DBDeviceReadHandle::Reset(DBDeviceDirIterator dir_iter, off_t offset) {
  int ret;
  struct stat state_file;

  Backups::iterator iter = backups_.find(dir_iter.GetIndex()); 
  if ( iter == backups_.end() ) {
    std::string new_filepath = *dir_iter;
    int new_fd = open(new_filepath.c_str(), O_RDONLY);
    MEGA_FAIL_HANDLE(new_fd<0)

    backups_.insert(std::pair<size_t, BackupItem>(
        dir_iter.GetIndex(), 
        (struct BackupItem){new_fd, Time::GetCurrentSec()}));

    fd_=new_fd;
    dir_iter_=dir_iter; //set last because it indicates validation

    ClearExpiredBackupItems_(dir_iter.GetIndex());
  } else {
    dir_iter_ = DBDeviceDirIterator(*db_device_dir_, iter->first);

    if (0 != stat((*dir_iter_).c_str(), &state_file)) {
      close(iter->second.fd);
      backups_.erase(iter->first);
      dir_iter_ = db_device_dir_->End();
      fd_=-1;
      MEGA_FAIL_HANDLE_FATAL(true, "file[" << *dir_iter_ << "] no_longer_exists")
    }
    fd_ = iter->second.fd;
    iter->second.timestamp_in_sec = Time::GetCurrentSec();
  }

  ret = lseek(fd_, offset, SEEK_SET);
  MEGA_FAIL_HANDLE_WARN(-1==ret, 
      "filepath[" << *dir_iter_ << "] fail_seek_to[" << offset << "]")
  return true;

  ERROR_HANDLE:
  return false;
}

DBDeviceReadHandle::~DBDeviceReadHandle() {
  for (Backups::iterator iter = backups_.begin(); iter != backups_.end(); ++iter) {
    close(iter->second.fd);
  }
}

void DBDeviceReadHandle::ClearExpiredBackupItems_(int index_except) {
  for (Backups::const_iterator iter = backups_.begin(); iter != backups_.end(); ) {
    if ( Time::GetCurrentSec() - iter->second.timestamp_in_sec > int64_t(kBackupItemExpireTimeInSec) 
        && index_except != iter->first ) {
      close(iter->second.fd);
      backups_.erase(iter++);
    } else {
      ++iter;
    }
  }
}

}}
