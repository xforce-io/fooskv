#include "../db_device_dir.h"

#include <sys/statfs.h>
#include <dirent.h>

namespace zmt { namespace foosdb {

bool DBDeviceDir::Init(
    const std::vector<std::string>& dirs,
    size_t max_size_device_block) {
  if (0 == dirs.size()) {
    return false;
  }

  dirs_=dirs;
  max_size_device_block_=max_size_device_block;
  last_index_=-1;
  for (size_t i=0; i < dirs_.size(); ++i) {
    const std::string& dir = dirs_[i];
    DIR* dp = opendir(dir.c_str());
    if (NULL==dp) {
      WARN("fail_open_dir[" << dir << "]");
      return false;
    }

    dirent* dirp;
    char* endptr;
    while ( (dirp = readdir(dp)) != NULL ) {
      if ( 0 != strncmp(dirp->d_name, kDBFilePrefix.c_str(), 
          kDBFilePrefix.length()) ) {
        continue;
      }

      int index = strtol( dirp->d_name + kDBFilePrefix.length(), &endptr, 10);
      if ( '\0' == *endptr && index>last_index_ && index<kMaxNoIndex ) {
        last_index_=index;
      }

      index_to_dir_.insert(std::pair<int, int>(index, i));
    }
    closedir(dp);
  }
  return true;
}

bool DBDeviceDir::NewFile(size_t index) {
  if ( (size_t)-1 != index && (int64_t)index <= last_index_ ) {
    return true;
  }

  int chosen_dir_index=0;
  if (-1!=last_index_) {
    IndexToDir::const_iterator iter = index_to_dir_.find(last_index_);
    if ( index_to_dir_.end() == iter ) {
      return false;
    }
    chosen_dir_index = iter->second;
  }

  last_index_ = ( (size_t)-1 == index ? last_index_+1 : index );

  size_t i;
  for (i=0; i < dirs_.size(); ++i) {
    const std::string& current_dir = dirs_[chosen_dir_index];
    int64_t space_left = System::GetSpaceLeftBytes(current_dir);
    if (space_left<=0) {
      WARN("fail_get_space_left[" << current_dir << "]");
      return false;
    } else if ( size_t(space_left) > max_size_device_block_ ) {
      break;
    }
    chosen_dir_index = (chosen_dir_index+1) % dirs_.size();
  }

  if ( dirs_.size() != i ) {
    index_to_dir_.insert(std::pair<int, int>(last_index_, i));
    return true;
  } else {
    WARN("no_device_has_space_ge[" << max_size_device_block_ << "]");
    return false;
  }
}

const std::string* DBDeviceDir::GetDir(int index) const { 
  IndexToDir::const_iterator iter = index_to_dir_.find(index);
  return index_to_dir_.end() != iter ? &(dirs_[iter->second]) : NULL;
}

int DBDeviceDir::GetSpaceLeftRatio() const {
  typedef std::unordered_set<long> FsSet;
  FsSet fs_set;
  long space_left=0, space_all=0;
  for (size_t i=0; i < dirs_.size(); ++i) {
    struct statfs stat;
    int ret = statfs( dirs_[i].c_str(), &stat );
    if (0!=ret) return -1;

    long fsid = *RCAST<long*>(&(stat.f_fsid));
    FsSet::const_iterator fs_ret = fs_set.find(fsid);
    if ( fs_set.end() == fs_ret ) {
      space_left += stat.f_bavail;
      space_all += stat.f_blocks;
      fs_set.insert(fsid);
    }
  }
  return (double)space_left * 100/space_all;
}

std::string DBDeviceDir::GetPath(size_t dir_index, uint32_t index) const {
  char buf_path[Limits::kMaxSizePath];
  snprintf(
      buf_path, 
      sizeof(buf_path), 
      "%s/%s%u", 
      dirs_[dir_index].c_str(),
      kDBFilePrefix.c_str(), 
      index);
  return buf_path;
}

std::string DBDeviceDir::GetPathWithGreatestSpace(size_t index) const {
  int idx = GetDirIndexWithGreatestSpace();
  return idx>=0 ? GetPath(idx, index) : "";
}

int DBDeviceDir::GetDirIndexWithGreatestSpace() const {
  size_t idx_candidate=-1;
  int64_t space_candidate=0;
  for (size_t i=0; i < dirs_.size(); ++i) {
    int64_t space = System::GetSpaceLeftBytes(dirs_[i]);
    if (space>space_candidate) {
      idx_candidate=i;
      space_candidate=space;
    } else {
      return -1;
    }
  }
  return idx_candidate;
}

int DBDeviceDirIterator::MoveToNextFile() {
  if (-1==index_) return 1;

  int index_now=index_;
  index_ = DBDeviceDir::kMaxNoIndex;

  const DBDeviceDir::Dirs& dirs = db_device_dir_->GetDirs();
  for (size_t i=0; i < dirs.size(); ++i) {
    DIR* dp = opendir(dirs[i].c_str());
    if (NULL==dp) {
      WARN("fail_open_dir[" << dirs[i] << "]");
      return -1;
    }

    dirent* dirp;
    char* endptr;
    while ( (dirp = readdir(dp)) != NULL ) {
      if ( 0 != strncmp(dirp->d_name, kDBFilePrefix.c_str(), 
          kDBFilePrefix.length()) ) {
        continue;
      }

      int index = strtol( dirp->d_name + kDBFilePrefix.length(), &endptr, 10);
      if ('\0' == *endptr && index>index_now && index<index_) {
        index_=index;
      }
    }
    closedir(dp);
  }
  return DBDeviceDir::kMaxNoIndex != index_ ? 0 : 1;
}

int DBDeviceDirIterator::MoveToPrevFile() {
  if (-1==index_) return 1;

  int index_now=index_;
  index_=-1;

  const DBDeviceDir::Dirs& dirs = db_device_dir_->GetDirs();
  for (size_t i=0; i < dirs.size(); ++i) {
    DIR* dp = opendir(dirs[i].c_str());
    if (NULL==dp) {
      WARN("fail_open_dir[" << dirs[i] << "]");
      return -1;
    }

    dirent* dirp;
    char* endptr;
    while ( (dirp = readdir(dp)) != NULL ) {
      if ( 0 != strncmp(dirp->d_name, kDBFilePrefix.c_str(), 
          kDBFilePrefix.length()) ) {
        continue;
      }

      int index = strtol( dirp->d_name + kDBFilePrefix.length(), &endptr, 10);
      if ( '\0' == *endptr && index<index_now && index>index_ ) {
        index_=index;
      }
    }
    closedir(dp);
  }
  return -1!=index_ ? 0 : 1;
}

std::string DBDeviceDirIterator::operator*() const {
  DBDeviceDir::IndexToDir::const_iterator iter = db_device_dir_->index_to_dir_.find(index_);
  if ( db_device_dir_->index_to_dir_.end() == iter ) return NULL;

  return db_device_dir_->GetPath(iter->second, index_);
}

DBDeviceDirIterator::DBDeviceDirIterator(const DBDeviceDir& db_device_dir, int index) :
  db_device_dir_(&db_device_dir),
  index_(index) {}

}}
