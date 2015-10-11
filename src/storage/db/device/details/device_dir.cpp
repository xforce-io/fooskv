#include "../device_dir.h"

#include <sys/statfs.h>
#include <dirent.h>

namespace zmt { namespace foosdb {

DeviceDir::DeviceDir() :
  tmp_iter_(NULL) {}

bool DeviceDir::Init(
    const std::vector<std::string>& dirs,
    size_t max_size_device_block) {
  if (0 == dirs.size()) {
    return false;
  }

  dirs_=dirs;
  max_size_device_block_=max_size_device_block;

  tmp_iter_ = new DeviceDirIterator(*this);
  int ret = tmp_iter_->MoveToFirst();
  if (0!=ret) {
    return 1==ret ? true : false;
  }
  index_to_dir_.insert(std::pair<int, int>(tmp_iter_->GetIndex(), tmp_iter_->GetDir()));

  while (true) {
    int ret = tmp_iter_.MoveToNext();
    if (0!=ret) {
      return 1==ret ? true : false;
    }
    index_to_dir_.insert(std::pair<int, int>(tmp_iter_->GetIndex(), tmp_iter_->GetDir()));
  }
  return true;
}

bool DeviceDir::NewFile(int index) {
  int last_index;
  int ret = tmp_iter_->MoveToLast();
  if (0==ret) {
    last_index = tmp_iter_->GetIndex();
    if ( -1 != index && index <= last_index ) {
      return true;
    }
  } else if (ret>0) {
    if (-1!=index) {
      return false;
    }
  } else {
     return false;
  }

  int chosen_dir_index=0;
  if (-1!=last_index) {
    IndexToDir::const_iterator iter = index_to_dir_.find(last_index);
    if ( index_to_dir_.end() == iter ) {
      return false;
    }
    chosen_dir_index = iter->second;
  }

  last_index = ( (size_t)-1 == index ? last_index+1 : index );

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
    index_to_dir_.insert(std::pair<int, int>(last_index, i));
    return true;
  } else {
    WARN("no_device_has_space_ge[" << max_size_device_block_ << "]");
    return false;
  }
}

const std::string* DeviceDir::GetDir(int index) const { 
  IndexToDir::const_iterator iter = index_to_dir_.find(index);
  return index_to_dir_.end() != iter ? &(dirs_[iter->second]) : NULL;
}

int DeviceDir::GetSpaceLeftRatio() const {
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

std::string DeviceDir::GetPath(size_t dir_index, uint32_t index) const {
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

std::string DeviceDir::GetPathWithGreatestSpace(size_t index) const {
  int idx = GetDirIndexWithGreatestSpace();
  return idx>=0 ? GetPath(idx, index) : "";
}

int DeviceDir::GetDirIndexWithGreatestSpace() const {
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

DeviceDirIterator::DeviceDirIterator(const DeviceDir& device_dir, int index) :
  device_dir_(&device_dir),
  index_(index) {}

int DeviceDirIterator::MoveToFirst() {
  return MoveToSuitableIndex_(-1, DeviceDir::kMaxNoIndex, true);
}

int DeviceDirIterator::MoveToLast() {
  return MoveToSuitableIndex_(-1, DeviceDir::kMaxNoIndex, false);
}

int DeviceDirIterator::MoveToNextIndex() {
  if (-1==index_) return 1;
  return MoveToSuitableIndex_(index_, DeviceDir::kMaxNoIndex, true);
}

int DeviceDirIterator::MoveToPrevIndex() {
  if (-1==index_) return 1;
  return MoveToSuitableIndex_(-1, index_, false);
}

std::string DeviceDirIterator::operator*() const {
  DeviceDir::IndexToDir::const_iterator iter = device_dir_->index_to_dir_.find(index_);
  if ( device_dir_->index_to_dir_.end() == iter ) return NULL;

  return device_dir_->GetPath(iter->second, index_);
}

int DeviceDirIterator::MoveToSuitableIndex_(int left_bound, int right_bound, bool is_first) {
  int index_ = is_first ? right_bound : left_bound;

  const DeviceDir::Dirs& dirs = device_dir_->GetDirs();
  for (size_t i=0; i < dirs.size(); ++i) {
    DIR* dp = opendir(dirs[i].c_str());
    if (NULL==dp) {
      WARN("fail_open_dir[" << dirs[i] << "]");
      return -1;
    }

    dirent* dirp;
    char* endptr;
    while ( (dirp = readdir(dp)) != NULL ) {
      if ( 0 != strncmp(dirp->d_name, kDBFilePrefix.c_str(), kDBFilePrefix.length()) ) {
        continue;
      }

      int index = strtol( dirp->d_name + kDBFilePrefix.length(), &endptr, 10);
      if (is_first) {
        if ( '\0' == *endptr && left_bound<index && index<index_) {
          dir_=i;
          index_=index;
        }
      } else {
        if ( '\0' == *endptr && index_<index && index<right_bound) {
          dir_=i;
          index_=index;
        }
      }
    }
    closedir(dp);
  }

  if (is_first) {
    if (index_!=right_bound) {
      return 0;
    } else {
      index_ = -1;
      return 1;
    }
  } else {
    if (index_!=left_bound) {
      return 0;
    } else {
      index_ = -1;
      return 1;
    }
  }
}

}}
