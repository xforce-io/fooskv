#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class Index {
 private:
  typedef Index Self;
  typedef std::unordered_map<NoTable, TableIndex*> TableIndexes;
  
 public: 
  Index();

  bool Init(const Config& config, bool* end);
  const DevicePos& GetReplayPos() const { return logic_mark_.Get(); }

  inline static bool Replay(
      Self& self, 
      NoTable no_table,
      bool is_add, 
      KeyHash key_hash, 
      DevicePos device_pos);

  ErrNo CreateTable(NoTable no_table, const std::string& name_table, size_t num_buckets);
  ErrNo DropTable(NoTable no_table);
  inline ErrNo Add(NoTable no_table, KeyHash key_hash, DevicePos device_pos);
  inline ErrNo Remove(NoTable no_table, KeyHash key_hash);

  virtual ~Index();
 
 private: 
  bool Recover_();
  inline ErrNo Modify_(bool is_add, NoTable no_table, KeyHash key_hash, DevicePos device_pos);
  inline void Dump_();

 private:
  const Config* config_;
  bool* end_;

  TableIndexes table_indexes_; 
  time_t last_dump_sec_;

  //cache
  NoTable cache_no_table_;
  TableIndex* cache_table_index_;
};

bool Index::Replay(
    Self& self, 
    NoTable no_table,
    bool is_add, 
    KeyHash key_hash, 
    DevicePos device_pos) {
  return is_add ? self.Add(no_table, key_hash, device_pos) 
      : self.Remove(no_table, key_hash);
}

ErrNo Index::Add(NoTable no_table, KeyHash key_hash, DevicePos device_pos) {
  time_t cur_sec = Time::GetCurrentSec();
  if (cur_sec - last_dump_sec_ > 0) {
    Dump_();
  }

  bool ret = Modify_(true, no_table, key_hash, device_pos);
  if (ret) {
    logic_mark_.Save(device_pos);
  }
  return ret;
}

ErrNo Index::Remove(NoTable no_table, KeyHash key_hash) {
  static const DevicePos tmp_device_pos = (struct DevicePos){0, 0};
  return Modify_(false, no_table, key_hash, tmp_device_pos);
}

ErrNo Index::Modify_(
    bool is_add, 
    NoTable no_table, 
    KeyHash key_hash, 
    DevicePos device_pos) {
  if (cache_no_table_ == no_table) {
    return is_add ? cache_table_index_->Add(key_hash, device_pos)
        : cache_table_index_->Remove(key_hash);
  }

  TableIndex::iterator iter = table_indexes_->find(key_hash);
  if (iter != table_indexes_.end()) {
    cache_no_table_ = no_table;
    cache_table_index_ = *iter;
    return is_add ? iter->Add(key_hash, device_pos) : iter->Remove(key_hash);
  } else {
    return kTableNotExist;
  }
}

void Index::Dump_() {
  TableIndexes::iterator iter;
  for (iter = table_indexes_.begin(); iter != table_indexes_.end(); ++iter) {
    iter.second->Dump();
  }
}

}}
