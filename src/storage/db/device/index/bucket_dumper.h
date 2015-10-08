#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

struct ModifyRec {
  static const size_t kIOTrials=3;

  bool isAdd;
  KeyHash key_hash;
  DevicePos device_pos;

  inline static bool Read(FILE* fp);
  inline static bool Write(FILE* fp);
};

class BucketDumper {
 private:
  typedef BucketDumper Self;
  typedef MultiCowBtreeMap<KeyHash, DevicePos> Index; 
  typedef std::vector<ModifyRec> ModifyRecs;
 
 public:
  BucketDumper();

  bool Init(
      const Config& config, 
      NoTable no_table, 
      const std::string& name_table,
      size_t no_bucket,
      bool* end,
      bool newly_created);

  bool Recovery(Index& index);
  const DevicePos* GetReplayPos();

  inline void Add(KeyHash key_hash, DevicePos device_pos, const Index& index);
  inline void Remove(KeyHash key_hash);


  virtual ~BucketDumper();
 
 private:
  bool RecoveryIndex_();
  bool RecoveryRecs_();

  bool Dump_(DevicePos device_pos, const Index& index);

  bool DumpStart_(const Index& index);
  bool DumpEnd_();

  bool DumpIndex_(const Index& index);
  bool DumpRecs_(size_t from, size_t to, bool is_append=true);

  bool RealDump_();

  std::string GetDumpIndexFilepath_() const;
  std::string GetDumpTmpIndexFilepath_() const;
  std::string GetDumpRecsFilepath_() const;
  std::string GetDumpTmpRecsFilepath_() const;

  static void* Dumper_(void* arg); 

 private:
  const Config* config_;
  NoTable no_table_;
  const std::string* name_table_;
  size_t no_bucket_;
  bool* end_;
  bool newly_created_;

  std::string dump_dir_;
  std::string index_filepath_;
  std::string recs_filepath_;
  std::string tmp_dump_dir_;
  std::string tmp_index_filepath_;
  std::string tmp_recs_filepath_;
  time_t dump_interval_sec_;

  LogicMark<DevicePos> dump_mark_;
  Index* index_for_dump_;

  pthread_t tid_dumper_;

  ModifyRecs modify_recs_;
  size_t total_len_modify_recs_;

  time_t last_dump_sec_;

  size_t cur_index_modify_rec_;
  atomic<bool> is_dumping_;
  atomic<bool> dump_end_;
  atomic<bool> deep_dump_;
  atomic<bool> error_;
};

}}

namespace xforce { namespace fooskv {

bool ModifyRec::Read(FILE* fp) {
  return StorageIOHelper::Read(this, sizeof(ModifyRec), 1, fp);
}

bool ModifyRec::Write(FILE* fp) {
  return StorageIOHelper::Write(this, sizeof(ModifyRec), 1, fp);
}

void BucketDumper::Add(KeyHash key_hash, DevicePos device_pos, const Index& index) {
  if (!Dump_(device_pos, Index)) {
    FATAL("fail_dump_for_table[" << name_table_ << "]");
  }

  ModifyRec modify_rec;
  modify_rec.isAdd = true;
  modify_rec.key_hash = key_hash;
  modify_rec.device_pos = device_pos;
  modify_recs_.push_back(modify_rec);
}

void BucketDumper::Remove(KeyHash key_hash) {
  ModifyRec modify_rec;
  modify_rec.isAdd = false;
  modify_rec.key_hash = key_hash;
  modify_rec.device_pos = device_pos;
  modify_recs_.push_back(modify_rec);
}

}}
