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
      bool* end);

  bool Recovery(Index& index);

  inline void Add(KeyHash key_hash, DevicePos device_pos);
  inline void Remove(KeyHash key_hash);

  virtual ~BucketDumper();
 
 private:
  bool RecoveryIndex_();
  bool RecoveryRecs_();

  bool Dump_();

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

  std::string index_filepath_;
  std::string tmp_index_filepath_;
  std::string recs_filepath_;
  std::string tmp_recs_filepath_;

  Index* index_for_dump_;

  pthread_t tid_dumper_;

  FILE* fout_;
  ModifyRecs modify_recs_;
  size_t last_index_modify_rec_;
};

}}

namespace xforce { namespace fooskv {

bool ModifyRec::Read(FILE* fp) {
  if (1 == fread(this, sizeof(ModifyRec), 1, fp)) {
    return true;
  }

  size_t cnt = 0;
  while (++cnt < kIOTrials) {
    if (1 == fread(this, sizeof(ModifyRec), 1, fp)) {
      return true;
    }
  }
  return false;
}

bool ModifyRec::Write(FILE* fp) {
  if (1 == fwrite(this, sizeof(ModifyRec), 1, fp)) {
    return true;
  }

  size_t cnt = 0;
  while (++cnt < kIOTrials) {
    if (1 == fwrite(this, sizeof(ModifyRec), 1, fp)) {
      return true;
    }
  }
  return false;
}

void TableIndexBucket::Add(KeyHash key_hash, DevicePos device_pos) {
}

void TableIndexBucket::Remove(KeyHash key_hash) {
}

}}
