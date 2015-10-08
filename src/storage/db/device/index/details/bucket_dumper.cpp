#include "../bucket_dumper.h"

namespace xforce { namespace fooskv {

BucketDumper::BucketDumper() :
  index_for_dump_(NULL),
  tid_dumper_(0) {}

bool BucketDumper::Init(
    const Config& config, 
    NoTable no_table,
    const std::string& name_table,
    size_t no_bucket,
    bool* end,
    bool newly_created) :
  config_(config),
  no_table_(no_table),
  name_table_(name_table),
  no_bucket_(no_bucket),
  end_(end),
  newly_created_(newly_created),
  total_len_modify_recs_(0),
  last_dump_sec_(0) {
  bool ret;

  std::stringstream ss;
  ss << config["dumpDir"].AsStr()
      << "/" << name_table 
      << "/" << no_bucket;
  dumpDir = ss.str();

  ss.str("");
  ss << dump_dir_
      << "/" << config["indexFilepath"].AsStr();
  index_filepath_ = ss.str();

  ss.str("");
  ss << dump_dir_ 
      << "/" << config["recsFilepath"].AsStr();
  recs_filepath_ = ss.str();

  ss.str("");
  ss << config["dumpDir"].AsStr()
      << "/" << name_table 
      << "/" << no_bucket 
      << "_tmp";
  tmp_dump_dir_ = ss.str();

  ss.str("");
  ss << tmp_dump_dir_
      << "/" << config["indexFilepath"].AsStr();
  tmp_index_filepath_ = ss.str();

  ss.str("");
  ss << tmp_dump_dir_
      << "/" << config["recsFilepath"].AsStr();
  tmp_recs_filepath_ = ss.str();

  dump_interval_sec_ = config["dumpIntervalSec"].AsStr();

  ret = dump_mark_.Init(config["dumpDir"].AsStr(), config["dumpMarkFilename"].AsStr());
  XFC_FAIL_HANDLE_WARN(!ret, "fail_init_dump_mark table[" 
      << *name_table_
      << "] no_bucket["
      << no_bucket_
      << "]")

  XFC_NEW(index_for_dump_, Index)
  XFC_STOP(0 != pthread_create(&tid_dumper_, NULL, Dumper_, this))
  return true;

  ERROR_HANDLE:
  return false;
}

bool BucketDumper::Recovery(Index& index) {
  if (!newly_created_) {
    return RecoveryIndex_(index) && RecoveryRecs_();
  } else {
    std::stringstream ss;
    ss << "mkdir -p " << dump_dir_;
    return 0 == system(ss.str().c_str());
  }
}

const DevicePos* BucketDumper::GetReplayPos() {  
  return dump_mark_.RestoreAndGet();
}

bool BucketDumper::Dump_(DevicePos device_pos, const Index& index) {
  time_t cur_sec = Time::GetCurrentSec(true);
  if (!is_dumping_.load()) {
    if (cur_sec-last_dump_sec_ > dump_interval_sec_) {
      *index_for_dump_ = index;
      dump_mark_.Set(device_pos);
      return DumpStart_(index);
    }
  } else {
    return DumpEnd_();
  }
  return true;
}

BucketDumper::~BucketDumper() {
  if (0!=tid_dumper_) {
    pthread_join(tid_dumper_, NULL);
  }

  if (NULL!=index_for_dump_) {
    XFC_DELETE(index_for_dump_)
  }
}

bool BucketDumper::RecoveryIndex_(Index& index) {
  bool ret;
  size_t num_recs = 0;

  FILE* fp = fopen(index_filepath_.c_str(), "r");
  XFC_FAIL_HANDLE_WARN(NULL==fp, "fail_open_file[" << index_filepath_ << "]")

  ret = StorageIOHelper::Read(&num_recs, sizeof(num_recs), 1, fp);
  XFC_FAIL_HANDLE(1!=ret)

  for (size_t i=0; i<num_recs; ++i) {
    KeyHash key_hash;
    DevicePos device_pos;
    ret = StorageIOHelper::Read(&key_hash, sizeof(key_hash), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)

    ret = StorageIOHelper::Read(&device_pos, sizeof(device_pos), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)

    index.Insert(key_hash, device_pos);
  }
  fclose(fp);
  return true;

  ERROR_HANDLE:
  fclose(fp);
  WARN("fail_recover_index");
  return false;
}

bool BucketDumper::RecoveryRecs_() {
  bool ret;
  size_t num_recs = 0;

  FILE* fp = fopen(recs_filepath_.c_str(), "r");
  XFC_FAIL_HANDLE_WARN(NULL==fp, "fail_open_file_for_read[" << index_filepath_ << "]")

  ret = StorageIOHelper::Read(&num_recs, sizeof(num_recs), 1, fp);
  XFC_FAIL_HANDLE(1!=ret)

  for (size_t i=0; i<num_recs; ++i) {
    ModifyRec modify_rec;
    ret = StorageIOHelper::Read(&modify_rec, sizeof(modify_rec), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)

    modify_recs_.push_back(modify_rec);
  }
  total_len_modify_recs_ = num_recs;
  fclose(fp);
  return true;

  ERROR_HANDLE:
  fclose(fp);
  WARN("fail_recover_recs");
  return false
}

void BucketDumper::DumpStart_(const Index& index) {
  is_dumping_.store(true);
  if (total_len_modify_recs_ + modify_recs_.size() > config["modifyRecThreshold"]) {
    deep_dump_.store(true);
    *index_for_dump_ = index;
    cur_index_modify_rec_ = modify_recs_.size();
  } else {
    deep_dump_.store(false);
    cur_index_modify_rec_ = modify_recs_.size();
  }
  error_.store(false);
  dump_end_.store(false);
}

void BucketDumper::DumpEnd_() {
  if (!dump_end_.load()) {
    return;
  }

  if (!error_.load()) {
    if (deep_dump_.load()) {
      total_len_modify_recs_ = 0;
    } else {
      total_len_modify_recs_ += modify_recs_.size();
    }

    for (size_t i=cur_index_modify_rec_; i < modify_recs_.size(); ++i) {
      modify_recs_[i-cur_index_modify_rec_] = modify_recs_[i];
    }
    modify_recs_.resize(modify_recs_.size() - cur_index_modify_rec_);
    dump_mark_.Save();
  }

  deep_dump_.store(false);
  is_dumping_.store(false);
}

bool BucketDumper::DumpIndex_(const Index& index) {
  bool ret;

  FILE* fp = fopen(tmp_index_filepath_.c_str(), "a");
  XFC_FAIL_HANDLE_WARN(NULL==fp, "fail_open_file_for_write[" << tmp_index_filepath_ << "]")

  ret = StorageIOHelper::Write(index.Size(), sizeof(size_t), 1, fp);
  XFC_FAIL_HANDLE(1!=ret)

  Index::ConstIterator iter = index.Begin();
  while (iter != index.End()) {
    const KeyHash& key_hash = iter.GetKey();
    const DevicePos& device_pos = iter.GetVal();

    ret = StorageIOHelper::Write(&key_hash, sizeof(key_hash), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)

    ret = StorageIOHelper::Write(&device_pos, sizeof(device_pos), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)
  }
  fclose(fp);
  return true;

  ERROR_HANDLE:
  fclose(fp);
  return false;
}

bool BucketDumper::DumpRecs_(size_t from, size_t to, bool is_append) {
  bool ret;

  FILE* fp = fopen(tmp_recs_filepath_.c_str(), is_append ? "a" : "w");
  XFC_FAIL_HANDLE_WARN(NULL==fp, "fail_open_file_for_write[" << tmp_recs_filepath_ << "]")

  ret = StorageIOHelper::Write(to-from, sizeof(size_t), 1, fp);
  XFC_FAIL_HANDLE(1!=ret)

  for (size_t i=from; i<to; ++i) {
    ret = StorageIOHelper::Write(&(modify_recs_[i]), sizeof(ModifyRec), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)
  }
  fclose(fp);
  return true;

  ERROR_HANDLE:
  fclose(fp);
  return false;
}

bool BucketDumper::RealDump_() {
  if (dump_end_.load() || is_dumping_.load()) {
    return false;
  }

  mkdir(tmp_dump_dir_);
  remove(tmp_index_filepath_.c_str());
  remove(tmp_recs_filepath_.c_str());
   
  if (deep_dump_.load()) {
    if (!DumpIndex_(*index_for_dump_)) {
      error_.store(true);
      FATAL("fail_dump_index_for_table[" << name_table_ << "]");
    }
  } else {
    if (!DumpRecs_(0, cur_index_modify_rec_)) {
      error_.store(true);
      FATAL("fail_dump_recs_for_table[" << name_table_ << "]");
    }
  }

  if (!error_.load()) {
    if (0 != rename(tmp_dump_dir_.c_str(), dump_dir_.c_str())) {
      error_.store(true);
      FATAL("fail_rename[" << tmp_dump_dir_<< "|" << dump_dir_ << "]");
    }
  }

  dump_end_.store(true);
  return true;
}

void* BucketDumper::Dumper_(void* arg) {
  Self* self = RCAST<Self*>(arg);
  while (!(*end_)) {
    if (!self->RealDump_()) {
      usleep(10*1000);
    }
  }
  self->RealDump_();
  return NULL;
}

}}
