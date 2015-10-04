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
    bool* end) :
  config_(config),
  no_table_(no_table),
  name_table_(name_table),
  no_bucket_(no_bucket),
  end_(end),
  last_index_modify_rec_(0) {

  std::stringstream ss;
  ss << config["dumpDir"] 
      << "/" << name_table 
      << "/" << no_bucket 
      << "/" << config["indexFilepath"];
  index_filepath_ = ss.str();
  tmp_index_filepath_ = index_filepath_ + "_tmp";

  ss.str("");
  ss << config["dumpDir"] 
      << "/" << name_table 
      << "/" << no_bucket 
      << "/" << config["recsFilepath"];
  recs_filepath_ = ss.str();
  tmp_recs_filepath_ = recs_filepath_ + "_tmp";

  XFC_NEW(index_for_dump_, Index)
  XFC_STOP(0 != pthread_create(&tid_dumper_, NULL, Dumper_, this))
  return true;

  ERROR_HANDLE:
  return false;
}

bool BucketDumper::Recovery(Index& index) {
  return RecoveryIndex_(index) && RecoveryRecs_();
}

bool BucketDumper::Dump(const Index& index) {
  if (!is_dumping_) {
    cur_index_modify_rec_ = modify_recs_.size();
    dump_end_ = false;
    is_dumping_ = true;
  } else {
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
  XFC_FAIL_HANDLE_WARN(NULL==fp, "fail_open_file[" << index_filepath_ << "]")

  ret = StorageIOHelper::Read(&num_recs, sizeof(num_recs), 1, fp);
  XFC_FAIL_HANDLE(1!=ret)

  for (size_t i=0; i<num_recs; ++i) {
    ModifyRec modify_rec;
    ret = StorageIOHelper::Read(&modify_rec, sizeof(modify_rec), 1, fp);
    XFC_FAIL_HANDLE(1!=ret)

    modify_recs_.push_back(modify_rec);
    last_index_modify_rec_ = modify_rec.size();
  }
  fclose(fp);
  return true;

  ERROR_HANDLE:
  fclose(fp);
  WARN("fail_recover_recs");
  return false
}

bool BucketDumper::Dump_() {
}

void* BucketDumper::Dumper_(void* arg) {
  Self* self = RCAST<Self*>(arg);
  while (!(*end_)) {
    if (!self->Dump_()) {
      usleep(10*1000);
    }
  }
  self->Dump_();
  return NULL;
}

}}
