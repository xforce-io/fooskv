#include "../bucket_dumper.h"

namespace xforce { namespace fooskv {

BucketDumper::BucketDumper() :
  index_for_dump_(NULL),
  tid_dumper_(0),
  fout_(NULL) {}

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
  fout_(NULL),
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

BucketDumper::~BucketDumper() {
  if (NULL!=fout_) {
    fclose(fout_);
  }

  if (0!=tid_dumper_) {
    pthread_join(tid_dumper_, NULL);
  }

  if (NULL!=index_for_dump_) {
    XFC_DELETE(index_for_dump_)
  }
}

bool BucketDumper::RecoveryIndex_(Index& index) {
}

bool BucketDumper::RecoveryRecs_() {
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
