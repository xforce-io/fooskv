#include "../table_index_bucket.h"

namespace xforce { namespace fooskv {

TableIndexBucket::TableIndexBucket(
    const Config& config,
    NoTable no_table, 
    const std::string& name_table,
    size_t no_bucket,
    bool* end,
    bool newly_created) :
  config_(&config),
  no_table_(no_table),
  name_table_(name_table),
  no_bucket_(no_bucket),
  end_(end),
  newly_created_(newly_created),
  has_recover_(false) {
  XFC_NEW(index_, Index)
  XFC_NEW(bucket_dumper_, 
      new BucketDumper(config, no_table, name_table, no_bucket, end))
}

bool TableIndexBucket::Recover() {
  bool ret = bucket_dumper_->Recovery(*index_);
  if (ret) {
    has_recover_ = true;
    return true;
  } else {
    FATAL("fail_recovery_table[" 
      << name_table_ 
      << "] no_bucket["
      << no_bucket_
      << "]");
    return false;
  }
}

const DevicePos* TableIndexBucket::GetReplayPos() {
  return bucket_dumper_->GetReplayPos();
}

TableIndexBucket::~TableIndexBucket() {
  XFC_DELETE(bucket_dumper_)
  XFC_DELETE(index_)
}

}}
