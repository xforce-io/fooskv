#include "../table_index_bucket.h"

namespace xforce { namespace fooskv {

TableIndexBucket::TableIndexBucket()

bool TableIndexBucket::Init(
    const Config& config,
    NoTable no_table, 
    const std::string& name_table,
    size_t no_bucket,
    bool* end) {
  config_ = &config;
  no_table_ = no_table;
  name_table_ = name_table;
  no_bucket_ = no_bucket;
  XFC_NEW(index_, Index)
  XFC_NEW(bucket_dumper_, new BucketDumper(config, no_table, name_table, no_bucket, end))

  bool ret = bucket_dumper_->Recovery(*index_);
  XFC_FAIL_HANDLE_WARN(!ret, "fail_recovery_table[" 
      << name_table_ 
      << "] no_bucket["
      << no_bucket_
      << "]")
  return true;

  ERROR_HANDLE:
  return false;
}

TableIndexBucket::~TableIndexBucket() {
  XFC_DELETE(bucket_dumper_)
  XFC_DELETE(index_)
}

bool TableIndexBucket::Dump() {
  return bucket_dumper_->Dump(*index);
}

}}
