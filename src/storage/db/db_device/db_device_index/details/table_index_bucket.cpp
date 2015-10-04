#include "../table_index_bucket.h"

namespace xforce { namespace fooskv {

TableIndexBucket::TableIndexBucket(
    const Config& config,
    NoTable no_table, 
    const std::string& name_table,
    size_t no_bucket,
    bool* end) :
  config_(config),
  no_table_(no_table),
  name_table_(name_table),
  no_bucket_(no_bucket),
  index_(new Index),
  bucket_dumper_(new BucketDumper(config, no_table, name_table, no_bucket, end)) {}

TableIndexBucket::~TableIndexBucket() {
  XFC_DELETE(bucket_dumper_)
  XFC_DELETE(index_)
}

bool TableIndexBucket::Dump() {
}

}}
