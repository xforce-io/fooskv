#include "../table_index_bucket.h"

namespace xforce { namespace fooskv {

TableIndexBucket::TableIndexBucket(
    const Config& config,
    NoTable no_table, 
    size_t no_bucket) :
  config_(config),
  no_table_(no_table),
  no_bucket_(no_bucket),
  index_(new Container) {}

TableIndexBucket::~TableIndexBucket() {
  XFC_DELETE(index_)
}

bool TableIndexBucket::Dump() {
}

}}
