#include "../table_index.h"

namespace xforce { namespace fooskv {

TableIndex::TableIndex() :
  table_index_buckets_(NULL) {}

bool TableIndex::Init(
    const Config& config, 
    NoTable no_table, 
    const std::string& name_table,
    size_t num_buckets) {
  num_buckets_ = num_buckets;
  table_index_buckets_ = new TableIndexBucket [num_buckets];
  for (size_t i=0; i<num_buckets_; ++i) {
    XFC_NEW(table_index_buckets_[i], TableIndexBucket(config, no_table, name_table, i))
  }
}

bool TableIndex::Dump() {
  for (size_t i=0; i<num_buckets_; ++i) {
    if (!table_index_buckets_[i]->Dump()) {
      return false;
    }
  }
  return true;
}

TableIndex::~TableIndex() {
  for (size_t i=0; i<num_buckets_; ++i) {
    XFC_DELETE(table_index_buckets_[i])
  }
  XFC_DELETE_ARRAY(table_index_buckets_)
}

}} 
