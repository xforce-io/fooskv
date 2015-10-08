#include "../index.h"

namespace xforce { namespace fooskv {

Index::Index() :
  last_dump_sec_(0) {}

bool Index::Init(const Config& config, bool* end) {
  config_ = &config;
  end_ = end;
  return true;

  ERROR_HANDLE:
  return false;
}

ErrNo Index::CreateTable(
    NoTable no_table, 
    const std::string& name_table, 
    size_t num_buckets) {
  TableIndex* table_index = new TableIndex;
  bool ret = table_index->Init(*config_, no_table, name_table, num_buckets, end_, true);
  table_indexes_.insert(std::make_pair(
      no_table, 
      new TableIndex(config, no_table, name_table, num_buckets)));
}

ErrNo Index::DropTable(NoTable no_table) {
  TableIndex::iterator iter = table_indexes_.find(no_table);
  if (table_indexes_.end() == iter) {
    return kNotExist;
  }

  XFC_DELETE(*iter)
  table_indexes_.erase(no_table);
  return kOK;
}

Index::~Index() {
}

}}
