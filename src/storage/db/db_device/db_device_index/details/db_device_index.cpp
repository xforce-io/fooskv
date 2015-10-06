#include "../db_device_index.h"

namespace xforce { namespace fooskv {

DBDeviceIndex::DBDeviceIndex() :
  {}

bool DBDeviceIndex::Init(const Config& config) {
  config_ = &config;
  return true;
}

ErrNo DBDeviceIndex::CreateTable(
    NoTable no_table, 
    const std::string& name_table, 
    size_t num_buckets) {
  TableIndex* table_index = new TableIndex;
  bool ret = table_index->Init();
  table_indexes_.insert(std::make_pair(
      no_table, 
      new TableIndex(config, no_table, name_table, num_buckets)));
}

ErrNo DBDeviceIndex::DropTable(NoTable no_table) {
  TableIndex::iterator iter = table_indexes_.find(no_table);
  if (table_indexes_.end() == iter) {
    return kNotExist;
  }

  XFC_DELETE(*iter)
  table_indexes_.erase(no_table);
  return kOK;
}

DBDeviceIndex::~DBDeviceIndex() {
}

}}
