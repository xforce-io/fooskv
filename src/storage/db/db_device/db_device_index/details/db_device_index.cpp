#include "../db_device_index.h"

namespace xforce { namespace fooskv {

DBDeviceIndex::DBDeviceIndex() :
  {}

bool DBDeviceIndex::Init(const Config& config);

ErrNo DBDeviceIndex::CreateTable(NoTable no_table, size_t num_buckets) {
  table_indexes_.insert(std::make_pair(no_table, new TableIndex(no_table, num_buckets)));
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
