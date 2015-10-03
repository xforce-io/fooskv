#include "../db_device_index.h"

namespace xforce { namespace fooskv {

DBDeviceIndex::DBDeviceIndex() :
  {}

bool DBDeviceIndex::Init(const Config& config);

ErrNo DBDeviceIndex::CreateTable(NoTable no_table, size_t num_buckets) {
  table_indexes_.insert(std::make_pair(no_table));
}

ErrNo DBDeviceIndex::Add(KeyHash key_hash, DevicePos device_pos) {
}

ErrNo DBDeviceIndex::Remove(KeyHash key_hash) {
}

DBDeviceIndex::~DBDeviceIndex() {
}

}}
