#include "../db.h"

namespace xforce { namespace fooskv {

DB::DB() :
  db_base_(NULL) {}

bool DB::Init(const Config& config, bool* end) {
  XFC_NEW(db_base_, DBBase)

  bool ret = db_base_->Init(config, end);
  XFC_FAIL_HANDLE_WARN(!ret, "fail_init_db_base")
  return true;

  ERROR_HANDLE:
  return false;
}

void DB::WriteCmd(DBCmd& db_cmd) {
  db_base_->WriteCmd(db_cmd);
}

const KV& DB::FindKV(const Key& key) {
}

DB::~DB() {
  XFC_DELETE(db_base_)
}

}}
