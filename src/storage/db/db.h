#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

/*
 * TPD stands for thread private data
 */
struct DBTPD {
  bool EndOfWriteCmd;
};

class DB {
 public:
  DB();

  bool Init(const Config& config, bool* end);

  void WriteCmd(DBCmd& db_cmd);

  const KV& FindKV(const KeyBatch& key_batch);

  virtual ~DB();

 private:
  DBBase* db_base_;
};

}}
