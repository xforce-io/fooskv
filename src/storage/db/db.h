#pragma once

namespace fooskv {

class DB {
 public:
  int AddKV(const KVBatch& kv_batch);
  int RemoveKV(const KeyBatch& key_batch);
  const KV& FindKV(const KeyBatch& key_batch);

 private:
};

}


