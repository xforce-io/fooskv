#pragma once

namespace fooskv {

class DB {
 public:
  int AddKV(const KV& kv);
  int RemoveKV(const Key& key);
  const KV& FindKV(const Key& key);

 private:
};

}


