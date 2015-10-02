#pragma once

#include "public.h"

namespace xforce { namespace fooskv {

class DBDevice {
 public:
  bool Init(const Config& config);

  ErrNo Add(const KV& kv);
  ErrNo Remove(const KV& kv);
 
 private: 
};

}}
