#pragma once

namespace fooskv {

struct Key {
  char* buf;
  size_t size;
};

struct KV {
  char *buf;
  size_t size;
  size_t size_key;
};

}
