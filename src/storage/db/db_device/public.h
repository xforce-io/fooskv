#pragma once

#include "../public.h"

namespace xforce { namespace fooskv {

struct DeviceCmd {
  enum Cmd {
    kUninit,
    kCreateTable,
    kDropTable,
    kAddRecord,
    kRemoveRecord,
    kNumCmd,
  };
};

}}
