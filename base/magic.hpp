#pragma once

#include <cinttypes>
#include "base/third_party/sarray.h"

namespace csci5570 {

    using Key = uint32_t;

    using Keys = third_party::SArray<Key>;
    using KVPairs = std::pair<third_party::SArray<Key>, third_party::SArray<double>>;

}  // namespace csci5570
