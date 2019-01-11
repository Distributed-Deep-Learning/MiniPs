#pragma once

#include <cinttypes>
#include "base/third_party/sarray.h"

namespace minips {

    using Key = uint32_t;

    using Keys = third_party::SArray<Key>;
    using KVPairs = std::pair<third_party::SArray<Key>, third_party::SArray<double>>;

    using SVMItem = std::pair<std::vector<std::pair<int, double>>, double>;

}  // namespace minips
