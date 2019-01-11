#pragma once

#include <cinttypes>

namespace minips {

    class AbstractIdMapper {
    public:
        virtual ~AbstractIdMapper() {}

        virtual uint32_t GetNodeIdForThread(uint32_t tid) = 0;
    };

}  // namespace minips
