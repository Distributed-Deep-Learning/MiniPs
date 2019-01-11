#pragma once

#include "base/message.hpp"
#include "base/node.hpp"

namespace minips {

    class AbstractMailbox {
    public:
        virtual ~AbstractMailbox() {}

        virtual int Send(const Message &msg) = 0;
    };

}  // namespace minips
