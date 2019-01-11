#pragma once

#include <cinttypes>
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "base/node.hpp"

namespace minips {

    class AbstractModel {
    public:
        virtual void Clock(Message &msg) = 0;

        virtual void Add(Message &msg) = 0;

        virtual void Get(Message &msg) = 0;

        virtual int GetProgress(int tid) = 0;

        virtual void ResetWorker(Message &msg) = 0;

        virtual void Dump(Message &msg) = 0;

        virtual void Restore() = 0;

        virtual void Update(int failed_node_id, std::vector<Node> &nodes, third_party::Range &range) = 0;

        virtual ~AbstractModel() {}
    };

}  // namespace minips
