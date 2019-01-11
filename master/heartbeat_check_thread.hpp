
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include "base/actor_model.hpp"
#include "base/node.hpp"
#include "master_thread.hpp"

namespace minips {

    class HeartBeatCheckThread : public Actor {
    public:
        HeartBeatCheckThread(uint32_t checker_thread_id, const std::vector<Node> &nodes,
                             MasterThread *const master_thread)
                : Actor(checker_thread_id), nodes_(nodes), master_thread_(master_thread) {}

        void UpdateNodes(std::vector<Node> &nodes) {
            nodes_ = nodes;
        }

    protected:
        virtual void Main() override;

        std::vector<Node> nodes_;
        MasterThread *const master_thread_;
    };

}
