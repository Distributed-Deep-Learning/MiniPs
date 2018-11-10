
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include "base/actor_model.hpp"
#include "base/node.hpp"

namespace csci5570 {

    class HeartBeatCheckThread : public Actor {
    public:
        HeartBeatCheckThread(uint32_t checker_thread_id, std::unordered_map<uint32_t, time_t> &heartbeats,
                             const std::vector<Node> &nodes, bool &serving, uint32_t interval = 20) : Actor(checker_thread_id),
                                                                                      heartbeats_(heartbeats),
                                                                                      nodes_(nodes), serving_(serving),
                                                                                      interval_threshold_(interval) {}

    protected:
        virtual void Main() override;

        std::unordered_map<uint32_t, time_t> heartbeats_;
        uint32_t interval_threshold_;
        std::vector<Node> nodes_;
        bool &serving_;
    };

}
