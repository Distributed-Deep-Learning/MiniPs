
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include <unordered_map>
#include "base/actor_model.hpp"

namespace csci5570 {

    class MasterThread : public Actor {
    public:
        MasterThread(uint32_t master_id, const std::vector<Node> &nodes) : Actor(master_id), nodes_(nodes) {}

        void Init() {
            serving_ = true;
            quit_count_ = 0;
            for (Node node : nodes_) {
                heartbeats_[node.id] = time(NULL);
            }
        }

        time_t GetHeartBeat(uint32_t node_id) {
            return heartbeats_[node_id];
        }

        bool IsServing() {
            return serving_;
        }

    protected:
        virtual void Main() override;

        std::unordered_map<uint32_t, time_t> heartbeats_;
        bool serving_;
        int32_t quit_count_;
        std::vector<Node> nodes_;
    };

}