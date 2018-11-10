
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include <unordered_map>
#include "base/actor_model.hpp"

namespace csci5570 {

    class MasterThread : public Actor {
    public:
        MasterThread(uint32_t master_id, std::unordered_map<uint32_t, time_t> &heartbeats, bool &serving)
                : Actor(master_id), heartbeats_(heartbeats), serving_(serving) {}

    protected:
        virtual void Main() override;

        std::unordered_map<uint32_t, time_t> heartbeats_;
        bool &serving_;
        int32_t quit_count_;
    };

}