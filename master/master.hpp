
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include <driver/engine.hpp>
#include "master_thread.hpp"
#include "heartbeat_check_thread.hpp"

namespace csci5570 {

    class Master {

    public:
        Master(const Node &master_node, const std::vector<Node> &nodes) : serving_(true) {
            engine_.reset(new Engine(master_node, nodes, master_node));
            StartMaster(master_node, nodes);
        }

        void StartMaster(const Node &master_node, const std::vector<Node> &nodes) {
            LOG(INFO) << "StartMaster for heartbeat...";
            engine_->CreateIdMapper(1);
            engine_->CreateMailbox();
            engine_->StartMailbox();
            engine_->StartSender();

            for (Node node : nodes) {
                heartbeats_[node.id] = time(NULL);
            }

            // receving heartbeats update from slave nodes
            master_thread_.reset(new MasterThread(master_node.id, heartbeats_, serving_));
            engine_->RegisterQueue(master_thread_->GetId(), master_thread_->GetWorkQueue());
            master_thread_->Start();

            // check heartbeats according time
            heartbeat_thread_.reset(new HeartBeatCheckThread(0, heartbeats_, nodes, serving_));
            heartbeat_thread_->Start();
        }

        void StopMaster() {
            master_thread_->Stop();
            heartbeat_thread_->Stop();

            LOG(INFO) << "StopMaster for heartbeat...";
            engine_->StopMailbox(false);
            LOG(INFO) << "StopMaster for heartbeat...1";
            engine_->StopSender();
            LOG(INFO) << "StopMaster for heartbeat...2";
        }

    private:
        std::unique_ptr<Engine> engine_;
        std::unique_ptr<MasterThread> master_thread_;
        std::unique_ptr<HeartBeatCheckThread> heartbeat_thread_;

        std::unordered_map<uint32_t, time_t> heartbeats_;
        bool serving_;
    };

}
