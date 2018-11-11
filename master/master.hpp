
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
        Master(const Node &master_node, const std::vector<Node> &nodes) {
            engine_.reset(new Engine(master_node, nodes, master_node));
            StartMaster(master_node, nodes);
        }

        void StartMaster(const Node &master_node, const std::vector<Node> &nodes) {
            LOG(INFO) << "StartMaster for heartbeat...";
            engine_->CreateIdMapper(1);
            engine_->CreateMailbox();
            engine_->StartMailbox();
            engine_->StartSender();

            // receving heartbeats update from slave nodes
            master_thread_.reset(new MasterThread(master_node.id, nodes));
            engine_->RegisterQueue(master_thread_->GetId(), master_thread_->GetWorkQueue());
            master_thread_->Start();

            // check heartbeats according time
            heartbeat_thread_.reset(new HeartBeatCheckThread(0, nodes, master_thread_.get()));
            heartbeat_thread_->Start();
        }

        void StopMaster() {
            master_thread_->Stop();
            heartbeat_thread_->Stop();

            engine_->StopMailbox(false);
            engine_->StopSender();
            LOG(INFO) << "StopMaster and heatbeat detecting completed.";
        }

    private:
        std::unique_ptr<Engine> engine_;
        std::unique_ptr<MasterThread> master_thread_;
        std::unique_ptr<HeartBeatCheckThread> heartbeat_thread_;
    };

}
