
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include <driver/engine.hpp>
#include "master_thread.hpp"
#include "heartbeat_check_thread.hpp"

namespace minips {

    class Master {

    public:
        Master(const Node &master_node, const std::vector<Node> &nodes) : nodes_(nodes) {
            engine_.reset(new Engine(master_node, nodes, master_node));
            StartMaster(master_node, nodes);
        }

        void StartMaster(const Node &master_node, const std::vector<Node> &nodes) {
            LOG(INFO) << "[Master] startMaster for heartbeat...";
            engine_->CreateIdMapper(1);
            engine_->CreateMailbox();
            engine_->StartMailbox();
            engine_->StartSender();

            // receving heartbeats update from slave nodes
            master_thread_.reset(new MasterThread(master_node.id, nodes));
            std::function<void(int32_t)> func = [this](int32_t failed_node_id){
                RollBack(failed_node_id);
            };

            std::function<void(int32_t)> scale_func = [this](int32_t failed_node_id){
                ScaleRollBack(failed_node_id);
            };
            master_thread_->SetRollBack(func);
            master_thread_->SetScaleRollBack(scale_func);
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

        void RollBack(int failed_node_id) {
            LOG(INFO) << "Rollback with failed node:" << failed_node_id;
            engine_->RollBack(failed_node_id);

            nodes_.erase(std::remove_if(nodes_.begin(), nodes_.end(), [&](Node const &node) {
                return failed_node_id == node.id;
            }), nodes_.end());

            master_thread_->UpdateNodes(nodes_, failed_node_id);
            heartbeat_thread_->UpdateNodes(nodes_);
        }

        void ScaleRollBack(int failed_node_id) {
            LOG(INFO) << "ScaleRollback with failed node:" << failed_node_id;
            engine_->ScaleRollBack(failed_node_id);

            nodes_.erase(std::remove_if(nodes_.begin(), nodes_.end(), [&](Node const &node) {
                return failed_node_id == node.id;
            }), nodes_.end());

            master_thread_->UpdateNodes(nodes_, failed_node_id);
            heartbeat_thread_->UpdateNodes(nodes_);
        }

    private:
        std::unique_ptr<Engine> engine_;
        std::unique_ptr<MasterThread> master_thread_;
        std::unique_ptr<HeartBeatCheckThread> heartbeat_thread_;

        std::vector<Node> nodes_;
    };

}
