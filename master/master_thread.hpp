
//
// Created by aiyongbiao on 2018/11/10.
//

#pragma once

#include <unordered_map>
#include "base/actor_model.hpp"

namespace minips {

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

        void RollBack(int32_t failed_node_id) {
            rollback_func_(failed_node_id);
        }

        void SetRollBack(std::function<void(int32_t)> func) {
            rollback_func_ = func;
        }

        void ScaleRollBack(int32_t failed_node_id) {
            scale_rollback_func_(failed_node_id);
        }

        void SetScaleRollBack(std::function<void(int32_t)> func) {
            scale_rollback_func_ = func;
        }

        void UpdateNodes(std::vector<Node> &nodes, int32_t failed_node_id) {
            nodes_ = nodes;
            heartbeats_.erase(failed_node_id);
        }

        void SetRecoveringNodeId(int32_t recovering_node_id) {
            recovering_node_id_ = recovering_node_id;
        }

        bool IsRecovering() {
            return recovering_node_id_ != -1;
        }

    protected:
        virtual void Main() override;

        std::unordered_map<uint32_t, time_t> heartbeats_;
        bool serving_;
        int32_t quit_count_;
        std::vector<Node> nodes_;

        std::function<void(int32_t)> rollback_func_;
        std::function<void(int32_t)> scale_rollback_func_;

        int32_t recovering_node_id_ = -1;
    };

}