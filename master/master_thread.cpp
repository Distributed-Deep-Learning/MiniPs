
//
// Created by aiyongbiao on 2018/11/10.
//

#include <base/node.hpp>
#include "master_thread.hpp"
#include "glog/logging.h"

namespace minips {

    void MasterThread::Main() {
        Init();

        while (serving_) {
            Message msg;
            work_queue_.WaitAndPop(&msg);

            if (msg.meta.flag == Flag::kQuitHeartBeat) {
                quit_count_++;
//                LOG(INFO) << "MasterThread heartbeat quit serving on process:" << quit_count_;
                if (quit_count_ >= heartbeats_.size()) {
                    serving_ = false;
                    LOG(INFO) << "[Master] master process quit.";
                    exit(0);
//                    return;
                }
            }

            if (msg.meta.flag == Flag::kHeartBeat) {
                heartbeats_[msg.meta.sender] = time(NULL);
                if (msg.meta.sender == recovering_node_id_) {
                    LOG(INFO) << "[Master] send rollback message to other nodes.";
                    SetRecoveringNodeId(-1);
                    RollBack(msg.meta.sender);
                }
                LOG(INFO) << "[Master] heartbeat updated on node:" << msg.meta.sender;
            }

            if (msg.meta.flag == Flag::kScale) {
                LOG(INFO) << "[Master] send rollback message to other nodes for scale.";
                ScaleRollBack(msg.meta.sender);
            }
        }
    }

}