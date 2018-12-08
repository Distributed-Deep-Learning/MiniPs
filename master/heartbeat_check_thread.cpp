
//
// Created by aiyongbiao on 2018/11/10.
//

#include <base/context.hpp>
#include "heartbeat_check_thread.hpp"
#include "glog/logging.h"
#include <iostream>
#include <string>
#include <base/utils.hpp>

namespace csci5570 {

    void HeartBeatCheckThread::Main() {
        int32_t interval = Context::get_instance().get_int32("heartbeat_interval");
        while (interval > 0 && master_thread_->IsServing()) {
            std::this_thread::sleep_for(std::chrono::seconds(interval));

            if (master_thread_->IsRecovering()) {
                LOG(INFO) << "node is recovering, ignore the restart...";
                continue;
            }

            for (Node node : nodes_) {
                time_t cur_time = time(NULL);
                time_t last_time = master_thread_->GetHeartBeat(node.id);

                if (cur_time - last_time > interval * 3) {
                    CheckFaultTolerance(2);
//                    LOG(INFO) << "node:" << node.id << ", fault detected, start to relaunch the node...";
                    std::string cmd = Context::get_instance().get_string("relaunch_cmd");
                    cmd.append(std::to_string(node.id));
//                    LOG(INFO) << "restart with command:" << cmd;
                    system(cmd.c_str());
                    master_thread_->SetRecoveringNodeId(node.id);
                    break;
                }
            }
        }
    }

}