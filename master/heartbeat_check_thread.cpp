
//
// Created by aiyongbiao on 2018/11/10.
//

#include "heartbeat_check_thread.hpp"
#include "glog/logging.h"

namespace csci5570 {

    void HeartBeatCheckThread::Main() {
        while (interval_threshold_ > 0 && serving_) {
            std::this_thread::sleep_for(std::chrono::seconds(interval_threshold_));

            for (Node node : nodes_) {
                time_t cur_time = time(NULL);
                time_t last_time = heartbeats_[node.id];

                if (cur_time - last_time > interval_threshold_ * 2) {
                    LOG(INFO) << "node:" << node.id << ", fault detected...";
                }
            }
        }
    }

}