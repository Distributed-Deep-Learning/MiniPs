
//
// Created by aiyongbiao on 2018/11/10.
//

#include <base/context.hpp>
#include "heartbeat_check_thread.hpp"
#include "glog/logging.h"

namespace csci5570 {

    void HeartBeatCheckThread::Main() {
        int32_t interval = Context::get_instance().get_int32("heartbeat_interval");
        while (interval > 0 && master_thread_->IsServing()) {
            std::this_thread::sleep_for(std::chrono::seconds(interval));

            for (Node node : nodes_) {
                time_t cur_time = time(NULL);
                time_t last_time = master_thread_->GetHeartBeat(node.id);

                if (cur_time - last_time > interval * 5) {
                    LOG(INFO) << "node:" << node.id << ", fault detected...";
                }
            }
        }
    }

}