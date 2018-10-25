
//
// Created by aiyongbiao on 2018/9/28.
//

#include "worker_thread.hpp"
#include "glog/logging.h"

namespace csci5570 {

    void WorkerThread::Check(uint32_t app_thread_id, uint32_t model_id) {
        CHECK(recv_handle_.find(app_thread_id) != recv_handle_.end()) << "recv_handle_ for app_thread_id:" << app_thread_id
                                                                      << " is not registered";
        CHECK(recv_handle_[app_thread_id].find(model_id) != recv_handle_[app_thread_id].end())
        << "recv_handle_ for model:" << model_id << " is not registered";

        CHECK(recv_finish_handle_.find(app_thread_id) != recv_finish_handle_.end())
        << "recv_finish_handle_ for model:" << app_thread_id << " is not registered";
        CHECK(recv_finish_handle_[app_thread_id].find(model_id) != recv_finish_handle_[app_thread_id].end())
        << "recv_finish_handle_ for model:" << model_id << " is not registered";
    }

    void WorkerThread::Main() {
        while (true) {
            Message msg;
            work_queue_.WaitAndPop(&msg);

            if (msg.meta.flag == Flag::kExit)
                break;

            AddResponse(msg.meta.recver, msg.meta.model_id, msg);
        }
    }

    void WorkerThread::RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                          const std::function<void(Message &)> &recv_handle) {
        std::lock_guard<std::mutex> lk(mu_);
        recv_handle_[app_thread_id][model_id] = recv_handle;
    }

    void WorkerThread::RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                                const std::function<void()> &recv_finish_handle) {
        std::lock_guard<std::mutex> lk(mu_);
        recv_finish_handle_[app_thread_id][model_id] = recv_finish_handle;
    }

    void WorkerThread::NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) {
        std::lock_guard<std::mutex> lk(mu_);
        Check(app_thread_id, model_id);
        tracker_[app_thread_id][model_id] = {expected_responses, 0};
    }

    void WorkerThread::WaitRequest(uint32_t app_thread_id, uint32_t model_id) {
        Check(app_thread_id, model_id);
        std::unique_lock<std::mutex> lk(mu_);

        cond_.wait(lk, [this, app_thread_id, model_id] {
            return tracker_[app_thread_id][model_id].first == tracker_[app_thread_id][model_id].second;
        });
    }

    void WorkerThread::AddResponse(uint32_t app_thread_id, uint32_t model_id, Message &msg) {
        bool recv_finish;

        {
            std::lock_guard<std::mutex> lk(mu_);
            recv_finish = tracker_[app_thread_id][model_id].first == (tracker_[app_thread_id][model_id].second + 1);
        }

        recv_handle_[app_thread_id][model_id](msg);
        if (recv_finish) {
            recv_finish_handle_[app_thread_id][model_id]();
        }

        {
            std::lock_guard<std::mutex> lk(mu_);
            tracker_[app_thread_id][model_id].second += 1;
            if (recv_finish) {
                cond_.notify_all();
            }
        }
    }

}