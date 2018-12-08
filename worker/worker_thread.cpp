
//
// Created by aiyongbiao on 2018/9/28.
//

#include <base/context.hpp>
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

            if (msg.meta.flag == Flag::kCheckpoint) {
                CheckPointResponse();
                continue;
            }

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

//        LOG(INFO) << "WorkerThread WaitRequest:" << app_thread_id << ", " << model_id << "---start";
        cond_.wait(lk, [this, app_thread_id, model_id] {
//            LOG(INFO) << "WorkerThread WaitRequest:" << app_thread_id << ", " << model_id << "---middle, " << tracker_[app_thread_id][model_id].first << "," << tracker_[app_thread_id][model_id].second;
            return tracker_[app_thread_id][model_id].first <= tracker_[app_thread_id][model_id].second;
        });
//        LOG(INFO) << "WorkerThread WaitRequest:" << app_thread_id << ", " << model_id << "---end";
    }

    void WorkerThread::AddResponse(uint32_t app_thread_id, uint32_t model_id, Message &msg) {
        bool recv_finish;

        {
            std::lock_guard<std::mutex> lk(mu_);
            recv_finish = tracker_[app_thread_id][model_id].first == (tracker_[app_thread_id][model_id].second + 1);
        }

        recv_handle_[app_thread_id][model_id](msg);
        if (recv_finish && recv_finish_handle_[app_thread_id][model_id]) {
            recv_finish_handle_[app_thread_id][model_id]();
        }

        {
            std::lock_guard<std::mutex> lk(mu_);
            tracker_[app_thread_id][model_id].second += 1;
//            LOG(INFO) << "AddResponse finish=" << recv_finish << ",id=" << app_thread_id << ",cur=" << tracker_[app_thread_id][model_id].second;
            if (recv_finish) {
                cond_.notify_all();
            }
        }
    }

    void WorkerThread::NewCheckPoint(uint32_t expected_responses) {
        std::lock_guard<std::mutex> lk(mu_);
        checkpoint_expect_ = expected_responses;
        checkpoint_current_ = 0;
    }

    void WorkerThread::WaitCheckPoint() {
        std::unique_lock<std::mutex> lk(mu_);

        cond_.wait(lk, [this] {
            return checkpoint_expect_ == checkpoint_current_;
        });
    }

    void WorkerThread::CheckPointResponse() {
        std::lock_guard<std::mutex> lk(mu_);
        checkpoint_current_ += 1;
        if (checkpoint_expect_ == checkpoint_current_) {
            cond_.notify_all();
        }
    }

    void WorkerThread::RollBackWorker() {
        LOG(INFO) << "RollBackWorker On Node=" << Context::get_instance().get_int32("my_id");
        for (auto app_it = tracker_.begin(); app_it != tracker_.end(); app_it++) {
            uint32_t app_thread_id = app_it->first;
            auto models = app_it->second;
            for (auto model_it = models.begin(); model_it != models.end(); model_it++) {
                uint32_t model_id = model_it->first;
                auto pair = model_it->second;

                {
                    std::lock_guard<std::mutex> lk(mu_);
                    if (pair.first > 0) {
                        tracker_[app_thread_id][model_id].second = pair.first;
                        recv_finish_handle_[app_thread_id][model_id] = 0;
//                        LOG(INFO) << "WorkThread Update:" << app_thread_id << "," << model_id << "," << pair.first << ", " << pair.second;
                        if (tracker_[app_thread_id][model_id].first <= tracker_[app_thread_id][model_id].second) {
                            //recv_finish_handle_[app_thread_id][model_id]();
                            cond_.notify_all();
                        }
                    }
                }
            }
        }
    }

    void WorkerThread::Update() {
        if (checkpoint_expect_ > 0) {
            checkpoint_expect_--;
            if (checkpoint_expect_ == checkpoint_current_) {
                cond_.notify_all();
            }
        }

        for (auto app_it = tracker_.begin(); app_it != tracker_.end(); app_it++) {
            uint32_t app_thread_id = app_it->first;
            auto models = app_it->second;
            for (auto model_it = models.begin(); model_it != models.end(); model_it++) {
                uint32_t model_id = model_it->first;
                auto pair = model_it->second;

                {
                    std::lock_guard<std::mutex> lk(mu_);
                    if (pair.first > 0) {
                        tracker_[app_thread_id][model_id].first -= 1;
//                        LOG(INFO) << "WorkThread Update:" << app_thread_id << "," << model_id << "," << pair.first << ", " << pair.second;
                        if (tracker_[app_thread_id][model_id].first <= tracker_[app_thread_id][model_id].second) {
                            //recv_finish_handle_[app_thread_id][model_id]();
                            cond_.notify_all();
                        }
                    }
                }
            }
        }
    }

}