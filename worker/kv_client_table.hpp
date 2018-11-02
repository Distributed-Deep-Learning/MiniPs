#pragma once

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "glog/logging.h"
#include <cinttypes>
#include <vector>
#include "base/message.hpp"
#include <algorithm>

namespace csci5570 {

    /**
     * Provides the API to users, and implements the worker-side abstraction of model
     * Each model in one application is uniquely handled by one KVClientTable
     *
     * @param Val type of model parameter values
     */
    template<typename Val>
    class KVClientTable {
    public:
        /**
         * @param app_thread_id       user thread id
         * @param model_id            model id
         * @param sender_queue        the work queue of a sender communication thread
         * @param partition_manager   model partition manager
         * @param callback_runner     callback runner to handle received replies from servers
         */
        KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue <Message> *const sender_queue,
                      const AbstractPartitionManager *const partition_manager,
                      AbstractCallbackRunner *const callback_runner)
                : app_thread_id_(app_thread_id),
                  model_id_(model_id),
                  sender_queue_(sender_queue),
                  partition_manager_(partition_manager),
                  callback_runner_(callback_runner) {
            callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_, [&](Message &msg) { HandleMsg_(msg); });
        };

        void Clock() {
            Clock_();
        };

        void Add(const std::vector<Key> &keys, const std::vector<Val> &vals) {
            Add_(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
        };

        void Get(const std::vector<Key> &keys, std::vector<Val> *vals) {
            Get_(third_party::SArray<Key>(keys), vals);
        };

        void Add(const third_party::SArray <Key> &keys, const third_party::SArray <Val> &vals) {
            Add_(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
        };

        void Get(const third_party::SArray <Key> &keys, third_party::SArray <Val> *vals) {
            Get_(keys, vals);
        };

        void CheckPoint() {
            CheckPoint_();
        }

    private:
        template<typename V>
        void HandleFinish_(const third_party::SArray <Key> &keys, V *vals) {
            size_t total_key = 0, total_val = 0;
            for (const auto &s : recv_kvs_) {
                third_party::Range range = third_party::FindRange(keys, s.first.front(), s.first.back() + 1);
                CHECK_EQ(range.size(), s.first.size()) << "unmatched keys size from one server";
                total_key += s.first.size();
                total_val += s.second.size();
            }
            CHECK_EQ(total_key, keys.size()) << "lost some servers?";
            std::sort(recv_kvs_.begin(), recv_kvs_.end(),
                      [](const KVPairs &a, const KVPairs &b) { return a.first.front() < b.first.front(); });
            CHECK_NOTNULL(vals);
            vals->resize(total_val);
            Val *p_vals = vals->data();
            for (const auto &s : recv_kvs_) {
                memcpy(p_vals, s.second.data(), s.second.size() * sizeof(Val));
                p_vals += s.second.size();
            }
            recv_kvs_.clear();
        }

        void HandleMsg_(Message &msg) {
            CHECK_EQ(msg.data.size(), 2);
            KVPairs kvPairs = std::make_pair(third_party::SArray<Key>(msg.data[0]),
                                             third_party::SArray<double>(msg.data[1]));
            recv_kvs_.push_back(kvPairs);
        }

        // Send sliced data to the queue, the queue was listened by workers
        void Send_(const std::vector<std::pair<int, KVPairs>> &sliced, bool is_add) {
            CHECK_NOTNULL(partition_manager_);
            for (size_t i = 0; i < sliced.size(); i++) {
                Message msg;
                msg.meta.sender = app_thread_id_;
                msg.meta.recver = sliced[i].first;
                msg.meta.model_id = model_id_;
                msg.meta.flag = is_add ? Flag::kAdd : Flag::kGet;
                const auto &kvs = sliced[i].second;
                if (kvs.first.size()) {
                    msg.AddData(kvs.first);
                    if (is_add) {
                        msg.AddData(kvs.second);
                    }
                }
                sender_queue_->Push(std::move(msg));
            }
        };

        void Clock_() {
            CHECK_NOTNULL(partition_manager_);
            const auto& server_thread_ids = partition_manager_->GetServerThreadIds();
            for (uint32_t server_id : server_thread_ids) {
                Message msg;
                msg.meta.sender = app_thread_id_;
                msg.meta.recver = server_id;
                msg.meta.model_id = model_id_;
                msg.meta.flag = Flag::kClock;
                sender_queue_->Push(std::move(msg));
            }
        }

        void CheckPoint_() {
            CHECK_NOTNULL(partition_manager_);
            const auto& server_thread_ids = partition_manager_->GetServerThreadIds();
            for (uint32_t server_id : server_thread_ids) {
                Message msg;
                msg.meta.sender = app_thread_id_;
                msg.meta.recver = server_id;
                msg.meta.model_id = model_id_;
                msg.meta.flag = Flag::kCheckpoint;
                sender_queue_->Push(std::move(msg));
            }
        }

        void Add_(const third_party::SArray <Key> &keys, const third_party::SArray <Val> &vals) {
            CHECK_NOTNULL(partition_manager_);
            std::vector<std::pair<int, KVPairs>> sliced;
            partition_manager_->Slice(std::make_pair(keys, vals), &sliced);
            Send_(sliced, true);
        }

        template<typename V>
        void Get_(const third_party::SArray <Key> &keys, V *vals) {
            // Slice the data, vals should be empty, as it will return in the HandleFinish_
            std::vector<std::pair<int, KVPairs>> sliced;
            partition_manager_->Slice(std::make_pair(keys, third_party::SArray<Val>()), &sliced);

            // The FinishHandle will be called when callback_runner_.AddResponse was called for each worker
            callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_,
                                                       [&]() { HandleFinish_(keys, vals); });
            // Init the request count, the count will be used in WaitRequest
            callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());

            // Send sliced data to all the workers
            Send_(sliced, false);

            // Wait callback_runner_.AddResponse to be called, and the number should equals to sliced.size()
            callback_runner_->WaitRequest(app_thread_id_, model_id_);
        }

        uint32_t app_thread_id_;  // identifies the user thread
        uint32_t model_id_;       // identifies the model on servers

        ThreadsafeQueue <Message> *const sender_queue_;             // not owned
        AbstractCallbackRunner *const callback_runner_;            // not owned
        const AbstractPartitionManager *const partition_manager_;  // not owned

        std::vector<KVPairs> recv_kvs_;

    };

}  // namespace csci5570
