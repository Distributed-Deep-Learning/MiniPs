#include "server/consistency/ssp_model.hpp"
#include "glog/logging.h"
#include <base/utils.hpp>
#include <base/node.hpp>
#include <lib/svm_dumper.hpp>
#include <thread>

namespace minips {

    SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage> &&storage_ptr, int staleness,
                       ThreadsafeQueue<Message> *reply_queue)
            : model_id_(model_id), staleness_(staleness), reply_queue_(reply_queue) {
        this->storage_ = std::move(storage_ptr);
        if (Context::get_instance().get_bool("use_weight_file")) {
            Restore();
        }
    }

    void SSPModel::Clock(Message &msg) {
        int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
        if (updated_min_clock != -1) {  // min clock updated
            Flush(updated_min_clock, progress_tracker_.GetProgress(msg.meta.sender % 1000));
        }
    }

    void SSPModel::Flush(int updated_min_clock, int progress) {
        auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
        for (auto req : reqs_blocked_at_this_min_clock) {
            Message msg = storage_->Get(req);
//            if (Context::get_instance().get_bool("has_scale_node")) {
//                int scale_node_id = Context::get_instance().get_int32("scale_node_id");
//                int converted_node_id = msg.meta.recver / SimpleIdMapper::kMaxThreadsPerNode;
//                if (scale_node_id == converted_node_id) {
//                    if (progress % 300 == 0) {
//                        LOG(INFO) << "Flush scale node msg:" << msg.DebugString() << ", progress=" << progress;
//                    }
//                }
//            }

            reply_queue_->Push(msg);
        }
        storage_->FinishIter();
    }

    void SSPModel::FlushAll() {
        auto reqs = buffer_.PopAll();
        LOG(INFO) << "SSPModel::FlushAll with message size=" << reqs.size();
        for (auto req : reqs) {
            reply_queue_->Push(storage_->Get(req));
        }
        storage_->FinishIter();
    }

    void SSPModel::Add(Message &msg) {
        storage_->Add(msg);
    }

    void SSPModel::Get(Message &msg) {
        int tid = ConvertTID(msg.meta.sender);
        CHECK(progress_tracker_.CheckThreadValid(tid));

        int progress = progress_tracker_.GetProgress(tid);
        int min_clock = progress_tracker_.GetMinClock();

//        if (Context::get_instance().get_bool("has_scale_node")) {
//            int scale_node_id = Context::get_instance().get_int32("scale_node_id");
//            int converted_node_id = msg.meta.sender / SimpleIdMapper::kMaxThreadsPerNode;
//            if (scale_node_id == converted_node_id) {
//                tid = msg.meta.sender % SimpleIdMapper::kMaxThreadsPerNode;
//                if (progress % 300 == 0) {
//                    LOG(INFO) << "Get scale node=" << scale_node_id << ", tid=" << tid << ", progress=" << progress
//                              << ", min_clock=" << min_clock;
//                }
//            }
//        }

//        LOG(INFO) << "SSPModel Get:" << "process," << progress << ", min_clock," << min_clock;
//        progress_tracker_.DebugString();
        if (progress > min_clock + staleness_) {
            buffer_.Push(progress - staleness_, msg);
        } else {
            Message message = storage_->Get(msg);
//            if (Context::get_instance().get_bool("has_scale_node")) {
//                int scale_node_id = Context::get_instance().get_int32("scale_node_id");
//                int converted_node_id = message.meta.recver / SimpleIdMapper::kMaxThreadsPerNode;
//                if (scale_node_id == converted_node_id) {
//                    if (progress % 300 == 0) {
//                        LOG(INFO) << "progress=" << progress << ", min_clock=" << min_clock;
//                        LOG(INFO) << "Flush scale node msg:" << message.DebugString() << ", progress=" << progress;
//                    }
//                }
//            }
            reply_queue_->Push(message);
        }
    }

    int SSPModel::GetProgress(int tid) {
        return progress_tracker_.GetProgress(tid);
    }

    int SSPModel::GetPendingSize(int progress) {
        return buffer_.Size(progress);
    }

    void SSPModel::ResetWorker(Message &msg) {
        CHECK_EQ(msg.data.size(), 1);

        if (!Context::get_instance().get_bool("use_weight_file")) {
            third_party::SArray<uint32_t> sArray;
            sArray = msg.data[0];
            std::vector<uint32_t> vec = SArrayToVector<uint32_t>(sArray);
            this->progress_tracker_.Init(vec);
        }

        Message reply_msg;
        reply_msg.meta.model_id = model_id_;
        reply_msg.meta.recver = msg.meta.sender;
        reply_msg.meta.flag = Flag::kResetWorkerInModel;
        reply_queue_->Push(reply_msg);
    }

    void SSPModel::Dump(Message &msg) {
        storage_->Dump();
        progress_tracker_.Dump();
        SVMDumper dumper;
        dumper.DumpConfigData(Context::get_instance().GetIterationMap());

        Message reply;
        reply.meta.recver = msg.meta.sender;
        reply.meta.sender = msg.meta.recver;
        reply.meta.flag = msg.meta.flag;
        reply.meta.model_id = msg.meta.model_id;

        reply_queue_->Push(reply);
    }

    void SSPModel::Restore() {
        storage_->Restore();
        progress_tracker_.Restore();

//        std::this_thread::sleep_for(std::chrono::seconds(int(5)));
        FlushAll();

        int min_clock = progress_tracker_.GetMinClock();
        LOG(INFO) << "SSPModel min_clock=" << min_clock;
        progress_tracker_.DebugString();
    }

    void SSPModel::Update(int failed_node_id, std::vector<Node> &nodes, third_party::Range &range) {
        storage_->Update(range);

        int result = progress_tracker_.Update(failed_node_id, nodes);
        if (result != -1) {
            Flush(result);
        }
    }

}  // namespace minips
