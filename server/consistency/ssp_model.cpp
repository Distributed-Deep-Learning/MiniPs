#include "server/consistency/ssp_model.hpp"
#include "glog/logging.h"
#include <base/utils.hpp>

namespace csci5570 {

    SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage> &&storage_ptr, int staleness,
                       ThreadsafeQueue<Message> *reply_queue)
            : model_id_(model_id), staleness_(staleness), reply_queue_(reply_queue) {
        this->storage_ = std::move(storage_ptr);
    }

    void SSPModel::Clock(Message &msg) {
        int updated_min_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
        if (updated_min_clock != -1) {  // min clock updated
            auto reqs_blocked_at_this_min_clock = buffer_.Pop(updated_min_clock);
            for (auto req : reqs_blocked_at_this_min_clock) {
                reply_queue_->Push(storage_->Get(req));
            }
            storage_->FinishIter();
        }
    }

    void SSPModel::Add(Message &msg) {
        storage_->Add(msg);
    }

    void SSPModel::Get(Message &msg) {
        CHECK(progress_tracker_.CheckThreadValid(msg.meta.sender));

        int progress = progress_tracker_.GetProgress(msg.meta.sender);
        int min_clock = progress_tracker_.GetMinClock();
        if (progress > min_clock + staleness_) {
            buffer_.Push(progress - staleness_, msg);
        } else {
            reply_queue_->Push(storage_->Get(msg));
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

        third_party::SArray<uint32_t> sArray;
        sArray = msg.data[0];
        std::vector<uint32_t> vec = SArrayToVector<uint32_t >(sArray);
        this->progress_tracker_.Init(vec);

        Message reply_msg;
        reply_msg.meta.model_id = model_id_;
        reply_msg.meta.recver = msg.meta.sender;
        reply_msg.meta.flag = Flag::kResetWorkerInModel;
        reply_queue_->Push(reply_msg);
    }

    void SSPModel::Dump(Message &msg) {
        storage_->Dump(msg.meta.recver);

        Message reply;
        reply.meta.recver = msg.meta.sender;
        reply.meta.sender = msg.meta.recver;
        reply.meta.flag = msg.meta.flag;
        reply.meta.model_id = msg.meta.model_id;

        reply_queue_->Push(reply);
    }

}  // namespace csci5570
