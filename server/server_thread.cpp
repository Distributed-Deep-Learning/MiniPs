#include "server/server_thread.hpp"

#include "glog/logging.h"

namespace minips {

    void ServerThread::RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel> &&model) {
        CHECK(models_.find(model_id) == models_.end());
        models_.insert(std::make_pair(model_id, std::move(model)));
    }

    AbstractModel *ServerThread::GetModel(uint32_t model_id) {
        CHECK(models_.find(model_id) != models_.end());
        return models_[model_id].get();
    }

    void ServerThread::UpdateModel(int failed_node_id, std::vector<Node> &nodes, third_party::Range &range) {
        for (auto it = models_.begin(); it != models_.end(); it++) {
            it->second->Update(failed_node_id, nodes, range);
        }
    }

    void ServerThread::Main() {
        while (true) {
            Message msg;
            work_queue_.WaitAndPop(&msg);

            if (msg.meta.flag == Flag::kExit)
                break;

            uint32_t model_id = msg.meta.model_id;
            if (models_.find(model_id) == models_.end()) {
                LOG(INFO) << "Unknown model_id: " << model_id << "Debug msg" << msg.DebugString();
                continue;
            }
            switch (msg.meta.flag) {
                case Flag::kClock: {
                    models_[model_id]->Clock(msg);
                    break;
                }
                case Flag::kAdd: {
                    models_[model_id]->Add(msg);
                    break;
                }
                case Flag::kGet: {
                    models_[model_id]->Get(msg);
                    break;
                }
                case Flag::kResetWorkerInModel: {
                    models_[model_id]->ResetWorker(msg);
                    break;
                }
                case Flag::kCheckpoint: {
                    models_[model_id]->Dump(msg);
                    break;
                }
                default:
                    CHECK(false) << "Unknown flag in msg: " << FlagName[static_cast<int>(msg.meta.flag)];
            }
        }
    }

}  // namespace minips

