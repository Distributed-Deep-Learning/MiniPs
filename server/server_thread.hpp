#pragma once

#include "base/actor_model.hpp"
#include "base/threadsafe_queue.hpp"
#include "server/abstract_model.hpp"

#include <thread>
#include <unordered_map>

namespace minips {

    class ServerThread : public Actor {
    public:
        ServerThread(uint32_t server_id) : Actor(server_id) {}

        // for model maintenance
        void RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel> &&model);

        AbstractModel *GetModel(uint32_t model_id);

        void UpdateModel(int failed_node_id, std::vector<Node> &nodes, third_party::Range &range);

        void RollbackModel() {
            for (auto it = models_.begin(); it != models_.end(); it++) {
                it->second->Restore();
            }
        }

    protected:
        virtual void Main() override;                                  // where the actor polls events and reacts

        std::unordered_map<uint32_t, std::unique_ptr<AbstractModel>> models_;
    };

}  // namespace minips
