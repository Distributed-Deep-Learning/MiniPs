#pragma once

#include "base/actor_model.hpp"
#include "base/message.hpp"
#include "base/threadsafe_queue.hpp"
#include "abstract_callback_runner.hpp"
#include "base/message.hpp"
#include <condition_variable>
#include <memory>
#include <thread>
#include <unordered_map>
#include "base/node.hpp"

namespace minips {

    class WorkerThread : public Actor, public AbstractCallbackRunner {
    public:
        WorkerThread(uint32_t worker_id) : Actor(worker_id) {}

        virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void(Message&)>& recv_handle) override;

        virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                              const std::function<void()>& recv_finish_handle) override;

        virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) override;

        virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) override;

        virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) override;

        virtual void NewCheckPoint(uint32_t expected_responses) override;

        virtual void WaitCheckPoint() override;

        virtual void CheckPointResponse() override;

        virtual void Update();

        virtual void RollBackWorker();

    protected:
        void Main() override;
        // callback on receival of a message

        // there may be other functions
        //   Wait() and Nofify() for telling when parameters are ready

        // there may be other states such as
        //   a local copy of parameters
        //   some locking mechanism for notifying when parameters are ready
        //   a counter of msgs from servers, etc.

    private:
        void Check(uint32_t app_thread_id, uint32_t model_id);

        std::condition_variable cond_;
        std::mutex mu_;

        // app_thread_id, model_id, <expected, current>
        std::map<uint32_t, std::map<uint32_t, std::pair<uint32_t, uint32_t>>> tracker_;

        uint32_t checkpoint_expect_;
        uint32_t checkpoint_current_;

        // app_thread_id, model_id, callback
        std::map<uint32_t, std::map<uint32_t, std::function<void(Message& message)>>> recv_handle_;
        std::map<uint32_t, std::map<uint32_t, std::function<void()>>> recv_finish_handle_;
    };

}  // namespace minips
