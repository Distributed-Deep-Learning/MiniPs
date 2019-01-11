#include "driver/engine.hpp"

#include <vector>
#include "base/abstract_partition_manager.hpp"
#include "base/node.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/server_thread.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/worker_thread.hpp"

namespace minips {

    void Engine::StartEverything(int num_server_threads_per_node) {
        CreateIdMapper(num_server_threads_per_node);
        CreateMailbox();

        StartMailbox();
        StartSender();
        StartServerThreads();
        StartWorkerThreads();
        StartHeartbeatThread();
    }

    void Engine::UpdateNodes(std::vector<Node> &nodes) {
        nodes_ = nodes;
        id_mapper_->Update(nodes, 1);
    }

    void Engine::StopEverything() {
        StopHeartbeatThread();
        StopMailbox();
        StopSender();
        StopServerThreads();
        StopWorkerThreads();
    }

    void Engine::CreateIdMapper(int num_server_threads_per_node) {
        id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
        id_mapper_->Init(num_server_threads_per_node);
    }

    void Engine::CreateMailbox() {
        mailbox_.reset(new Mailbox(node_, nodes_, id_mapper_.get(), this));
    }

    void Engine::StartServerThreads() {
        CHECK(sender_);
        CHECK(mailbox_);
        auto server_thread_ids = id_mapper_->GetServerThreadsForId(node_.id);
        CHECK_GT(server_thread_ids.size(), 0);

        for (int server_thread_id : server_thread_ids) {
            std::unique_ptr<ServerThread> server_thread(new ServerThread(server_thread_id));
            mailbox_->RegisterQueue(server_thread->GetId(), server_thread->GetWorkQueue());
            server_thread->Start();

            server_thread_group_.emplace_back(std::move(server_thread));
        }

        std::stringstream ss;
        for (auto id : server_thread_ids) {
            ss << id << " ";
        }
        VLOG(1) << "server_threads:" << ss.str() << " start on node:" << node_.id;
    }

    void Engine::StartWorkerThreads() {
        CHECK(id_mapper_);
        CHECK(mailbox_);
        auto worker_thread_ids = id_mapper_->GetWorkerHelperThreadsForId(node_.id);
        CHECK_EQ(worker_thread_ids.size(), 1);

        worker_thread_.reset(new WorkerThread(worker_thread_ids[0]));
        mailbox_->RegisterQueue(worker_thread_->GetId(), worker_thread_->GetWorkQueue());
        worker_thread_->Start();
        VLOG(1) << "worker_thread:" << worker_thread_ids[0] << " starts on node:" << node_.id;
    }

    void Engine::UpdateAndRestart(int failed_node_id, const std::vector<minips::Node> &nodes) {
        LOG(INFO) << "Engine UpdateAndRestart on node:" << node_.id;
        nodes_ = nodes;
        for (Node node : nodes_) {
            LOG(INFO) << "Engine UpdateAndRestart new node:" << node.id;
        }

        std::vector<third_party::Range> ranges = getRanges();
        mailbox_->Update(nodes_);
        id_mapper_->Update(nodes_, 1);

        // worker, partition, id_mapper...etc.
        UpdateServerThreads(failed_node_id, nodes_, ranges[0]);
        UpdateWorkerThreads(nodes_);
        UpdatePartitionManagers(ranges);
        // restarter_(node_, nodes_, master_node_);
    }

    void Engine::StartHeartbeatThread() {
        if (HasMaster()) {
            heartbeat_running_ = true;
            heartbeat_thread_ = std::thread([this]{
//                if (Context::get_instance().get_bool("use_weight_file")) {
//                    LOG(INFO) << "Send heartbeat right now after restart...";
//                    HeartBeat(node_.id);
//                }

                int32_t interval = Context::get_instance().get_int32("heartbeat_interval");
                while (interval > 0 && heartbeat_running_) {
                    std::this_thread::sleep_for(std::chrono::seconds(interval));
                    HeartBeat(node_.id);
                    //LOG(INFO) << "[Slave] post heartbeat to master from node:" << node_.id;
                }
                //LOG(INFO) << "[Slave] node:" << node_.id << ", heartbeat service stopped...";
                HeartBeat(node_.id, true);
            });
        }
    }

    void Engine::StopHeartbeatThread() {
        if (HasMaster()) {
            heartbeat_running_ = false;
            heartbeat_thread_.join();
        }
    }

    void Engine::StartMailbox() {
        CHECK(mailbox_);
        mailbox_->Start(master_node_);
        VLOG(1) << "mailbox starts on node" << node_.id;
    }

    void Engine::StartSender() {
        sender_.reset(new Sender(mailbox_.get()));
        sender_->Start();
    }

    void Engine::StopServerThreads() {
        for (auto &server_thread : server_thread_group_) {
            server_thread->Stop();
        }
        VLOG(1) << "server_threads stop on node" << node_.id;
    }

    void Engine::StopWorkerThreads() {
        CHECK(worker_thread_);
        worker_thread_->Stop();
        VLOG(1) << "worker_thread stops on node" << node_.id;
    }

    void Engine::StopSender() {
        CHECK(sender_);
        sender_->Stop();
        VLOG(1) << "sender stops on node" << node_.id;
    }

    void Engine::StopMailbox(bool barrier) {
        CHECK(mailbox_);
        mailbox_->Stop(barrier);
        VLOG(1) << "mailbox stops on node" << node_.id;
    }

    void Engine::Barrier(bool send) {
        mailbox_->Barrier(send);
    }

    void Engine::ForceQuit(uint32_t node_id) {
        mailbox_->ForceQuit(node_id);
    }

    WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc> &worker_alloc) {
        CHECK(id_mapper_);
        WorkerSpec worker_spec(worker_alloc);

        // Need to make sure that all the engines allocate the same set of workers
        for (auto &kv : worker_spec.GetNodeToWorkers()) {
            for (int i = 0; i < kv.second.size(); ++i) {
                uint32_t tid = id_mapper_->AllocateWorkerThread(kv.first);
                worker_spec.InsertWorkerIdThreadId(kv.second[i], tid);
            }
        }
        return worker_spec;
    }

    void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t> &worker_ids) {
        CHECK(id_mapper_);
        CHECK(mailbox_);
        std::vector<uint32_t> local_servers = id_mapper_->GetServerThreadsForId(node_.id);
        int count = local_servers.size();
        if (count == 0)
            return;

        // Register receiving queue
        auto id = id_mapper_->AllocateWorkerThread(node_.id);  // TODO allocate background thread?
        ThreadsafeQueue<Message> queue;
        mailbox_->RegisterQueue(id, &queue);

        // Create and send reset worker message
        Message reset_msg;
        reset_msg.meta.flag = Flag::kResetWorkerInModel;
        reset_msg.meta.model_id = table_id;
        reset_msg.meta.sender = id;
        reset_msg.AddData(third_party::SArray<uint32_t>(worker_ids));
        for (auto local_server : local_servers) {
            reset_msg.meta.recver = local_server;
            sender_->GetMessageQueue()->Push(reset_msg);
        }

        // Wait for reply
        Message reply;
        while (count > 0) {
            queue.WaitAndPop(&reply);
            CHECK(reply.meta.flag == Flag::kResetWorkerInModel);
            CHECK(reply.meta.model_id == table_id);
            --count;
        }

        // Free receiving queue
        mailbox_->DeregisterQueue(id);
        id_mapper_->DeallocateWorkerThread(node_.id, id);
    }

    void Engine::Run(const MLTask &task, bool quit) {
        CHECK(task.IsSetup());
        WorkerSpec worker_spec = AllocateWorkers(task.GetWorkerAlloc());

        // Init tables
        const std::vector<uint32_t> &tables = task.GetTables();
        for (auto table : tables) {
            InitTable(table, worker_spec.GetAllThreadIds());
        }

        if (!Context::get_instance().get_bool("use_weight_file")) {
            mailbox_->Barrier();
        }

        // Spawn user threads
        if (worker_spec.HasLocalWorkers(node_.id)) {
            const auto &local_threads = worker_spec.GetLocalThreads(node_.id);
            const auto &local_workers = worker_spec.GetLocalWorkers(node_.id);

            CHECK_EQ(local_threads.size(), local_workers.size());
            std::vector<std::thread> thread_group(local_threads.size());
//            LOG(INFO) << thread_group.size() << " workers run on proc: " << node_.id;

            std::map<uint32_t, AbstractPartitionManager *> partition_manager_map;
            for (auto &table : tables) {
                auto it = partition_manager_map_.find(table);
                CHECK(it != partition_manager_map_.end());
                partition_manager_map[table] = it->second.get();
            }
            for (int i = 0; i < thread_group.size(); ++i) {
                // TODO: Now I register the thread_id with the queue in worker_helper_thread to the mailbox.
                // So that the message sent to thread_id will be pushed into worker_helper_thread_'s queue
                // and worker_helper_thread_ is in charge of handling the message.
                mailbox_->RegisterQueue(local_threads[i], worker_thread_->GetWorkQueue());
                Info info;
                info.thread_id = local_threads[i];
                info.worker_id = local_workers[i];
                info.send_queue = sender_->GetMessageQueue();
                info.partition_manager_map = partition_manager_map;
                info.callback_runner = worker_thread_.get();

                thread_group[i] = std::thread([&task, info]() { task.RunLambda(info); });
            }
            // LOG(INFO) << "thread_group join start";
            for (auto &th : thread_group) {
                th.join();
                // LOG(INFO) << "thread_group join end";
            }
        }
        // Let all the on-the-fly messages be recevied based on TCP/IP assumption
        mailbox_->Barrier();
    }

    void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
        partition_manager_map_.insert(std::make_pair(table_id, std::move(partition_manager)));
    }

    void Engine::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message> *const queue) {
        mailbox_->RegisterQueue(queue_id, queue);
    }

}  // namespace minips
