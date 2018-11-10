#pragma once

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
#include "server/abstract_storage.hpp"
#include "server/map_storage.hpp"
#include "server/vector_storage.hpp"
#include "server/consistency/bsp_model.hpp"
#include "server/consistency/ssp_model.hpp"
#include "server/consistency/asp_model.hpp"
#include "base/range_partition_manager.hpp"
#include "base/context.hpp"

namespace csci5570 {

    enum class ModelType {
        SSP, BSP, ASP
    };
    enum class StorageType {
        Map, Vector
    };

    class Mailbox;

    class Engine {
    public:
        /**
         * Engine constructor
         *
         * @param node     the current node
         * @param nodes    all nodes in the cluster
         */
        Engine(const Node &node, const std::vector<Node> &nodes, const Node &master_node = {}) :
                node_(node), nodes_(nodes), master_node_(master_node) {
            // load config into context
            Context::get_instance();
        }

        /**
         * The flow of starting the engine:
         * 1. Create an id_mapper and a mailbox
         * 2. Start Sender
         * 3. Create ServerThreads and WorkerThreads
         * 4. Register the threads to mailbox through ThreadsafeQueue
         * 5. Start the communication threads: bind and connect to all other nodes
         *
         * @param num_server_threads_per_node the number of server threads to start on each node
         */
        void StartEverything(int num_server_threads_per_node = 1);

        void UpdateNodes(std::vector<Node> &nodes);

        void CreateIdMapper(int num_server_threads_per_node = 1);

        void CreateMailbox();

        void StartServerThreads();

        void StartWorkerThreads();

        void StartMailbox();

        void StartSender();

        std::vector<Node> getNodes() {
            return nodes_;
        }

        /**
         * The flow of stopping the engine:
         * 1. Stop the Sender
         * 2. Stop the mailbox: by Barrier() and then exit
         * 3. The mailbox will stop the corresponding registered threads
         * 4. Stop the ServerThreads and WorkerThreads
         */
        void StopEverything();

        void StopServerThreads();

        void StopWorkerThreads();

        void StopSender();

        void StopMailbox(bool barrier = true);

        /**
         * Synchronization barrier for processes
         */
        void Barrier();

        void ForceQuit(uint32_t node_id);

        /**
         * Create the whole picture of the worker group, and register the workers in the id mapper
         *
         * @param worker_alloc    the worker allocation information
         */
        WorkerSpec AllocateWorkers(const std::vector<WorkerAlloc> &worker_alloc);

        /**
         * Create the partitions of a model on the local servers
         * 1. Assign a table id (incremental and consecutive)
         * 2. Register the partition manager to the model
         * 3. For each local server thread maintained by the engine
         *    a. Create a storage according to <storage_type>
         *    b. Create a model according to <model_type>
         *    c. Register the model to the server thread
         *
         * @param partition_manager   the model partition manager
         * @param model_type          the consistency of model - bsp, ssp, asp
         * @param storage_type        the storage type - map, vector...
         * @param model_staleness     the staleness for ssp model
         * @return                    the created table(model) id
         */
        template<typename Val>
        uint32_t CreateTable(std::unique_ptr<AbstractPartitionManager> partition_manager, ModelType model_type,
                             StorageType storage_type, const std::vector<third_party::Range> &ranges,
                             int model_staleness = 0) {
            // 1. Assign a table id (incremental and consecutive)
            // 2. Register the partition manager to the model
            RegisterPartitionManager(model_count_, std::move(partition_manager));

            CHECK(id_mapper_);
            auto server_thread_ids = id_mapper_->GetAllServerThreads();

            // 3. For each local server thread maintained by the engine
            for (auto &server_thread : server_thread_group_) {
                std::unique_ptr<AbstractStorage> storage;
                std::unique_ptr<AbstractModel> model;

                // a. Create a storage according to <storage_type>
                if (storage_type == StorageType::Map) {
                    storage.reset(new MapStorage<Val>());
                } else if (storage_type == StorageType::Vector) {
                    auto it = std::find(server_thread_ids.begin(), server_thread_ids.end(), server_thread->GetId());
                    storage.reset(new VectorStorage<Val>(ranges[it - server_thread_ids.begin()]));
                } else {
                    CHECK(false) << "Unknown storage_type";
                }

                // b. Create a model according to <model_type>
                if (model_type == ModelType::SSP) {
                    model.reset(new SSPModel(model_count_, std::move(storage), model_staleness,
                                             sender_->GetMessageQueue()));
                } else if (model_type == ModelType::BSP) {
                    model.reset(new BSPModel(model_count_, std::move(storage), sender_->GetMessageQueue()));
                } else if (model_type == ModelType::ASP) {
                    model.reset(new ASPModel(model_count_, std::move(storage), sender_->GetMessageQueue()));
                } else {
                    CHECK(false) << "Unknown model_type";
                }

                // c. Register the model to the server thread
                server_thread->RegisterModel(model_count_, std::move(model));
            }

            return model_count_++;
        }

        /**
         * Create the partitions of a model on the local servers using a default partitioning scheme
         * 1. Create a default partition manager
         * 2. Create a table with the partition manager
         *
         * @param model_type          the consistency of model - bsp, ssp, asp
         * @param storage_type        the storage type - map, vector...
         * @param model_staleness     the staleness for ssp model
         * @return                    the created table(model) id
         */
        template<typename Val>
        uint32_t CreateTable(const std::vector<third_party::Range> &ranges, ModelType model_type,
                             StorageType storage_type, int model_staleness = 0) {
            // 1. Create a default partition manager
            CHECK(id_mapper_);
            const auto server_thread_ids = id_mapper_->GetAllServerThreads();
            int32_t master_node_id = master_node_.is_master ? master_node_.id : -1;
            std::unique_ptr<AbstractPartitionManager> partition_manager(
                    new RangePartitionManager(server_thread_ids, ranges, master_node_id));

            // 2. Create a table with the partition manager
            return CreateTable<Val>(std::move(partition_manager), model_type, storage_type, ranges,
                                    model_staleness);
        }

        /**
         * Reset workers in the specified model so that each model knows the workers with the right of access
         */
        void InitTable(uint32_t table_id, const std::vector<uint32_t> &worker_ids);

        /**
         * Run the task
         *
         * After starting the system, the engine run a task by starting the prescribed threads to run UDF
         *
         * @param task    the task to run
         */
        void Run(const MLTask &task, bool quit = false);

        /**
         * Returns the server thread ids
         */
        std::vector<uint32_t> GetServerThreadIds() { return id_mapper_->GetAllServerThreads(); }

        void RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message> *const queue);

    private:
        /**
         * Register partition manager for a model to the engine
         *
         * @param table_id            the model id
         * @param partition_manager   the partition manager for the specific model
         */
        void RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager);

        std::map<uint32_t, std::unique_ptr<AbstractPartitionManager>> partition_manager_map_;

        // nodes
        Node node_;
        Node master_node_;
        std::vector<Node> nodes_;

        // mailbox
        std::unique_ptr<SimpleIdMapper> id_mapper_;
        std::unique_ptr<Mailbox> mailbox_;
        std::unique_ptr<Sender> sender_;

        // worker elements
        std::unique_ptr<WorkerThread> worker_thread_;

        // server elements
        std::vector<std::unique_ptr<ServerThread>> server_thread_group_;
        size_t model_count_ = 0;
    };

}  // namespace csci5570
