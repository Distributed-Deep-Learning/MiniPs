#include "gflags/gflags.h"
#include "glog/logging.h"
#include <gperftools/profiler.h>
#include "base/node_utils.hpp"
#include "driver/engine.hpp"
#include "worker/kv_client_table.hpp"
#include <algorithm>
#include <numeric>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <lib/abstract_data_loader.hpp>
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/hdfs_manager.hpp"
#include "kmeans_helper.hpp"

DEFINE_int32(my_id, 0, "The process id of this program");
DEFINE_string(config_file, "/Users/aiyongbiao/Desktop/projects/csci5570/config/localnode", "The config file path");
DEFINE_string(hdfs_namenode, "localhost", "The hdfs namenode hostname");
DEFINE_string(input, "hdfs:///a9a", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs namenode port");
DEFINE_int32(assigner_master_port, 19201, "The hdfs_assigner master_port");

DEFINE_uint64(num_dims, 123, "number of dimensions");
DEFINE_int32(num_iters, 1000, "number of iters");

DEFINE_string(kModelType, "SSP", "ASP/SSP/BSP");
DEFINE_string(kStorageType, "Vector", "Map/Vector");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_uint32(num_workers_per_node, 1, "num_workers_per_node");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");
DEFINE_int32(num_local_load_thread, 2, "num_local_load_thread");
DEFINE_int32(num_nodes, 1, "num_nodes");
DEFINE_int32(batch_size, 100, "batch size of each epoch");
DEFINE_uint64(K, 2, "K");
DEFINE_double(alpha, 0.1, "learning rate coefficient");
DEFINE_string(kmeans_init_mode, "random", "random/kmeans++/kmeans_parallel");
DEFINE_int32(report_interval, 2, "report interval");
DEFINE_int32(report_worker, 0, "report worker");

DEFINE_bool(init_dump, true, "init_dump");
DEFINE_bool(use_weight_file, false, "use weight file to restore progress");
DEFINE_bool(checkpoint_toggle, true, "open checkpoint");
DEFINE_string(weight_file_prefix, "", "the prefix filename of weight file");
DEFINE_string(checkpoint_file_prefix, "hdfs://localhost:9000/dump/dump_", "the checkpoint file prefix");
DEFINE_string(checkpoint_raw_prefix, "hdfs:///dump/dump_", "the checkpoint raw prefix");
DEFINE_int32(heartbeat_interval, 10, "the heatbeat check interval");
DEFINE_string(relaunch_cmd,
              "python /Users/aiyongbiao/Desktop/projects/csci5570/scripts/logistic_regression.py relaunch 1",
              "the restart cmd");


namespace csci5570 {

    void Run() {
        srand(0);
        CHECK_NE(FLAGS_my_id, -1);
        CHECK(!FLAGS_config_file.empty());
        CHECK(FLAGS_kModelType == "ASP" || FLAGS_kModelType == "BSP" || FLAGS_kModelType == "SSP");
        CHECK(FLAGS_kStorageType == "Map" || FLAGS_kStorageType == "Vector");
        CHECK_GT(FLAGS_num_dims, 0);
        CHECK_GT(FLAGS_num_iters, 0);
        CHECK_LE(FLAGS_kStaleness, 5);
        CHECK_GE(FLAGS_kStaleness, 0);
        CHECK_LE(FLAGS_num_workers_per_node, 20);
        CHECK_GE(FLAGS_num_workers_per_node, 1);
        CHECK_LE(FLAGS_num_servers_per_node, 20);
        CHECK_GE(FLAGS_num_servers_per_node, 1);

        if (FLAGS_my_id == 0) {
            LOG(INFO) << "Running in " << FLAGS_kModelType << " mode";
            LOG(INFO) << "num_dims: " << FLAGS_num_dims;
            LOG(INFO) << "num_iters: " << FLAGS_num_iters;
            LOG(INFO) << "num_workers_per_node: " << FLAGS_num_workers_per_node;
            LOG(INFO) << "num_servers_per_node: " << FLAGS_num_servers_per_node;
        }

        VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

        // 0. Parse config_file
        std::vector<Node> nodes = ParseFile(FLAGS_config_file);
        CHECK(CheckValidNodeIds(nodes));
        CHECK(CheckUniquePort(nodes));
        Node my_node = GetNodeById(nodes, FLAGS_my_id);
        LOG(INFO) << my_node.DebugString();

        // 1. Load data
        HDFSManager::Config config;
        config.url = FLAGS_input;
        config.worker_host = my_node.hostname;
        config.worker_port = my_node.port;
        config.master_port = FLAGS_assigner_master_port;
        config.master_host = nodes[0].hostname;
        config.hdfs_namenode = FLAGS_hdfs_namenode;
        config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
        config.num_local_load_thread = FLAGS_num_local_load_thread;

        // parse data
        std::vector<SVMItem> data;
        lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
        lib::Parser<SVMItem> parser;
        std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
            return parser.parse_libsvm(line);
        };
        loader.load(config, my_node, nodes, parse, data);
        LOG(INFO) << "Finished loading data.";

        // 2. Start engine
        Engine engine(my_node, nodes);
        engine.StartEverything(FLAGS_num_servers_per_node);

        // 3. Create tables
        std::vector<third_party::Range> range;
        int num_total_servers = nodes.size() * FLAGS_num_servers_per_node;
        uint64_t num_total_params = (FLAGS_K + 1) * FLAGS_num_dims;
        for (int i = 0; i < num_total_servers - 1; ++i) {
            range.push_back({num_total_params / num_total_servers * i, num_total_params / num_total_servers * (i + 1)});
        }
        range.push_back({num_total_params / num_total_servers * (num_total_servers - 1), num_total_params});

        ModelType model_type;
        if (FLAGS_kModelType == "ASP") {
            model_type = ModelType::ASP;
        } else if (FLAGS_kModelType == "SSP") {
            model_type = ModelType::SSP;
        } else if (FLAGS_kModelType == "BSP") {
            model_type = ModelType::BSP;
        } else {
            CHECK(false) << "model type error: " << FLAGS_kModelType;
        }

        StorageType storage_type;
        if (FLAGS_kStorageType == "Map") {
            storage_type = StorageType::Map;
        } else if (FLAGS_kStorageType == "Vector") {
            storage_type = StorageType::Vector;
        } else {
            CHECK(false) << "storage type error: " << FLAGS_kStorageType;
        }

        std::vector<third_party::Range> range2;
        for (int i = 0; i < num_total_servers - 1; ++i) {
            range2.push_back({FLAGS_K / num_total_servers * i, FLAGS_K / num_total_servers * (i + 1)});
        }
        range2.push_back({FLAGS_K / num_total_servers * (num_total_servers - 1), FLAGS_K});

        uint32_t kTableId = engine.CreateTable<double>(range, model_type, storage_type, FLAGS_kStaleness);
        uint32_t kTableId2 = engine.CreateTable<double>(range2, model_type, storage_type, FLAGS_kStaleness);
        engine.Barrier();

        // 4. Construct tasks
        MLTask init_task;

        std::vector<WorkerAlloc> worker_alloc;
        for (auto &node : nodes) {
            worker_alloc.push_back(
                    {node.id, FLAGS_num_workers_per_node});  // each node has num_workers_per_node workers
        }
        init_task.SetWorkerAlloc(worker_alloc);

        init_task.SetTables({kTableId});  // Use table 0
        init_task.SetLambda([&data, kTableId](const Info &info) {
            if (info.worker_id == 0) {
                LOG(INFO) << "Start Initalization for KMeans...";

                auto start_time = std::chrono::steady_clock::now();

                auto table = info.CreateKVClientTable<double>(kTableId);
                std::vector<Key> keys((FLAGS_K + 1) * FLAGS_num_dims);
                std::iota(keys.begin(), keys.end(), 0);
                std::vector<std::vector<double>> params(FLAGS_K + 1);
                std::vector<double> push((FLAGS_K + 1) * FLAGS_num_dims);

                for (int i = 0; i < FLAGS_K + 1; ++i) {
                    params[i].resize(FLAGS_num_dims, 0);
                }
                init_centers(FLAGS_K, FLAGS_num_dims, data, params, FLAGS_kmeans_init_mode);

                for (int i = 0; i < FLAGS_K + 1; ++i)
                    for (int j = 0; j < FLAGS_num_dims; ++j)
                        push[i * FLAGS_num_dims + j] = std::move(params[i][j]);

                table->Add(keys, push);
                table->Clock();
                CHECK_EQ(push.size(), keys.size());
                auto end_time = std::chrono::steady_clock::now();
                auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
                LOG(INFO) << "initial task total time: " << total_time << " ms on worker: " << info.worker_id;
            }
        });

        MLTask task;
        task.SetWorkerAlloc(worker_alloc);
        task.SetTables({kTableId, kTableId2});  // Use table 0,1
        task.SetLambda([data, kTableId, kTableId2](const Info &info) {
            if (info.worker_id == 0) {
                LOG(INFO) << "Start KMeans Training...";
            }

            auto start_time = std::chrono::steady_clock::now();
            srand(time(0));

            auto table = info.CreateKVClientTable<double>(kTableId);
            std::vector<Key> keys((FLAGS_K + 1) * FLAGS_num_dims);
            std::iota(keys.begin(), keys.end(), 0);
            std::vector<std::vector<double>> params(FLAGS_K + 1);
            std::vector<std::vector<double>> deltas(FLAGS_K + 1);
            std::vector<double> pull((FLAGS_K + 1) * FLAGS_num_dims);
            std::vector<double> push((FLAGS_K + 1) * FLAGS_num_dims);

            auto table2 = info.CreateKVClientTable<double>(kTableId2);
            std::vector<Key> keys2(FLAGS_K);
            std::iota(keys2.begin(), keys2.end(), 0);
            std::vector<double> cluster_members(FLAGS_K);

            for (int i = 0; i < FLAGS_K + 1; ++i) {
                params[i].resize(FLAGS_num_dims);
            }

            for (int iter = 0; iter < FLAGS_num_iters; ++iter) {
                table->Get(keys, &pull);
                CHECK_EQ(keys.size(), pull.size());
                table2->Get(keys2, &cluster_members);
                CHECK_EQ(keys2.size(), cluster_members.size());
                for (int i = 0; i < FLAGS_K + 1; ++i)
                    for (int j = 0; j < FLAGS_num_dims; ++j)
                        params[i][j] = std::move(pull[i * FLAGS_num_dims + j]);
                deltas = params;
                auto cluster_member_cnts = cluster_members;

                // training A mini-batch
                int id_nearest_center;
                double learning_rate;
                int startpt = rand() % data.size();
                for (int i = 0; i < FLAGS_batch_size / FLAGS_num_workers_per_node; ++i) {
                    if (startpt == data.size())
                        startpt = rand() % data.size();
                    auto &datapt = data[startpt];
                    auto &x = data[startpt].first;
                    startpt += 1;
                    id_nearest_center = get_nearest_center(datapt, FLAGS_K, deltas, FLAGS_num_dims).first;
                    // learning_rate = FLAGS_alpha / ++deltas[FLAGS_K][id_nearest_center];
                    learning_rate = FLAGS_alpha / ++cluster_members[id_nearest_center];

                    std::vector<double> distance = deltas[id_nearest_center];
                    for (auto field : x)
                        distance[field.first] -= field.second;  // first:fea, second:val

                    for (int j = 0; j < FLAGS_num_dims; j++)
                        deltas[id_nearest_center][j] -= learning_rate * distance[j];
                }

                // update params
                for (int i = 0; i < FLAGS_K + 1; ++i)
                    for (int j = 0; j < FLAGS_num_dims; ++j)
                        deltas[i][j] -= params[i][j];

                for (int i = 0; i < cluster_members.size(); ++i)
                    cluster_members[i] -= cluster_member_cnts[i];

                for (int i = 0; i < FLAGS_K + 1; ++i)
                    for (int j = 0; j < FLAGS_num_dims; ++j)
                        push[i * FLAGS_num_dims + j] = std::move(deltas[i][j]);

                table->Add(keys, push);
                table->Clock();
                CHECK_EQ(push.size(), keys.size());
                table2->Add(keys2, cluster_members);
                table2->Clock();
                CHECK_EQ(keys2.size(), cluster_members.size());

                if (iter % FLAGS_report_interval == 0 && info.worker_id == FLAGS_report_worker)
                    test_error(params, data, iter, FLAGS_K, FLAGS_num_dims, info.worker_id);
            }

            auto end_time = std::chrono::steady_clock::now();
            auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            LOG(INFO) << "Total cost time: " << total_time << " ms on worker: " << info.worker_id;
        });

        // 5. Run tasks
        engine.Run(init_task);
        engine.Barrier();
        engine.Run(task);

        // 6. Stop engine
        engine.StopEverything();
    }

}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    csci5570::Run();
}
