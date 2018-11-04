#include <lib/abstract_data_loader.hpp>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include <lib/parser.hpp>
#include <lib/labeled_sample.hpp>
#include "boost/utility/string_ref.hpp"
#include <functional>
#include "io/hdfs_manager.hpp"
#include "base/node_utils.hpp"
#include "driver/engine.hpp"
#include "server/abstract_storage.hpp"
#include "server/map_storage.hpp"
#include "server/vector_storage.hpp"
#include "lib/batch_data_sampler.cpp"
#include <cmath>

DEFINE_int32(my_id, 0, "The process id of this program");
DEFINE_string(config_file, "/Users/aiyongbiao/Desktop/projects/csci5570/config/localnode", "The config file path");
DEFINE_string(hdfs_namenode, "localhost", "The hdfs namenode hostname");
DEFINE_string(input, "hdfs:///a9a", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs namenode port");
DEFINE_int32(assigner_master_port, 19201, "The hdfs_assigner master_port");

DEFINE_string(kModelType, "SSP", "ASP/SSP/BSP/SparseSSP");
DEFINE_string(kStorageType, "Vector", "Map/Vector");
DEFINE_int32(num_dims, 54686452, "number of dimensions");
DEFINE_int32(batch_size, 1, "batch size of each epoch");
DEFINE_int32(num_iters, 1000, "number of iters");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_int32(kSpeculation, 5, "speculation");
DEFINE_string(kSparseSSPRecorderType, "Vector", "None/Map/Vector");
DEFINE_int32(num_workers_per_node, 2, "num_workers_per_node");
DEFINE_int32(with_injected_straggler, 1, "with injected straggler or not, 0/1");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");
DEFINE_double(alpha, 0.1, "learning rate");

namespace csci5570 {

    template<typename T>
    void test_error(third_party::SArray<double> &rets_w, std::vector<T> &data_) {
        LOG(INFO) << "Finish training, start test error...";
        int count = 0;
        float c_count = 0;  /// correct count
        for (int i = 0; i < data_.size(); ++i) {
            auto &data = data_[i];
            count = count + 1;
            auto &x = data.first;
            float y = data.second;
            if (y < 0)
                y = 0;

            float pred_y = 0.0;
            for (auto field : x)
                pred_y += rets_w[field.first] * field.second;

            pred_y = 1. / (1. + exp(-1 * pred_y));
            pred_y = (pred_y > 0.5) ? 1 : 0;
            if (int(pred_y) == int(y)) {
                c_count += 1;
            }
        }
        LOG(INFO) << " accuracy is " << std::to_string(c_count / count);
    }

    void Run() {
        CHECK_NE(FLAGS_my_id, -1);
        CHECK(!FLAGS_config_file.empty());
        VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

        // 0. Parse config_file
        std::vector<Node> nodes = ParseFile(FLAGS_config_file);
        CHECK(CheckValidNodeIds(nodes));
        CHECK(CheckUniquePort(nodes));
        CHECK(CheckConsecutiveIds(nodes));
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
        config.num_local_load_thread = FLAGS_num_workers_per_node;

        std::vector<SVMItem> data;
        lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
        lib::Parser<SVMItem> parser;
        std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
            // parse data
            return parser.parse_libsvm(line);
        };
        loader.load(config, my_node, nodes, parse, data);
        LOG(INFO) << "Finished loading data on node " << my_node.id;

        // 2. Start engine
        Engine engine(my_node, nodes);
        engine.StartEverything();

        // Quit the engine if no traning data is read
        if (data.empty()) {
            LOG(INFO) << "ForceQuit the engine as no data allocated";
            for (auto node : nodes) {
                engine.ForceQuit(node.id);
            }
            engine.StopEverything();
            return;
        }
        engine.Barrier();

        // 3. Create tables
        nodes = engine.getNodes();
        std::vector<third_party::Range> range;
        uint32_t num_total_servers = nodes.size() * FLAGS_num_servers_per_node;
        for (uint32_t i = 0; i < num_total_servers - 1; ++i) {
            range.push_back({FLAGS_num_dims / num_total_servers * i, FLAGS_num_dims / num_total_servers * (i + 1)});
        }
        range.push_back({FLAGS_num_dims / num_total_servers * (num_total_servers - 1), (uint64_t) FLAGS_num_dims});

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
        // Create table
        uint32_t kTableId = engine.CreateTable<double>(range, model_type, storage_type, FLAGS_kStaleness);
        engine.Barrier();

        // 3. Construct tasks
        MLTask task;
        std::vector<WorkerAlloc> worker_alloc;
        for (auto &node : nodes) {
            worker_alloc.push_back({node.id, (uint32_t) FLAGS_num_workers_per_node});
        }
        task.SetWorkerAlloc(worker_alloc);
        task.SetTables({kTableId});  // Use table 0
        task.SetLambda([kTableId, &data](const Info &info) {
            LOG(INFO) << info.DebugString();

            BatchDataSampler<SVMItem> batch_data_sampler(data, FLAGS_batch_size);
            //prepare all_keys
            third_party::SArray<Key> all_keys;
            for (int i = 0; i < FLAGS_num_dims; ++i)
                all_keys.push_back(i);

            // prepare future_keys
            std::vector<third_party::SArray<Key>> future_keys;
            std::vector<std::vector<SVMItem *>> future_data_ptrs;

            for (int i = 0; i < FLAGS_num_iters + FLAGS_kSpeculation; ++i) {
                batch_data_sampler.random_start_point();
                future_keys.push_back(batch_data_sampler.prepare_next_batch());
                future_data_ptrs.push_back(batch_data_sampler.get_data_ptrs());
            }
            CHECK_EQ(future_keys.size(), FLAGS_num_iters + FLAGS_kSpeculation);

            auto start_time = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point end_time;
            srand(time(0));

            auto table = info.CreateKVClientTable<double>(kTableId);
            third_party::SArray<double> params;
            third_party::SArray<double> deltas;
            for (int i = 0; i < FLAGS_num_iters; i++) {
                CHECK_LT(i, future_keys.size());
                auto &keys = future_keys[i];
                table->Get(keys, &params);
                CHECK_EQ(keys.size(), params.size());
                deltas.resize(keys.size(), 0.0);

                for (auto data : future_data_ptrs[i]) {  // iterate over the data in the batch
                    auto &x = data->first;
                    float y = data->second;
                    if (y < 0)
                        y = 0;
                    float pred_y = 0.0;
                    int j = 0;
                    for (auto field : x) {
                        while (keys[j] < field.first)
                            j += 1;
                        pred_y += params[j] * field.second;
                    }
                    pred_y = 1. / (1. + exp(-1 * pred_y));
                    j = 0;
                    for (auto field : x) {
                        while (keys[j] < field.first)
                            j += 1;
                        deltas[j] += FLAGS_alpha * field.second * (y - pred_y);
                    }
                }
                table->Add(keys, deltas);
                table->Clock();
                CHECK_EQ(params.size(), keys.size());

                if (i % 200 == 0 && info.worker_id == 0) {
                    auto now = std::chrono::steady_clock::now();
                    LOG(INFO) << "Start checkpoint, sent by worker: 0";
                    table->CheckPoint();
                    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time).count();
                    LOG(INFO) << "Finish checkpoint, cost:" << cost << " ms";
                }

                if (i % 50 == 0 && info.worker_id == 0) {
                    LOG(INFO) << "Iter: " << i << " finished";
                }

                if (FLAGS_with_injected_straggler) {
                    double r = (double) rand() / RAND_MAX;
                    if (r < 0.05) {
                        double delay = (double) rand() / RAND_MAX * 100;
                        std::this_thread::sleep_for(std::chrono::milliseconds(int(delay)));
                        // LOG(INFO) << "sleep for " << int(delay) << " ms";
                    }
                }
            }
            end_time = std::chrono::steady_clock::now();

            // test error
            table->Get(all_keys, &params);
            test_error<SVMItem>(params, data);

            auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            LOG(INFO) << "total time: " << total_time << " ms on worker: " << info.worker_id;
        });

        // 4. Run tasks
        engine.Run(task);

        // 5. Stop engine
        engine.StopEverything();
    }

}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    csci5570::Run();
}