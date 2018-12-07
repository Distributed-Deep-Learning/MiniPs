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
#include <master/master.hpp>
#include <lib/svm_dumper.hpp>

DEFINE_int32(my_id, 0, "The process id of this program");
DEFINE_string(config_file, "/Users/aiyongbiao/Desktop/projects/csci5570/config/localnode", "The config file path");
DEFINE_string(hdfs_namenode, "localhost", "The hdfs namenode hostname");
DEFINE_string(input, "hdfs:///a2a", "The hdfs input url");
DEFINE_int32(hdfs_namenode_port, 9000, "The hdfs namenode port");
DEFINE_int32(assigner_master_port, 19201, "The hdfs_assigner master_port");

DEFINE_string(kModelType, "SSP", "ASP/SSP/BSP/SparseSSP");
DEFINE_string(kStorageType, "Vector", "Map/Vector");
DEFINE_int32(num_dims, 123, "number of dimensions");
DEFINE_int32(batch_size, 1, "batch size of each epoch");
DEFINE_int32(num_iters, 1000, "number of iters");
DEFINE_int32(kStaleness, 0, "stalness");
DEFINE_int32(kSpeculation, 5, "speculation");
DEFINE_string(kSparseSSPRecorderType, "Vector", "None/Map/Vector");
DEFINE_int32(num_workers_per_node, 2, "num_workers_per_node");
DEFINE_int32(num_local_load_thread, 2, "num_local_load_thread");
DEFINE_int32(with_injected_straggler, 1, "with injected straggler or not, 0/1");
DEFINE_int32(num_servers_per_node, 1, "num_servers_per_node");
DEFINE_double(alpha, 0.1, "learning rate");

DEFINE_bool(init_dump, true, "init_dump");
DEFINE_bool(use_weight_file, true, "use weight file to restore progress");
DEFINE_bool(checkpoint_toggle, true, "open checkpoint");
DEFINE_string(weight_file_prefix, "", "the prefix filename of weight file");
DEFINE_string(checkpoint_file_prefix, "hdfs://localhost:9000/dump/dump_", "the checkpoint file prefix");
DEFINE_string(checkpoint_raw_prefix, "hdfs:///dump/dump_", "the checkpoint raw prefix");
DEFINE_int32(heartbeat_interval, 10, "the heatbeat check interval");
DEFINE_string(relaunch_cmd,
              "python /Users/aiyongbiao/Desktop/projects/csci5570/scripts/logistic_regression.py relaunch 1",
              "the restart cmd");

namespace csci5570 {

    template<typename T>
    void test_error(third_party::SArray<double> &rets_w, std::vector<T> &data_) {
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

    void RecoverIteration() {
        SVMDumper dumper;
        std::unordered_map<int, int> map = dumper.LoadConfigData();
        for (auto it = map.begin(); it != map.end(); it++) {
            Context::get_instance().SetIteration(it->first, it->second);
        }
    }

    void Training(Node &my_node, std::vector<Node> &nodes, Node &master_node) {
        // 1. Load data
        std::vector<SVMItem> data;
        HDFSManager::Config config;
        config.url = FLAGS_input;
        config.worker_host = my_node.hostname;
        config.worker_port = my_node.port;
        config.master_port = FLAGS_assigner_master_port;
        config.master_host = nodes[0].hostname;
        config.hdfs_namenode = FLAGS_hdfs_namenode;
        config.hdfs_namenode_port = FLAGS_hdfs_namenode_port;
        config.num_local_load_thread = FLAGS_num_local_load_thread;

        bool recovering = FLAGS_use_weight_file;
        if (recovering) {
            LOG(INFO) << "Recovering data...";
            SVMDumper dumper;
            dumper.LoadSVMData(my_node, config, data);
        } else {
            lib::Parser<SVMItem> parser;
            std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
                // parse data
                return parser.parse_libsvm(line);
            };
            lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
            loader.load(config, my_node, nodes, parse, data);
        }
        LOG(INFO) << "Finished loading data on node " << my_node.id;

        // 2. Start engine
        Engine engine(my_node, nodes, master_node);
        engine.StartEverything();
        std::function<void(Node &, std::vector<Node> &, Node &)> restarter_func = Training;
        engine.SetRestarter(restarter_func);

        if (recovering) {
            RecoverIteration();
        }

        // Quit the engine if no traning data is read
        if (data.empty()) {
            LOG(INFO) << "ForceQuit the engine as no data allocated";
            for (auto node : nodes) {
                engine.ForceQuit(node.id);
            }
            engine.StopEverything();
            return;
        }

        if (!recovering) {
            engine.Barrier();
        }

        // 3. Create tables
        nodes = engine.getNodes();
        std::vector<third_party::Range> range = engine.getRanges();

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

        if (!recovering) {
            engine.Barrier();
        }

        // Init CheckPoint Dump callback
        engine.SetDumpCallback([&data]() {
//            SVMDumper dumper;
//            dumper.DumpSVMData(data);
        });

        if (Context::get_instance().get_bool("checkpoint_toggle") &&
            Context::get_instance().get_bool("init_dump")) {
            SVMDumper dumper;
            dumper.DumpSVMData(data);
        }

        // 4. Construct tasks
        MLTask task;
        std::vector<WorkerAlloc> worker_alloc;
        for (auto &node : nodes) {
            worker_alloc.push_back({node.id, (uint32_t) FLAGS_num_workers_per_node});
        }
        task.SetWorkerAlloc(worker_alloc);
        task.SetTables({kTableId});  // Use table 0

        if (recovering) {
            LOG(INFO) << "Wait Fault Barrier on Failed Node=" << FLAGS_my_id;
            engine.Barrier();
            LOG(INFO) << "Pass Fault Barrier on Failed Node=" << FLAGS_my_id;
        }

        task.SetLambda([kTableId, &data, &engine, &recovering](const Info &info) {
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

            for (int i = Context::get_instance().GetIteration(info.worker_id); i < FLAGS_num_iters; i++) {
                CHECK_LT(i, future_keys.size());
                auto &keys = future_keys[i];
//                LOG(INFO) << "Start Get With Iter=" << i << " On Node:" << Context::get_instance().get_int32("my_id");
                table->Get(keys, &params);
//                LOG(INFO) << "End Get With Iter=" << i << " On Node:" << Context::get_instance().get_int32("my_id");
                if (engine.IsNeedRollBack()) {
                    engine.IncRollBackCount();

                    if (info.worker_id % FLAGS_num_workers_per_node == 0) {
                        LOG(INFO) << "Start RecoverIteration On Node:" << Context::get_instance().get_int32("my_id");
                        RecoverIteration();
                        engine.Barrier();
                        engine.RecoverEnd();
                        LOG(INFO) << "End RecoverIteration On Node:" << Context::get_instance().get_int32("my_id");
                    } else {
                        LOG(INFO) << "Start Wait Recover On Worker:" << info.worker_id;
                        engine.WaitRecover();
                        LOG(INFO) << "End Wait Recover On Worker:" << info.worker_id;
                    }
                    i = Context::get_instance().GetIteration(info.worker_id) - 1;
                    continue;
                }

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

                if (i > 0 && i % 300 == 0 && info.worker_id == 0) {
                    auto now = std::chrono::steady_clock::now();
                    LOG(INFO) << "Start checkpoint, sent by worker: 0";
                    table->CheckPoint();
                    auto cost = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time).count();
                    LOG(INFO) << "Finish checkpoint, cost:" << cost << " ms";
                }

                if (i % 100 == 0 && info.worker_id == 0) {
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

                Context::get_instance().SetIteration(info.worker_id, i);
            }
            end_time = std::chrono::steady_clock::now();

            // test error
            if (info.worker_id % FLAGS_num_workers_per_node == 0) {
                table->Get(all_keys, &params);
                test_error<SVMItem>(params, data);
            }

            auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            LOG(INFO) << "total time: " << total_time << " ms on worker: " << info.worker_id;
        });

        // 4. Run tasks
        engine.Run(task);

        // 5. Stop engine
        engine.StopEverything();
    }

    void Run() {
        CHECK_NE(FLAGS_my_id, -1);
        CHECK(!FLAGS_config_file.empty());
        VLOG(1) << FLAGS_my_id << " " << FLAGS_config_file;

        // 0. Parse config_file
        std::vector<Node> nodes = ParseFile(FLAGS_config_file);
        Node master_node = SelectMaster(nodes);
        CHECK(CheckValidNodeIds(nodes));
        CHECK(CheckUniquePort(nodes));

        // launch master node for heartbeating
        if (master_node.id == FLAGS_my_id && master_node.is_master) {
            Master master(master_node, nodes);
            master.StopMaster();
            return;
        }

        Node my_node = GetNodeById(nodes, FLAGS_my_id);
        LOG(INFO) << my_node.DebugString();

        Training(my_node, nodes, master_node);
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    csci5570::Run();
}