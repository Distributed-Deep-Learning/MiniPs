//
// Created by aiyongbiao on 2018/9/25.
//

#include <lib/abstract_data_loader.hpp>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include <lib/parser.hpp>
#include <lib/labeled_sample.hpp>
#include "boost/utility/string_ref.hpp"
#include <functional>
#include "io/hdfs_manager.hpp"

namespace csci5570 {

    void Run() {
        lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
        lib::Parser<SVMItem> parser;

        std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
            return parser.parse_libsvm(line);
        };
        std::vector<SVMItem> datastore;

        HDFSManager::Config config;
        config.master_host = "localhost";
        config.master_port = 19817;
        config.url = "hdfs:///datasets/classification/a1a";
        config.worker_host = "localhost";
        config.worker_port = 14560;
        config.hdfs_namenode = "localhost";
        config.hdfs_namenode_port = 9000;
        config.num_local_load_thread = 2;

        Node my_node;
        my_node.id = 0;
        my_node.hostname = "localhost";
        my_node.port = 33254;
        std::vector<Node> nodes = {my_node};

        loader.load(config, my_node, nodes, parse, &datastore);
        CHECK_EQ(datastore.size(), 1605);
    }

}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    csci5570::Run();
}