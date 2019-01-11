//
// Created by aiyongbiao on 2018/9/25.
//

#pragma once

#include <thread>
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "base/node.hpp"
#include "base/serialization.hpp"
#include "boost/utility/string_ref.hpp"
#include "io/coordinator.hpp"
#include "io/line_input_format.hpp"

namespace minips {

    class HDFSManager {
    public:
        struct Config {
            std::string master_host;
            int master_port;
            std::string worker_host;
            int worker_port;
            std::string hdfs_namenode;
            int hdfs_namenode_port;
            std::string url;
            int num_local_load_thread;
        };

        struct InputFormat {
            LineInputFormat *infmt_;
            boost::string_ref item;

            InputFormat(const Config &config, Coordinator *coordinator, int num_threads) {
                infmt_ = new LineInputFormat(config.url, num_threads, 0, coordinator, config.worker_host,
                                             config.hdfs_namenode,
                                             config.hdfs_namenode_port);
            }

            bool HasNext() {
                return infmt_->next(item);
            }

            boost::string_ref GetNextItem() { return item; }
        };

        HDFSManager(Node node, const std::vector<Node> &nodes, const Config &config, zmq::context_t *zmq_context);

        void Start();

        void Run(const std::function<void(InputFormat *, int)> &func);

        void Stop();

    private:
        const Node node_;
        const std::vector<Node> nodes_;
        const Config config_;
        Coordinator *coordinator_;
        zmq::context_t *zmq_context_;
        std::thread hdfs_main_thread_;  // Only in Node0
    };
}


