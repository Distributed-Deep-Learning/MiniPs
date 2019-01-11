#include "driver/simple_id_mapper.hpp"

#include <cinttypes>
#include <vector>

#include "base/node.hpp"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace minips {

    uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) {
        return tid / kMaxThreadsPerNode;
    }

    void SimpleIdMapper::Init(int num_server_threads_per_node) {
        CHECK_GT(num_server_threads_per_node, 0);
        CHECK_LE(num_server_threads_per_node, kWorkerHelperThreadId);

        // Suppose there are 1 server and 1 worker_thread for each node
        for (const auto &node : nodes_) {
            CHECK_LT(node.id, kMaxNodeId);

            for (int i = 0; i < num_server_threads_per_node; i++) {
                node2server_[node.id].push_back(node.id * kMaxThreadsPerNode + i);
            }

            node2worker_helper_[node.id].push_back(node.id * kMaxThreadsPerNode + kWorkerHelperThreadId);
        }
    }

    void SimpleIdMapper::Update(std::vector<Node> &nodes, int num_server_threads_per_node) {
        LOG(INFO) << "SimpleIdMapper Update start";

        nodes_ = nodes;
        node2server_.clear();
        node2worker_helper_.clear();
        node2worker_.clear();
        Init(num_server_threads_per_node);
    }

    uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
        CHECK(node2worker_helper_.find(node_id) != node2worker_helper_.end());
        CHECK_LE(node2worker_[node_id].size(), kMaxThreadsPerNode - kMaxBgThreadsPerNode);

        for (int i = kMaxBgThreadsPerNode; i < kMaxThreadsPerNode; i++) {
            int tid = i + node_id * kMaxThreadsPerNode;
            if (node2worker_[node_id].find(tid) == node2worker_[node_id].end()) {
                node2worker_[node_id].insert(tid);
                return tid;
            }
        }
        CHECK(false);
        return -1;
    }

    void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
        CHECK(node2worker_helper_.find(node_id) != node2worker_helper_.end());
        CHECK(node2worker_[node_id].find(tid) != node2worker_[node_id].end());

        node2worker_[node_id].erase(tid);
    }

    std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
        return node2server_[node_id];
    }

    std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
        return node2worker_helper_[node_id];
    }

    std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
        return {node2worker_[node_id].begin(), node2worker_[node_id].end()};
    }

    std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
        std::vector<uint32_t> ret;
        for (auto& kv : node2server_) {
            ret.insert(ret.end(), kv.second.begin(), kv.second.end());
        }
        return ret;
    }

    const uint32_t SimpleIdMapper::kMaxNodeId;
    const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
    const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
    const uint32_t SimpleIdMapper::kWorkerHelperThreadId;

}  // namespace minips
