#pragma once

#include <cinttypes>
#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/third_party/sarray.h"

#include "glog/logging.h"

namespace csci5570 {

    class RangePartitionManager : public AbstractPartitionManager {
    public:
        RangePartitionManager(const std::vector<uint32_t> &server_thread_ids,
                              const std::vector<third_party::Range> &ranges, int32_t master_node_id = -1)
                : AbstractPartitionManager(server_thread_ids, master_node_id), ranges_(ranges) {
            CHECK_EQ(ranges_.size(), server_thread_ids_.size());
        }

        const std::vector<third_party::Range> &GetRanges() const { return ranges_; }

        void Slice(const Keys &keys, std::vector<std::pair<int, Keys>> *sliced) const override {
            int server_count = GetNumServers();
            sliced->reserve(server_count);

            auto iterator = std::lower_bound(keys.begin(), keys.end(), ranges_[0].begin());
            size_t cur_split_start_index = iterator - keys.begin();

            for (int i = 0; i < server_count; i++) {
                iterator = std::lower_bound(iterator, keys.end(), ranges_[i].end());
                auto cur_split_end_index = iterator - keys.begin();

                if (cur_split_end_index > cur_split_start_index) {
                    Keys k = keys.segment(cur_split_start_index, cur_split_end_index);
                    sliced->push_back(std::make_pair(server_thread_ids_[i], k));
                }
                cur_split_start_index = cur_split_end_index;
            }
        }

        void Slice(const KVPairs &kvs, std::vector<std::pair<int, KVPairs>> *sliced) const override {
            int server_count = GetNumServers();
            sliced->reserve(server_count);

            auto values_ratio = kvs.second.size() / kvs.first.size();
            auto iterator = std::lower_bound(kvs.first.begin(), kvs.first.end(), ranges_[0].begin());
            size_t cur_split_start_index = iterator - kvs.first.begin();

            for (int i = 0; i < server_count; i++) {
                iterator = std::lower_bound(iterator, kvs.first.end(), ranges_[i].end());
                auto cur_split_end_index = iterator - kvs.first.begin();

                if (cur_split_end_index > cur_split_start_index) {
                    KVPairs kv;
                    kv.first = kvs.first.segment(cur_split_start_index, cur_split_end_index);
                    kv.second = kvs.second.segment(cur_split_start_index * values_ratio,
                                                   cur_split_end_index * values_ratio);
                    sliced->push_back(std::make_pair(server_thread_ids_[i], std::move(kv)));
                }
                cur_split_start_index = cur_split_end_index;
            }
        }

    private:
        std::vector<third_party::Range> ranges_;
    };

}  // namespace csci5570
