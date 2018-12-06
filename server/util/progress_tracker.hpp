#pragma once

#include <map>
#include <vector>
#include "base/node.hpp"
#include "glog/logging.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <base/context.hpp>
#include "base/third_party/general_fstream.hpp"
#include <math.h>

namespace csci5570 {

    class ProgressTracker {
    public:
        void Init(const std::vector<uint32_t> &tids);

        /**
         * Advance the progress of a worker thread
         * Return -1 if min_clock_ does not change,
         * return min_clock_ otherwise.
         *
         * @param tid worker thread id
         */
        int AdvanceAndGetChangedMinClock(int tid);

        /**
         * Get the progress of a worker thread
         *
         * @param tid worker thread id
         */
        int GetProgress(int tid) const;

        /**
         * Get the progress of the slowest worker
         */
        int GetMinClock() const;

        /**
         * Get the number of workers in the trace
         */
        int GetNumThreads() const;

        /**
         * Check whether a worker thread is the only one with the slowest progress
         *
         * @param tid worker thread id
         */
        bool IsUniqueMin(int tid) const;

        /**
         * Check whether a worker thread is in the trace (thus valid to interact with the model)
         *
         * @param tid worker thread id
         */
        bool CheckThreadValid(int tid) const;

        int DeleteNode(uint32_t node_id);

        int Update(int failed_node_id, std::vector<Node> &nodes);

        int32_t RoundHundred(uint32_t input) {
            return 100 * ::round(input / 100.0);
        }

        void Dump() {
            if (!Context::get_instance().get_bool("checkpoint_toggle")) {
                return;
            }

            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "server_progress_" + std::to_string(node_id);
            LOG(INFO) << "Dump Progress Storage To " << dump_file;

            petuum::io::ofstream w_stream(dump_file, std::ofstream::out | std::ofstream::trunc);
            w_stream << "min_clock" << ":" << RoundHundred(min_clock_) << " ";
            CHECK(w_stream);
            for (auto it = progresses_.begin(); it != progresses_.end(); it++) {
                w_stream << it->first << ":" << RoundHundred(it->second) << " ";
            }
            w_stream.close();
        }

        void Restore() {
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "server_progress_" + std::to_string(node_id);
            LOG(INFO) << "Restore Progress Storage From: " << dump_file;

            std::ifstream input(dump_file.c_str());
            std::string line;
            while (std::getline(input, line)) {
                std::vector<std::string> pairs;
                boost::split(pairs, line, boost::is_any_of(" "));
                for (std::string str : pairs) {
                    std::vector<std::string> pair;
                    boost::split(pair, str, boost::is_any_of(":"));

                    if (pair.size() < 2) {
                        continue;
                    }

                    if (pair[0].compare("min_clock") == 0) {
                        min_clock_ = std::atoi(pair[1].c_str());
                        // LOG(INFO) << "Restore min clock to:" << min_clock_;
                    } else {
                        int model_id = std::atoi(pair[0].c_str());
                        progresses_[model_id] = std::atoi(pair[1].c_str());
                        // LOG(INFO) << "Restore progress model id=" << model_id << ", progress=" << progresses_[model_id];
                    }
                }
            }
            input.close();
        }

        void DebugString() {
            auto node_id = Context::get_instance().get_int32("my_id");
            LOG(INFO) << "DebugString on node=" << std::to_string(node_id);
            for (auto it = progresses_.begin(); it != progresses_.end(); it++) {
                LOG(INFO) << "tid:" << it->first << ", progress:" << it->second;
            }
        }

    private:
        std::map<int, int> progresses_;  // {tid: progress}
        int min_clock_;                  // the slowest progress
    };

}  // namespace csci5570
