#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <vector>
#include <string>
#include <unordered_map>

namespace minips {

// An extension of google flags. It is a singleton that stores 1) google flags
// and 2) other lightweight global flags. Underlying data structure is map of
// string and string, similar to google::CommandLineFlagInfo.
    class Context {
    public:
        static Context &get_instance();

        int get_int32(std::string key);

        double get_double(std::string key);

        bool get_bool(std::string key);

        std::string get_string(std::string key);

        void set(std::string key, int value);

        void set(std::string key, double value);

        void set(std::string key, bool value);

        void set(std::string key, std::string value);

        void set(std::string key, std::unordered_map<int, int> map);

        void SetIteration(int worker_id, int cur_iter) {
            iteration_map_[worker_id] = cur_iter;
        }

        std::unordered_map<int, int> GetIterationMap() {
            return iteration_map_;
        };

        int GetIteration(int worker_id) {
            if (Context::get_instance().get_bool("scale")) {
                worker_id = worker_id % Context::get_instance().get_int32("num_workers_per_node");
            }
            int result = 0;
            if (iteration_map_.count(worker_id)) {
                result = iteration_map_[worker_id];
            }
            return result;
        }

    private:
        // Private constructor. Store all the gflags values.
        Context();

        // Underlying data structure
        std::unordered_map<std::string, std::string> ctx_;

        std::unordered_map<int, int> iteration_map_;
    };

}   // namespace minips
