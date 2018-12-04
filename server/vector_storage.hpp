//
// Created by aiyongbiao on 2018/9/19.
//

#pragma once

#include "base/message.hpp"
#include "server/abstract_storage.hpp"
#include "glog/logging.h"
#include <vector>
#include "base/context.hpp"
#include "base/third_party/general_fstream.hpp"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

namespace csci5570 {
    template<typename Val>
    class VectorStorage : public AbstractStorage {
    public:
        VectorStorage() = delete;

        VectorStorage(third_party::Range range) : range_(range), storage_(range.size(), Val()) {
//            LOG(INFO) << "Storage allocated from " << range_.begin() << " to " << range_.end() << ", size="
//                      << range_.size();
            CHECK_LE(range_.begin(), range_.end());
        }

        virtual void SubAdd(const third_party::SArray<Key> &typed_keys,
                            const third_party::SArray<char> &vals) override {
            auto typed_vals = third_party::SArray<Val>(vals);
            for (size_t index = 0; index < typed_keys.size(); index++) {
                CHECK_GE(typed_keys[index], range_.begin());
                CHECK_LT(typed_keys[index], range_.end());
                storage_[typed_keys[index] - range_.begin()] += typed_vals[index];
            }
        }

        virtual third_party::SArray<char> SubGet(const third_party::SArray<Key> &typed_keys) override {
            third_party::SArray<Val> reply_vals(typed_keys.size());
            for (size_t i = 0; i < typed_keys.size(); i++) {
                CHECK_GE(typed_keys[i], range_.begin());
                CHECK_LT(typed_keys[i], range_.end());
                reply_vals[i] = storage_[typed_keys[i] - range_.begin()];
            }
            return third_party::SArray<char>(reply_vals);
        }

        virtual void FinishIter() override {}

        virtual void Dump() override {
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "server_params_" + std::to_string(node_id);
            LOG(INFO) << "Dump Params Storage To " << dump_file;

            petuum::io::ofstream w_stream(dump_file, std::ofstream::out | std::ofstream::trunc);
            CHECK(w_stream);
            for (int i = 0; i < storage_.size(); i++) {
                if (storage_[i] != 0) {
                    w_stream << i << ":" << storage_[i] << " ";
                }
            }
            //w_stream << std::endl;
            w_stream.close();
        }

        virtual void Restore() override {
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "server_params_" + std::to_string(node_id);
            LOG(INFO) << "Restore Params Storage From " << dump_file;

            std::ifstream input(dump_file.c_str());
            std::string line;
            while (std::getline(input, line)) {
                std::vector<std::string> pair;
                boost::split(pair, line, boost::is_any_of(" "));
                int index = std::atoi(pair[0].c_str());
                storage_[index] = std::atoi(pair[1].c_str());
            }
            input.close();
        }

        virtual void Update(third_party::Range &range) override {
            range_ = range;
        }

    private:
        std::vector<Val> storage_;
        third_party::Range range_;
    };
}
