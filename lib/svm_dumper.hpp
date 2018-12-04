
//
// Created by aiyongbiao on 2018/11/13.
//

#pragma once

#include <base/magic.hpp>
#include <base/context.hpp>
#include <base/third_party/general_fstream.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

namespace csci5570 {

    class SVMDumper {
    public:
        // std::pair<std::vector<std::pair<int, double>>, double>
        void DumpSVMData(std::vector<SVMItem> data) {
            if (!Context::get_instance().get_bool("use_weight_file")) {
                return;
            }
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "worker_" + std::to_string(node_id);
            LOG(INFO) << "Dump Data To: " << dump_file;

            petuum::io::ofstream w_stream(dump_file, std::ofstream::out | std::ofstream::trunc);
            CHECK(w_stream);
            for (SVMItem item : data) {
                w_stream << item.second << " ";
                for (auto pair : item.first) {
                    w_stream << pair.first << ":" << pair.second << " ";
                }
                w_stream << "\n";
            }
            //w_stream << std::endl;
            w_stream.close();
        }

        int32_t RoundHundred(uint32_t input) {
            return 100 * round(input / 100.0);
        }

        void DumpConfigData(std::unordered_map<int, int> iter_map) {
            if (!Context::get_instance().get_bool("use_weight_file")) {
                return;
            }
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "worker_config_" + std::to_string(node_id);
            LOG(INFO) << "Dump Config To: " << dump_file;

            petuum::io::ofstream w_stream(dump_file, std::ofstream::out | std::ofstream::trunc);
            CHECK(w_stream);
            for (auto it = iter_map.begin(); it != iter_map.end(); it++) {
                w_stream << it->first << ":" << RoundHundred(it->second) << " ";
            }
            w_stream.close();
        }

        std::unordered_map<int, int> LoadConfigData() {
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "worker_config_" + std::to_string(node_id);
            LOG(INFO) << "Load Config From: " << dump_file;

            std::unordered_map<int, int> result_map;
            std::ifstream input(dump_file.c_str());
            std::string line;
            while (std::getline(input, line)) {
                std::vector<std::string> tokens;
                boost::split(tokens, line, boost::is_any_of(" "));
                for (int i = 0; i < tokens.size(); i++) {
                    std::vector<std::string> pair_items;
                    boost::split(pair_items, tokens[i], boost::is_any_of(":"));
                    if (pair_items.size() < 2) {
                        continue;
                    }

                    int worker_id = std::atoi(pair_items[0].c_str());
                    int iter = std::atoi(pair_items[1].c_str());
                    LOG(INFO) << "Load iter on worker:" << worker_id << ", with iter:" << iter;
                    result_map[worker_id] = iter;
                }
            }
            return result_map;
        }

        std::vector<SVMItem> LoadSVMData() {
            auto dump_prefix = Context::get_instance().get_string("checkpoint_file_prefix");
            auto node_id = Context::get_instance().get_int32("my_id");
            auto dump_file = dump_prefix + "worker_" + std::to_string(node_id);
            LOG(INFO) << "Load Data From: " << dump_file;

            std::vector<SVMItem> data;
            std::ifstream input(dump_file.c_str());
            std::string line;
            while (std::getline(input, line)) {
                SVMItem item;
                std::vector<std::string> tokens;
                boost::split(tokens, line, boost::is_any_of(" "));
                for (int i = 0; i < tokens.size(); i++) {
                    if (i == 0) {
                        item.second = std::atof(tokens[i].c_str());
                    } else {
                        std::vector<std::string> pair_items;
                        boost::split(pair_items, tokens[i], boost::is_any_of(":"));
                        if (pair_items.size() < 2) {
                            continue;
                        }

                        std::pair<int, double> pair_item;
                        pair_item.first = std::atoi(pair_items[0].c_str());;
                        pair_item.second = std::atof(pair_items[1].c_str());
                        item.first.push_back(pair_item);
                    }
                }
                data.push_back(item);
            }
            input.close();
            return data;
        }

    };

}
