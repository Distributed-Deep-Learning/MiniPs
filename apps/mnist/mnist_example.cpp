
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

namespace csci5570 {

    void Run() {
        lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
        lib::Parser<SVMItem> parser;

        std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
            return parser.parse_mnist(line);
        };
        std::vector<SVMItem> datastore;
        loader.load("hdfs:///datasets/classification/a1a", parse, &datastore);
        CHECK_EQ(datastore.size(), 1605);
    }

}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    csci5570::Run();
}