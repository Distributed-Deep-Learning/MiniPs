#pragma once

#include <functional>
#include <string>
#include "boost/utility/string_ref.hpp"
#include "lib/parser.hpp"
#include <io/hdfs_manager.hpp>
#include <base/node.hpp>
#include <vector>

namespace minips {
    namespace lib {

        // template <typename Sample, template <typename> typename DataStore<Sample>>
        template<typename Sample, typename DataStore>
        class AbstractDataLoader {
        public:
            /**
             * Load samples from the url into datastore
             *
             * @param url          input file/directory
             * @param parse        a parsing function
             * @param datastore    a container for the samples / external in-memory storage abstraction
             */
            template<typename Parse>
            // e.g. std::function<Sample(boost::string_ref, int)>
            static void load(HDFSManager::Config config, Node my_node, std::vector<Node> nodes,
                             Parse parse, DataStore &datastore) {
                // 1. Connect to the data source, e.g. HDFS, via the modules in io
                zmq::context_t* zmq_context = new zmq::context_t(1);
                HDFSManager hdfs_manager(my_node, nodes, config, zmq_context);
                hdfs_manager.Start();

                std::mutex lock;
                hdfs_manager.Run([my_node, &datastore, parse, &lock](HDFSManager::InputFormat* input_format, int local_tid) {
                    // 2. Extract and parse lines
                    int count = 0;
                    while (input_format->HasNext()) {
                        auto item = input_format->GetNextItem();
                        if (item.empty()) return;

                        // 3. Put samples into datastore
                        auto data = parse(item);
                        lock.lock();
                        datastore.push_back(data);
                        count++;
                        lock.unlock();
                    }
                });
                hdfs_manager.Stop();
            }

        };  // class AbstractDataLoader

    }  // namespace lib
}  // namespace minips
