#pragma once

#include "boost/utility/string_ref.hpp"
#include <base/magic.hpp>

namespace minips {
    namespace lib {

        template<typename Sample>
        class Parser {
        public:
            /**
             * Parsing logic for one line in file
             *
             * @param line  a line read from the input file
             */
            static Sample parse_libsvm(boost::string_ref line) {
                // check the LibSVM format and complete the parsing
                // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
                // so far we tried all the tree and found std::strtok_r is fastest :)
                SVMItem item;
                char* pos;
                std::unique_ptr<char> buffer(new char[line.size() + 1]);
                strncpy(buffer.get(), line.data(), line.size());
                buffer.get()[line.size()] = '\0';
                char* token = strtok_r(buffer.get(), " \t:", &pos);

                int i = -1;
                int index;
                double value;
                while (token != nullptr) {
                    if (i == 0) {
                        index = std::atoi(token) - 1;
                        i = 1;
                    } else if (i == 1) {
                        value = std::atof(token);
                        item.first.push_back(std::make_pair(index, value));
                        i = 0;
                    } else {
                        // the class label
                        item.second = std::atof(token);
                        i = 0;
                    }
                    token = strtok_r(nullptr, " \t:", &pos);
                }
                return item;
            }

            static Sample parse_mnist(boost::string_ref line) {
                // check the MNIST format and complete the parsing
                // TODO: may be done in the future
            }

            // You may implement other parsing logic

        };  // class Parser

    }  // namespace lib
}  // namespace minips
