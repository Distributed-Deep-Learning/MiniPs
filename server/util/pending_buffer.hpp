#pragma once

#include "base/message.hpp"

#include <unordered_map>

namespace csci5570 {

    class PendingBuffer {
    public:
        /**
         * Return the pending requests at the specific progress clock
         */
        virtual std::vector<Message> Pop(const int clock);

        virtual std::vector<Message> PopAll();

        virtual void EraseAll();

        /**
         * Add the pending requests at the specific progress clock
         */
        virtual void Push(const int clock, Message &message);

        /**
         * Return the number of pending requests at the specific progress
         */
        virtual int Size(const int progress);


    private:
        // TODO: each vec of buffer_ should be pop one by one, now it is not guaranteed
        std::unordered_map<int, std::vector<Message>> buffer_;
    };

}  // namespace csci5570
