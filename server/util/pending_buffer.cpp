#include "server/util/pending_buffer.hpp"

namespace csci5570 {

    std::vector<Message> PendingBuffer::Pop(const int clock) {
        std::vector<Message> pop_msg;
        if (buffer_.find(clock) != buffer_.end()) {
            pop_msg = std::move(buffer_[clock]);
            buffer_.erase(clock);
        }
        return pop_msg;
    }

    std::vector<Message> PendingBuffer::PopAll() {
        std::vector<Message> pop_msg;
        for (auto it = buffer_.begin(); it != buffer_.end(); it++) {
            std::vector<Message> msgs = it->second;
            for (Message msg : msgs) {
                pop_msg.push_back(msg);
            }
        }
        buffer_.clear();
        return pop_msg;
    }

    void PendingBuffer::EraseAll() {
        buffer_.clear();
    }

    void PendingBuffer::Push(const int clock, Message &msg) {
        // TODO: check the clock passed in is less than the minimum clock in the buffer
        buffer_[clock].push_back(std::move(msg));
    }

    int PendingBuffer::Size(const int progress) {
        return buffer_[progress].size();
    }

}  // namespace csci5570
