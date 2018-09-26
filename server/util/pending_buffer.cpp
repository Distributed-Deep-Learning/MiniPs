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

    void PendingBuffer::Push(const int clock, Message &msg) {
        // TODO: check the clock passed in is less than the minimum clock in the buffer
        buffer_[clock].push_back(std::move(msg));
    }

    int PendingBuffer::Size(const int progress) {
        return buffer_[progress].size();
    }

}  // namespace csci5570
