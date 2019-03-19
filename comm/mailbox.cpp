#include "comm/mailbox.hpp"

#include <algorithm>
#include <lib/svm_dumper.hpp>
#include "glog/logging.h"

namespace minips {

    inline void FreeData(void *data, void *hint) {
        if (hint == NULL) {
            delete[] static_cast<char *>(data);
        } else {
            delete static_cast<third_party::SArray<char> *>(hint);
        }
    }

    Mailbox::Mailbox(const Node &node, const std::vector<Node> &nodes, AbstractIdMapper *id_mapper, Engine *engine)
            : node_(node), nodes_(nodes), id_mapper_(id_mapper), engine_(engine) {
        // Do some checks
        CHECK(nodes_.size());
        // CHECK(std::find(nodes_.begin(), nodes_.end(), node_) != nodes_.end());
        CHECK_NOTNULL(id_mapper_);
        // Check for uniqueness
        for (int i = 0; i < nodes.size(); ++i) {
            for (int j = 0; j < nodes.size(); ++j) {
                if (i != j) {
                    CHECK_NE(nodes[i].id, nodes[j].id);
                }
            }
        }
    }

    size_t Mailbox::GetQueueMapSize() const { return queue_map_.size(); }

    void Mailbox::Start(const Node &master_node, const Node &scale_node) {
        ConnectAndBind(master_node, scale_node);
        StartReceiving();
    }

    void Mailbox::ConnectAndBind(const Node &master_node, const Node &scale_node) {
        context_ = zmq_ctx_new();
        CHECK(context_ != nullptr) << "create zmq context failed";
        zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);

        if (Context::get_instance().get_bool("scale")) {
            Bind(scale_node);
        } else {
            Bind(node_);
        }
        VLOG(1) << "Finished binding";
        for (const auto &node : nodes_) {
            Connect(node);
        }
        if (master_node.is_master) {
            Connect(master_node);
        }
        if (Context::get_instance().get_bool("scale")) {
            Connect(scale_node);
        }
        VLOG(1) << "Finished connecting";
    }

    void Mailbox::StartReceiving() {
        receiver_thread_ = std::thread(&Mailbox::Receiving, this);
    }

    void Mailbox::Stop(bool barrier) {
        StopReceiving(barrier);
        CloseSockets();
    }

    void Mailbox::StopReceiving(bool barrier) {
        if (barrier) {
            Barrier();
        }
        Message exit_msg;
        exit_msg.meta.recver = node_.id;
        exit_msg.meta.flag = Flag::kExit;
        Send(exit_msg);
        receiver_thread_.join();
    }

    void Mailbox::CloseSockets() {
        // Kill all the registered threads
        Message exit_msg;
        exit_msg.meta.recver = node_.id;
        exit_msg.meta.flag = Flag::kExit;
        for (auto &queue : queue_map_) {
            queue.second->Push(exit_msg);
        }
        // close sockets
        int linger = -1;  // infinite linger period. Wait for all pending messages to be sent.
        int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
        CHECK(rc == 0 || errno == ETERM);
        CHECK_EQ(zmq_close(receiver_), 0);
        for (auto &it : senders_) {
            int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
            CHECK(rc == 0 || errno == ETERM);
            CHECK_EQ(zmq_close(it.second), 0);
        }
        //LOG(INFO) << "zmq_ctx_destroy on node:" << node_.id << ", start";
        // zmq_ctx_destroy(context_);
        //LOG(INFO) << "zmq_ctx_destroy on node:" << node_.id << ", end";
    }

    void Mailbox::Connect(const Node &node) {
        auto it = senders_.find(node.id);
        if (it != senders_.end()) {
            zmq_close(it->second);
        }
        void *sender = zmq_socket(context_, ZMQ_DEALER);
        CHECK(sender != nullptr) << zmq_strerror(errno);
        std::string my_id = "ps" + std::to_string(node_.id);
        zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
        std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
        if (zmq_connect(sender, addr.c_str()) != 0) {
            LOG(FATAL) << "connect to " + addr + " failed: " << zmq_strerror(errno);
        }
        senders_[node.id] = sender;
    }

    void Mailbox::Bind(const Node &node) {
        receiver_ = zmq_socket(context_, ZMQ_ROUTER);
        CHECK(receiver_ != nullptr) << "create receiver socket failed: " << zmq_strerror(errno);
        std::string address = "tcp://*:" + std::to_string(node.port);
        if (zmq_bind(receiver_, address.c_str()) != 0) {
            LOG(FATAL) << "bind to " + address + " failed: " << zmq_strerror(errno);
        }
    }

    void Mailbox::RegisterQueue(uint32_t queue_id, ThreadsafeQueue<Message> *const queue) {
        CHECK(queue_map_.find(queue_id) == queue_map_.end());
        queue_map_.insert({queue_id, queue});
    }

    void Mailbox::DeregisterQueue(uint32_t queue_id) {
        std::lock_guard<std::mutex> lk(mu_);
        CHECK(queue_map_.find(queue_id) != queue_map_.end());
        queue_map_.erase(queue_id);
    }

    void Mailbox::Receiving() {
        VLOG(1) << "Start receiving";
        while (true) {
            Message msg;
            int recv_bytes = Recv(&msg);
            if (msg.meta.flag == Flag::kExit) {
                break;
            } else if (msg.meta.flag == Flag::kBarrier) {
                std::unique_lock<std::mutex> lk(mu_);
                barrier_count_ += 1;
                LOG(INFO) << "Received barrier from node=" << msg.meta.sender << ", on node=" << node_.id
                          << ", current count=" << barrier_count_;
                if (barrier_count_ >= nodes_.size()) {
                    LOG(INFO) << "Collected " << nodes_.size() << " barrier, Node:"
                              << node_.id << " unblocking main thread with count=" << barrier_count_;
                    barrier_cond_.notify_all();
                }
            } else if (msg.meta.flag == Flag::kForceQuit) {
                LOG(INFO) << "Received kForceQuit from" << msg.meta.sender;
                nodes_.erase(std::remove_if(nodes_.begin(), nodes_.end(), [&](Node const &node) {
                    return msg.meta.sender == node.id;
                }), nodes_.end());

                engine_->UpdateNodes(nodes_);

                std::unique_lock<std::mutex> lk(mu_);
                quit_count_ += 1;
                if (barrier_count_ + quit_count_ >= nodes_.size()) {
                    barrier_cond_.notify_one();
                }
            } else if (msg.meta.flag == Flag::kRollBack) {
//                nodes_.erase(std::remove_if(nodes_.begin(), nodes_.end(), [&](Node const &node) {
//                    return msg.meta.sender == node.id;
//                }), nodes_.end());

                int failed_node_id = msg.meta.sender;
                if (failed_node_id != node_.id) {
                    LOG(INFO) << "Receving node dead msg:" << failed_node_id << ", on node=" << node_.id;
                    SVMDumper dumper;
                    std::unordered_map<int, int> map = dumper.LoadConfigData();
                    for (auto it = map.begin(); it != map.end(); it++) {
                        Context::get_instance().SetIteration(it->first, it->second);
                        LOG(INFO) << "Restore iteration on worker=" << it->first << ", with iter=" << it->second;
                    }

                    engine_->SetNeedRollBack(true);
                    engine_->RollBackServer();
                    engine_->RollBackWorker();
                }
//                engine_->UpdateAndRestart(failed_node_id, nodes_);

            } else if (msg.meta.flag == Flag::kCheckpoint) {
                engine_->RunDumpCallback();
                CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end());
                queue_map_[msg.meta.recver]->Push(std::move(msg));
            } else if (msg.meta.flag == Flag::kScaleRollback) {
                int failed_node_id = msg.meta.sender;
                if (failed_node_id != node_.id) {
                    LOG(INFO) << "Receving node scale msg:" << failed_node_id << ", on node=" << node_.id;
                    SVMDumper dumper;
                    std::unordered_map<int, int> map = dumper.LoadConfigData();
                    for (auto it = map.begin(); it != map.end(); it++) {
                        Context::get_instance().SetIteration(it->first, it->second);
                        LOG(INFO) << "Restore iteration on worker=" << it->first << ", with iter=" << it->second;
                    }

                    Node node = dumper.LoadScaleFile();
                    CHECK_EQ(failed_node_id, node.id);
                    LOG(INFO) << "kScaleRollback node id=" << node.id << ", and start connect to " << node.hostname
                              << " on port=" << node.port;
//                    nodes_.push_back(node);
                    Connect(node);
                    engine_->SetScaleNode(node);

                    engine_->SetNeedRollBack(true);
                    engine_->RollBackServer();
                    engine_->RollBackWorker();
                }
            } else {
                // CHECK(queue_map_.find(msg.meta.recver) != queue_map_.end()) << msg.DebugString();
                if (queue_map_.find(msg.meta.recver) != queue_map_.end()) {
                    queue_map_[msg.meta.recver]->Push(std::move(msg));
                } else {
                    LOG(INFO) << "Fucker!!! ingore";
                }
            }
        }
    }

    int Mailbox::Send(const Message &msg) {
        std::lock_guard<std::mutex> lk(mu_);
        // find the socket
        int id;
        if (msg.meta.flag == Flag::kBarrier || msg.meta.flag == Flag::kExit ||
            msg.meta.flag == Flag::kForceQuit || msg.meta.flag == Flag::kHeartBeat ||
            msg.meta.flag == Flag::kQuitHeartBeat || msg.meta.flag == Flag::kRollBack ||
            msg.meta.flag == Flag::kScale || msg.meta.flag == Flag::kScaleRollback) {
            // For kBarrier, kExit and kForceQuit which are sent by the Mailbox directly, no need to lookup for node id.
            id = msg.meta.recver;
        } else {
            id = id_mapper_->GetNodeIdForThread(msg.meta.recver);
        }

//        if (msg.meta.flag != Flag::kHeartBeat && msg.meta.flag != Flag::kQuitHeartBeat) {
//            auto node_it = std::find_if(nodes_.begin(), nodes_.end(), [&](const Node &node) {
//                return node.id == id;
//            });
//            if (node_it == nodes_.end()) {
//                LOG(WARNING) << "this node already quit " << id;
//                return -1;
//            }
//        }

        auto it = senders_.find(id);
        if (it == senders_.end()) {
            LOG(WARNING) << "there is no socket to node " << id;
            return -1;
        }

        void *socket = it->second;

        // send meta
        int meta_size = sizeof(Meta);

        int tag = ZMQ_SNDMORE;
        int num_data = msg.data.size();
        if (num_data == 0)
            tag = 0;
        char *meta_buf = new char[meta_size];
        memcpy(meta_buf, &msg.meta, meta_size);
        zmq_msg_t meta_msg;
        zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
        while (true) {
            if (zmq_msg_send(&meta_msg, socket, tag) == meta_size)
                break;
            if (errno == EINTR)
                continue;
            LOG(WARNING) << "failed to send message to node [" << id << "] errno: " << errno << " "
                         << zmq_strerror(errno);
            return -1;
        }
        zmq_msg_close(&meta_msg);
        int send_bytes = meta_size;

        // send data
        for (int i = 0; i < num_data; ++i) {
            zmq_msg_t data_msg;
            third_party::SArray<char> *data = new third_party::SArray<char>(msg.data[i]);
            int data_size = data->size();
            zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
            if (i == num_data - 1)
                tag = 0;
            while (true) {
                if (zmq_msg_send(&data_msg, socket, tag) == data_size)
                    break;
                if (errno == EINTR)
                    continue;
                LOG(WARNING) << "failed to send message to node [" << id << "] errno: " << errno << " "
                             << zmq_strerror(errno)
                             << ". " << i << "/" << num_data;
                return -1;
            }
            zmq_msg_close(&data_msg);
            send_bytes += data_size;
        }
        return send_bytes;
    }

    int Mailbox::Recv(Message *msg) {
        msg->data.clear();
        size_t recv_bytes = 0;
        for (int i = 0;; ++i) {
            zmq_msg_t *zmsg = new zmq_msg_t;
            CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
            while (true) {
                if (zmq_msg_recv(zmsg, receiver_, 0) != -1)
                    break;
                if (errno == EINTR)
                    continue;
                LOG(WARNING) << "failed to receive message. errno: " << errno << " " << zmq_strerror(errno);
                return -1;
            }

            size_t size = zmq_msg_size(zmsg);
            recv_bytes += size;

            if (i == 0) {
                // identify, don't care
                CHECK(zmq_msg_more(zmsg));
                zmq_msg_close(zmsg);
                delete zmsg;
            } else if (i == 1) {
                // Unpack the meta
                Meta *meta = CHECK_NOTNULL((Meta *) zmq_msg_data(zmsg));
                msg->meta.sender = meta->sender;
                msg->meta.recver = meta->recver;
                msg->meta.model_id = meta->model_id;
                msg->meta.flag = meta->flag;
                zmq_msg_close(zmsg);
                bool more = zmq_msg_more(zmsg);
                delete zmsg;
                if (!more)
                    break;
            } else {
                // data, zero-copy
                char *buf = CHECK_NOTNULL((char *) zmq_msg_data(zmsg));
                third_party::SArray<char> data;
                data.reset(buf, size, [zmsg, size](char *buf) {
                    zmq_msg_close(zmsg);
                    delete zmsg;
                });
                msg->data.push_back(data);
                if (!zmq_msg_more(zmsg)) {
                    break;
                }
            }
        }
        return recv_bytes;
    }

    void Mailbox::Barrier(bool send) {
        if (send) {
            for (auto &node : nodes_) {
                Message barrier_msg;
                barrier_msg.meta.sender = node_.id;
                barrier_msg.meta.recver = node.id;
                barrier_msg.meta.flag = Flag::kBarrier;
                Send(barrier_msg);
            }

            if (engine_->HasScaleNode()) {
                Message barrier_msg;
                barrier_msg.meta.sender = node_.id;
                barrier_msg.meta.recver = engine_->GetScaleNode().id;
                barrier_msg.meta.flag = Flag::kBarrier;
                Send(barrier_msg);
            }
        }

        int32_t target_size = static_cast<int32_t>(engine_->HasScaleNode() ? nodes_.size() + 1 : nodes_.size());

        std::unique_lock<std::mutex> lk(mu_);
        // Very tricky. Consider to use all-one-all method instead of all-all.
        barrier_cond_.wait(lk, [this, target_size]() {
            return barrier_count_ >= target_size;
        });

        if (send) {
            barrier_count_ -= target_size;
            if (barrier_count_ < 0) {
                LOG(INFO) << "barrier count not right with=" << barrier_count_;
                barrier_count_ = 0;
            }
        }
    }

    void Mailbox::ForceQuit(uint32_t node_id) {
        for (auto node : nodes_) {
            Message quit_msg;
            quit_msg.meta.sender = node_.id;
            quit_msg.meta.recver = node.id;
            quit_msg.meta.flag = Flag::kForceQuit;
            Send(quit_msg);
        }
    }

    void Mailbox::SafeQuit() {

    }

}  // namespace minips
