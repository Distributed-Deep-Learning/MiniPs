# CSCI5570 Report

## Additional feature

### Fault tolerance

* Store the model parameters and configuration into the file

* Detect error with heartbeat mechanism, restart the engine

* Read and recover the model parameters  after restart

### HDFS Read

* `Question`: When the given training data file size is too small, and the total file blocks is not enough to allocated to every working node, then some of the nodes will crash because the data is null, and finally the cluster will not continue as the barrier will block other nodes.
* `Solution`: When the given data is not enough for every working node, try to safely shutdown the node that haven't allocated any data, make sure other nodes in the cluster continue working.

Here is a data load example in a single working node:

```cpp
std::vector<SVMItem> data;
lib::AbstractDataLoader<SVMItem, std::vector<SVMItem>> loader;
lib::Parser<SVMItem> parser;
std::function<SVMItem(boost::string_ref)> parse = [parser](boost::string_ref line) {
    // parse data
    return parser.parse_libsvm(line);
};
loader.load(config, my_node, nodes, parse, data);
LOG(INFO) << "Finished loading data!";
```

When the HDFS file block have no data left for read, the data pointer will be NULL, then the app will crash when the task is running.

This is because `hdfs_file_splitter` read the training file block by block, and [node 0] allocate files to other working node. `hdfs_block_size` is usually 128KB or 128MB in HDFS.

io/hdfs_file_splitter.cpp

```cpp
int HDFSFileSplitter::read_block(const std::string &fn) {
    file_ = hdfsOpenFile(fs_, fn.c_str(), O_RDONLY, 0, 0, 0);
    CHECK(file_ != NULL) << "HDFS file open fails";
    hdfsSeek(fs_, file_, offset_);
    size_t start = 0;
    size_t nbytes = 0;
    while (start < hdfs_block_size) {
        nbytes = hdfsRead(fs_, file_, data_ + start, hdfs_block_size);
        start += nbytes;
        if (nbytes == 0)
            break;
    }
    return start;
}
```

When this happened, the working node which no data for trainning should be safely stopped without stop the cluster running.

To solving this, we need detect the no-trainning-data node, and shut it down carefully in the beginning stage.

comm/mailbox.cpp

```cpp
if (msg.meta.flag == Flag::kForceQuit) {
    LOG(INFO) << "Received kForceQuit from" << msg.meta.sender;
    nodes_.erase(std::remove_if(nodes_.begin(), nodes_.end(), [&](Node const & node) {
        return msg.meta.sender == node.id;
    }), nodes_.end());

    engine_->UpdateNodes(nodes_);

    std::unique_lock<std::mutex> lk(mu_);
    quit_count_ += 1;
    if (barrier_count_ + quit_count_ >= nodes_.size()) {
        barrier_cond_.notify_one();
    }
}
```

driver/engine.cpp

```cpp
void Engine::UpdateNodes(std::vector<Node> &nodes) {
    nodes_ = nodes;
    id_mapper_->Update(nodes, 1);
}
```

Force-quit the node if the empty data was read

```cpp
// Quit the engine if no traning data is read
if (data.empty()) {
    LOG(INFO) << "ForceQuit the engine as no data allocated";
    for (auto node : nodes) {
        engine.ForceQuit(node.id);
    }
    engine.StopEverything();
    return;
}
engine.Barrier();
```

## Application

### Logistisc Regression

### LDA

### Matrix Factorization

