#!/usr/bin/env python

import sys
from launch_utils import launch_util

local_debug = True if len(sys.argv) >= 2 and sys.argv[1] == "local" else False

hostfile = "config/localnodes" if local_debug else "config/clusternodes"
progfile = ("cmake-build-debug" if local_debug else "debug") + "/KMeans"

params = {
    "hdfs_namenode": "localhost" if local_debug else "proj10",
    "input": "hdfs:///a9a" if local_debug else "hdfs:///datasets/classification/webspam",
    "kStaleness": 1,
    "kModelType": "SSP",  # {ASP/SSP/BSP}
    "num_dims": 123 if local_debug else 16609143,
    "num_workers_per_node": 2,  # 3
    "num_servers_per_node": 1,
    "num_local_load_thread": 100,
    "num_iters": 10,  # 1000
    "kStorageType": "Vector",  # {Vector/Map}
    "hdfs_namenode_port": 9000,
    "K": 2,
    "batch_size": 6,  # 100
    "alpha": 0.1,
    "kmeans_init_mode": "random",
    "report_interval": 5,
    "checkpoint_toggle": False,
    "use_weight_file": False,
    "init_dump": True if local_debug else False,
    "weight_file_prefix": "",
    "heartbeat_interval": 10 if local_debug else 15, # join(proj_dir, "local/dump_")
    "checkpoint_file_prefix": "hdfs://localhost:9000/dump/dump_" if local_debug else "hdfs://proj10:9000/ybai/dump_",
    "checkpoint_raw_prefix": "hdfs:///dump/dump_" if local_debug else "hdfs:///ybai/dump_",
    "relaunch_cmd": "",
}

env_params = (
    "GLOG_logtostderr=true "
    "GLOG_v=-1 "
    "GLOG_minloglevel=0 "
)

# this is to enable hdfs short-circuit read (disable the warning info)
# change this path accordingly when we use other cluster
# the current setting is for proj5-10
if (local_debug is False):
    env_params += "LIBHDFS3_CONF=/data/opt/course/hadoop/etc/hadoop/hdfs-site.xml"

launch_util(progfile, hostfile, env_params, params, sys.argv)
