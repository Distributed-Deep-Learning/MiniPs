#!/usr/bin/env python

import sys
from launch_utils import launch_util

local_debug = True if len(sys.argv) >= 2 and sys.argv[1] == "local" else False

hostfile = "config/localnodes" if local_debug else "config/clusternodes"
progfile = ("cmake-build-debug" if local_debug else "debug") + "/KMeans"

params = {
    "hdfs_namenode": "localhost" if local_debug else "proj10",
    "input": "hdfs:///a9a" if local_debug else "hdfs:///jasper/SUSY",
    "kStaleness": 1,
    "kModelType": "SSP",  # {ASP/SSP/BSP}
    "num_dims": 18,
    "num_workers_per_node": 3,  # 3
    "num_servers_per_node": 1,
    "num_local_load_thread": 100,
    "num_iters": 1000,  # 1000
    "kStorageType": "Vector",  # {Vector/Map}
    "hdfs_namenode_port": 9000,
    "K": 2,
    "batch_size": 1000,  # 100
    "alpha": 0.1,
    "kmeans_init_mode": "random",
    "report_interval": "10",
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
