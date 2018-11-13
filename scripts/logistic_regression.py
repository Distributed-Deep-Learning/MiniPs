#!/usr/bin/env python

import sys
import os
import os.path
from os.path import dirname, join
from launch_utils import launch_util

# [Local]
# python logistic_regression.py local
#
# [Cluster]
# python logistic_regression.py

local_debug = True if len(sys.argv) >= 2 and sys.argv[1] == "local" else False

hostfile = "config/localnodes" if local_debug else "config/clusternodes"
progfile = ("cmake-build-debug" if local_debug else "debug") + "/LRExample"

script_path = os.path.realpath(__file__)
proj_dir = dirname(dirname(script_path))

params = {
    "hdfs_namenode": "localhost" if local_debug else "proj10",
    "hdfs_namenode_port": 9000,
    "assigner_master_port": 19201,
    "input": "hdfs:///a2a" if local_debug else "hdfs:///jasper/kdd12",
    "kStaleness": 0,
    "kSpeculation": 5,
    "kModelType": "SSP",  # {ASP/SSP/BSP/SparseSSP}
    "kSparseSSPRecorderType": "Vector",  # {Vector/Map}
    "num_dims": 54686452,
    "batch_size": 1,
    "num_workers_per_node": 2,
    "num_servers_per_node": 1,
    "num_iters": 1000,
    "alpha": 0.1,  # learning rate
    "with_injected_straggler": 1,  # {0/1}
    "kStorageType": "Vector",  # {Vector/Map}
    "use_weight_file": False,
    "weight_file_prefix": "",
    "heartbeat_interval": 10,
    "checkpoint_file_prefix": join(proj_dir, "local/dump_") if local_debug else "hdfs://proj10:9000/ybai/dump_",
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
