#!/usr/bin/env python

import sys
import os
import os.path
from os.path import dirname, join
from launch_utils import launch_util
from launch_utils import relaunch_nodes

# [Local]
# python logistic_regression.py local
#
# [Cluster]
# python logistic_regression.py

local_debug = True if len(sys.argv) >= 2 and (sys.argv[1] == "local" or sys.argv[1] == "relocal") else False
relaunch = True if len(sys.argv) >= 2 and (sys.argv[1] == "relaunch" or sys.argv[1] == "relocal") else False

failed_node_id = -1
if relaunch:
    failed_node_id = sys.argv[2]

hostfile = "config/localnodes" if local_debug else "config/clusternodes"
progfile = ("cmake-build-debug" if local_debug else "debug") + "/LRExample"

script_path = os.path.realpath(__file__)
proj_dir = dirname(dirname(script_path))

if local_debug:
    relaunch_cmd = "\'python " +  proj_dir + "/scripts/logistic_regression.py relocal \'"
else:
    relaunch_cmd = "\'python " +  proj_dir + "/scripts/logistic_regression.py relaunch \'"
[]
params = {
    "hdfs_namenode": "localhost" if local_debug else "proj10",
    "hdfs_namenode_port": 9000,
    "assigner_master_port": 18011,
    "input": "hdfs:///a2a" if local_debug else "hdfs:///datasets/classification/webspam",
    "kStaleness": 0,
    "kSpeculation": 5,
    "kModelType": "SSP",  # {ASP/SSP/BSP/SparseSSP}
    "kSparseSSPRecorderType": "Vector",  # {Vector/Map}
    "num_dims": 123 if local_debug else 16609143,
    "batch_size": 1,
    "num_workers_per_node": 2,
    "num_servers_per_node": 1,
    "num_local_load_thread": 2 if local_debug else 100,
    "num_iters": 1000,
    "alpha": 0.1,  # learning rate
    "with_injected_straggler": 1,  # {0/1}
    "kStorageType": "Vector",  # {Vector/Map}
    "checkpoint_toggle": True,
    "use_weight_file": False,
    "init_dump": True if local_debug else False,
    "weight_file_prefix": "",
    "heartbeat_interval": 10 if local_debug else 15, # join(proj_dir, "local/dump_")
    "checkpoint_file_prefix": "hdfs://localhost:9000/dump/dump_" if local_debug else "hdfs://proj10:9000/ybai/dump_",
    "checkpoint_raw_prefix": "hdfs:///dump/dump_" if local_debug else "hdfs:///ybai/dump_",
    "relaunch_cmd": relaunch_cmd, # hdfs://localhost:9000/dump/dump_
    "report_prefix": join(proj_dir, "local/report_lr_webspam.txt"),
    "report_interval": 5,
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

if relaunch is False:
    launch_util(progfile, hostfile, env_params, params, sys.argv)
else:
    relaunch_nodes(progfile, hostfile, env_params, params, failed_node_id)
