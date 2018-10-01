#!/usr/bin/env python

import sys
from launch_utils import launch_util

local_debug = True if len(sys.argv) >= 2 and sys.argv[1] == "local" else False

hostfile = "config/localnodes" if local_debug else "config/clusternodes"
progfile = ("cmake-build-debug" if local_debug else "debug") + "/BasicExample"

params = {
}

env_params = (
    "GLOG_logtostderr=true "
    "GLOG_v=-1 "
    "GLOG_minloglevel=0 "
)

# use `python scripts/launch.py` to launch the distributed programs
# use `python scripts/launch.py kill` to kill the distributed programs
launch_util(progfile, hostfile, env_params, params, sys.argv)
