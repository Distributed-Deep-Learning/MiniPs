import sys
import os
import os.path
from os.path import dirname, join

local_debug = False

ssh_cmd = (
    "ssh "
    "-o StrictHostKeyChecking=no "
    "-o UserKnownHostsFile=/dev/null "
)

app_dir = "/home/yongbiaoai/projects/MiniPs/scripts/yarn"
proj_dir = dirname(dirname(app_dir))
prog_path = join(proj_dir, ("cmake-build-debug" if local_debug else "debug") + "/LRExample")

client_id = sys.argv[1]
hostfile = sys.argv[2]

if local_debug:
    relaunch_cmd = "empty"
else:
    relaunch_cmd = "empty"

params = {
    "config_file": hostfile,
    "hdfs_namenode": "localhost" if local_debug else "instance-1",
    "hdfs_namenode_port": 9000,
    "assigner_master_port": 18011,
    "input": "hdfs:///a2a" if local_debug else "hdfs:///real-sim",
    "kStaleness": 0,
    "kSpeculation": 5,
    "kModelType": "SSP",  # {ASP/SSP/BSP/SparseSSP}
    "kSparseSSPRecorderType": "Vector",  # {Vector/Map}
    "num_dims": 123 if local_debug else 20958,
    "batch_size": 1,
    "num_workers_per_node": 2,
    "num_servers_per_node": 1,
    "num_local_load_thread": 2 if local_debug else 2,
    "num_iters": 100,
    "alpha": 0.1,  # learning rate
    "with_injected_straggler": 1,  # {0/1}
    "kStorageType": "Vector",  # {Vector/Map}
    "checkpoint_toggle": False,
    "use_weight_file": False,
    "init_dump": True if local_debug else False,
    "weight_file_prefix": "",
    "heartbeat_interval": -1 if local_debug else -1, # join(proj_dir, "local/dump_")
    "checkpoint_file_prefix": "hdfs://localhost:9000/dump/dump_" if local_debug else "hdfs://instance-1:9000/dump/dump_",
    "checkpoint_raw_prefix": "hdfs://localhost:9000/dump/dump_" if local_debug else "hdfs://instance-1:9000/dump/dump_",
    "relaunch_cmd": relaunch_cmd, # hdfs://localhost:9000/dump/dump_
    "report_prefix": join(proj_dir, "local/report_lr_webspam.txt"),
    "report_interval": -1,
}

env_params = (
    "GLOG_logtostderr=true "
    "GLOG_v=-1 "
    "GLOG_minloglevel=0 "
)

if (local_debug is False):
    env_params += "LIBHDFS3_CONF=/usr/local/hadoop/etc/hadoop/hdfs-site.xml"

# clear_cmd = "ls " + hostfile + " > /dev/null; ls " + prog_path + " > /dev/null; "

host = ""
port = ""
with open(hostfile.split("/")[-1], "r") as f:
    hostlist = []
    hostlines = f.read().splitlines()
    for line in hostlines:
        infos = line.split(":")
        if infos[0] == client_id:
            host = infos[1]
            port = infos[2]

print "node_id:%s, host:%s, port:%s" %(client_id, host, port)
cmd = ssh_cmd + host + " "  # Start ssh command
cmd += "\""  # Remote command starts
# Command to run program
cmd += env_params + " " + prog_path
cmd += " --my_id="+client_id
cmd += "".join([" --%s=%s" % (k,v) for k,v in params.items()])
# 
# cmd += "\""  # Remote Command ends
# cmd += " &"
print cmd
os.system(cmd)
