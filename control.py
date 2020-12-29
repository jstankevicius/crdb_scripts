import os
import time

def ssh_cmd(node_name, cmd):
    print('sudo ssh -t root@{} "{}"'.format(node_name, cmd))
    os.system('sudo ssh -t root@{} "{}"'.format(node_name, cmd))

def start_cluster(n_nodes):
    join_str = ",".join(["node-{}:26257".format(i) for i in range(n_nodes)])
    for i in range(n_nodes):
        cmd = "cockroach start --insecure"
        cmd += " --store=/mnt/sda4/node" # can change this
        cmd += " --http-addr=localhost:8080"
        cmd += " --listen-addr=node-{}:26257".format(i)
        cmd += " --join=" + join_str
        cmd += " --background"
        ssh_cmd("node-" + str(i), cmd)
    
    ssh_cmd("node-0", "cockroach init --insecure --host=node-0:26257")

    # Set rate limit
    ssh_cmd("node-0", "cockroach sql --insecure --host=node-0:26257 --execute 'SET CLUSTER SETTING sql.rate_limiter.limit = 15000'")
    time.sleep(2)

def kill_cluster(n_nodes):
    for i in range(n_nodes):
        node_name = "node-"+str(i)
        ssh_cmd(node_name, "pkill -9 cockroach")
        ssh_cmd(node_name, "pkill -9 cockroach")
        ssh_cmd(node_name, "sudo rm -r /mnt/sda4/node")


kill_cluster(4)