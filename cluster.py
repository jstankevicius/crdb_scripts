import subprocess

def ssh_cmd(node, cmd, check_rc=True):
    subprocess.run(
        ["sudo", "ssh", "-t", f"root@{node.get_name()}", f"{cmd}"], 
        stdout=subprocess.DEVNULL,
        check=check_rc
    )

class Node:

    def __init__(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def kill(self):

        # hard shutdown, then wipe disk
        ssh_cmd(self, "pkill -9 cockroach ; pkill -9 cockroach ; sudo rm -r /mnt/sda4/node", False)

    def start(self, nodes):
        print(f"starting node {self.name}")
        # construct command
        join_str = ",".join([f"{node.get_name()}:26257" for node in nodes])
        cmd = "cockroach start --insecure"
        cmd += " --store=/mnt/sda4/node" # can change this
        cmd += " --http-addr=localhost:8080"
        cmd += f" --listen-addr={self.name}:26257"
        cmd += " --join=" + join_str
        cmd += " --background"
        
        # ssh into node and run command
        ssh_cmd(self, cmd)

class Cluster:

    def __init__(self, node_names):
        self.nodes = [Node(name) for name in node_names]

    def start(self):

        for node in self.nodes:
            node.start(self.nodes)

        print(self.nodes[0])
        print(self.nodes[0].get_name())
        result = ssh_cmd(self.nodes[0], f"cockroach init --insecure --host={self.nodes[0].get_name()}:26257")

    def kill(self):
        for node in self.nodes:
            node.kill()

    def get_nodes(self):
        return self.nodes


        
