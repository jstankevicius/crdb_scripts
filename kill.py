from cluster import Cluster
import yaml
import os

if __name__ == "__main__":
    with open("config.yaml") as conf_file:
        conf = yaml.full_load(conf_file)
    c = Cluster(conf["nodes"] + conf["workload-nodes"])

    # remove traces
    for node in c.get_nodes():
        if (node.get_name() in conf["workload-nodes"]):
            os.system(f"sudo ssh -t {node.get_name()} 'rm start.txt end.txt'")
    os.system("sudo rm start.txt end.txt")
    c.kill()
