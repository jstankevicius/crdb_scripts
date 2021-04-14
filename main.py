import os
import numpy as np
import subprocess
import time
import yaml

from pqueue import PriorityQueue
from processing import process_timeseries
from tqdm import tqdm

def ssh_cmd(node_name, cmd):
    result = subprocess.run(
        ["sudo", "ssh", "-t", f"root@{node_name}", f"{cmd}"], 
        stdout=subprocess.DEVNULL
    )

    return result.returncode

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
    
    print("initializing cluster...")
    result = ssh_cmd("node-0", "cockroach init --insecure --host=node-0:26257")
    time.sleep(2)

def kill_cluster(n_nodes):
    for i in range(n_nodes):
        node_name = "node-"+str(i)
        ssh_cmd(node_name, "pkill -9 cockroach ; pkill -9 cockroach ; sudo rm -r /mnt/sda4/node")

def run():
    with open("config.yaml") as conf_file:

        conf = yaml.full_load(conf_file)
        name = conf["name"]
        n_nodes = len(conf["nodes"])

        defaults = conf["defaults"]

        # List of dictionaries containing workload-specific flags
        configs = conf["configs"]

        # SQL statements that should be executed against the cluster:
        sql_stmts = conf["sql-statements"]

        # Session variables.
        session_vars = conf["session-vars"]

        # Figure out connection strings. These stay the same during all workloads.
        # This assumes all nodes are named node-0 to node-(n_nodes-1), and that the
        # listen-addr flag was set to the node's 26257 port. 

        # When initializing the workload, we shouldn't be using any session 
        # variables because we might fail initialization. For example, init
        # will fail if we set statement_timeout to a low value.
        init_strings = ["postgresql://root@node-" + str(i) + ":26257?sslmode=disable" for i in range(n_nodes)]

        # Now attach all session parameters:
        conn_strings = [s + ''.join([f"&{var}={val}" for var, val in session_vars.items()]) for s in init_strings]

        # wrap in single quotes so terminal doesn't scream at us
        init_strings = [f"'{s}'" for s in init_strings]
        conn_strings = [f"'{s}'" for s in conn_strings]
        
        os.system("mkdir -p " + name + "/traces")

        for workload_config in configs:
            cmd = "cockroach workload run kv "
            flags = defaults

            start_cluster(n_nodes)

            for stmt in sql_stmts:
                os.system(f"cockroach sql --insecure --host=node-0:26257 --execute '{stmt}'")

            # Overwrite default flags with flags from workload config:
            for key in workload_config.keys():
                flags[key] = workload_config[key]

            for flag, value in flags.items():
                if type(value) is bool:
                    if value:
                        cmd += f"--{flag}"
                    else:
                        pass
                elif type(value) is int:
                    cmd += f"--{flag}={str(value)}"
                else:
                    cmd += f"--{flag}={value}"
                
                cmd += " "

            cmd += " ".join(conn_strings)

            # Run workload:
            print("running experiment w/ flags:")
            print(", ".join([f"{f}={v}" for f, v in flags.items()]))
            os.system("cockroach workload init kv " + " ".join(init_strings))
            os.system(cmd)
            
            # "duration" is actually a string, like "30s"
            duration = flags["duration"] 

            # This means don't run workloads under 1 second!
            time_unit = duration[-1]
            if time_unit == "s":
                duration = int(duration[:-1])
            elif time_unit == "m":
                duration = int(duration[:-1])*60

            # map of request ids to request start and finish times
            requests = {}
            
            # for computing unfinished request #
            unfinished = 0

            with open("start.txt") as started_file:
                for line in started_file:
                    req_id, ts = [int(s) for s in line.split()]
                    requests[req_id] = [ts, np.Inf]
                    unfinished += 1
            
            with open("end.txt") as finished_file:
                for line in finished_file:  
                    req_id, ts = [int(s) for s in line.split()]
                    requests[req_id][1] = ts
                    unfinished -= 1

            print(f"{unfinished} requests never finished.")

            # IDs are ALMOST sorted by time. Unfortunately, concurrency is cruel
            # and sometimes a goroutine ends up determining its ID, pausing, and
            # only taking the timestamp later, after another goroutine. So we 
            # have to sort.
            requests = list(requests.values())
            requests.sort(key=lambda x: x[0])            

            os.system("rm start.txt end.txt")

            # The "naming convention" (if you can call it that) is pretty dumb 
            # here. We just name the trace after the workload-specific flags 
            # that were used to create it.
            exp_name = "_".join([flag + str(value) for flag, value in workload_config.items()])
            
            # Process the trace into a YAML file:
            timeseries, aggregate_data = process_timeseries(np.array(requests), duration)
            with open("{}/{}.yaml".format(name, exp_name), "w") as data_file:

                exp_data = {
                    "name": name,
                    "flags": flags,
                    "aggregate": aggregate_data,
                    "ts": timeseries
                }
                yaml.dump(exp_data, data_file, default_flow_style=None, width=80)

            with open("{}/traces/{}.txt".format(name, exp_name), "w") as trace:
                # Convert everything back to a string and write out to file:
                lines = []
                for start, finish in requests:
                    lines.append(f"{start}\t{finish}\n")

                trace.writelines(lines)
            
            # retrieve logs
            os.system(f"mkdir -p {name}/logs")

            kill_cluster(n_nodes)
            time.sleep(2)

def main():
    try:
        run()
    except:
        print("An exception has occurred")
        os.system("rm start.txt end.txt")
        kill_cluster(4) # some more hardcoding


if __name__ == "__main__":
    main()