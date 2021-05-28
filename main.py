import os
import numpy as np
import subprocess
import time
import yaml
from cluster import Cluster

from pprint import pprint
from pqueue import PriorityQueue
from processing import process_timeseries
from tqdm import tqdm

def ssh_cmd(node_name, cmd):
    os.system(f"sudo ssh -tt {node_name} '{cmd}' &")

def run():

    # Get everything we need from config.yaml:
    with open("config.yaml") as conf_file:
        conf = yaml.full_load(conf_file)
    
    name = conf["name"]
    node_names = conf["nodes"]
    defaults = conf["defaults"]
    configs = conf["configs"]
    sql_stmts = conf["sql-statements"]
    session_vars = conf["session-vars"]
    workload_nodes = conf["workload-nodes"]

    # Figure out connection strings. These stay the same during all workloads.
    # We assume all nodes are listening on port 26257.

    # When initializing the workload, we shouldn't be using any s`e`ssion 
    # variables because we might fail initialization. For example, init
    # will fail if we set statement_timeout to a low value.
    init_strings = [f"postgresql://root@{name}:26257?sslmode=disable" for name in node_names]

    # Now attach all session parameters:
    conn_strings = [s + ''.join([f"&{var}={val}" for var, val in session_vars.items()]) for s in init_strings]

    # wrap in single quotes so terminal doesn't scream at us
    init_strings = [f"'{s}'" for s in init_strings]
    conn_strings = [f"'{s}'" for s in conn_strings]
    
    os.system("mkdir -p " + name + "/traces")

    for workload_config in configs:
        cmd = "cockroach workload run kv "
        flags = defaults

        cluster = Cluster(node_names)
        cluster.start()

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

        # Initialize workload
        os.system("cockroach workload init kv " + " ".join(init_strings))

        # Run workload:
        print("running experiment w/ flags:")
        print(", ".join([f"{f}={v}" for f, v in flags.items()]))

        # First run it on all the other workload nodes that aren't this one:
        for node in workload_nodes:
            ssh_cmd(node, cmd)
        # then run it on this one:
        # os.system(cmd)
        # "duration" is actually a string, like "30s"
        duration = flags["duration"] 

        # This means don't run workloads under 1 second!
        time_unit = duration[-1]
        if time_unit == "s":
            duration = int(duration[:-1])
        elif time_unit == "m":
            duration = int(duration[:-1])*60

        print("sleeping")
        time.sleep(duration + 30)
        print("finished sleeping")
        # map of request ids to request start and finish times
        timestamps = []
        
        # for computing unfinished request #
        unfinished = 0
        for node in workload_nodes:
            print(f"grabbing trace files from {node}")
            os.system(f"sudo scp {node}:start.txt .")
            os.system(f"sudo scp {node}:end.txt .")
            os.system(f"sudo ssh -t {node} 'rm start.txt end.txt'")

            requests = {}

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

            for r in requests:
                timestamps.append(r)
            
            os.system("sudo rm start.txt end.txt")
        
        timestamps.sort(key=lambda x: x[0])

        # The "naming convention" (if you can call it that) is pretty dumb 
        # here. We just name the trace after the workload-specific flags 
        # that were used to create it.
        exp_name = "_".join([flag + str(value) for flag, value in workload_config.items()])
        
        # Process the trace into a YAML file:
        timeseries, aggregate_data = process_timeseries(np.array(timestamps), duration)
        print(exp_name + ":")
        pprint(aggregate_data)

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
        
        cluster.kill()
        time.sleep(2)

def main():
    run()

if __name__ == "__main__":
    main()