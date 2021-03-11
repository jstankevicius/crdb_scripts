import os
import numpy as np
import subprocess
import time
import yaml

from pqueue import PriorityQueue
from tqdm import tqdm

def list_stoi(l):
    return [int(s) for s in l]

def process_timeseries(timestamps, duration):

    # Constants:
    N_REQUESTS = len(timestamps)
    START_NANO = timestamps[0][0]
    STEP_NANO = 10**8 # The length of the step window in nanoseconds.

    # Figure out how many windows we're going to collect data for:
    # (we add 1 because this is including the "fake" window that ends at t=0)
    N_WINDOWS = int(duration*(10**9)/STEP_NANO) + 1

    # window_timestamp is the current maximum start timestamp we will consider 
    # before deciding that we should move on to the next window.
    window_timestamp = START_NANO + STEP_NANO
    # Index of the "right side" of the current window.
    window_idx       = 1

    # Because latency is measured for every request, we cannot just measure
    # "total latency" for a single window. We're instead going to collect
    # latencies for all requests that finish inside a window and then take
    # percentiles.
    latencies        = []
    total_latencies  = [] # latencies of EVERY request

    # Timeseries data:
    outstanding      = np.zeros(shape=(N_WINDOWS,))
    throughput       = np.zeros(shape=(N_WINDOWS,))
    p99_latency      = np.zeros(shape=(N_WINDOWS,))
    p90_latency      = np.zeros(shape=(N_WINDOWS,))
    p50_latency      = np.zeros(shape=(N_WINDOWS,))

    # Number of requests finished in the current window:
    total_finished   = 0
    
    # Used to store all requests we consider to be outstanding at a given point 
    # in time.
    cur_outstanding  = PriorityQueue()

    for i in range(N_REQUESTS):

        start = timestamps[i][0]
        finish = timestamps[i][1]
        latency = (finish - start)/(10**6)
        total_latencies.append(latency)

        # Will the request finish in this window? (Probably not, since STEP_NANO 
        # is small) If not, we consider it to be outstanding.
        if finish > window_timestamp:
            cur_outstanding.push(finish)
        else:
            total_finished += 1
            latencies.append(latency) # convert to ms

        # Check the request's start time. Is it outside our current window? If 
        # yes, then we should collect all data about this interval and increment 
        # the window. We should also be writing data out if we run out of 
        # requests.
        if start > window_timestamp or i == N_REQUESTS - 1:
            since = (window_timestamp - START_NANO)/(10**9)
                        
            if cur_outstanding.head() is not None:

                while cur_outstanding.head() <= window_timestamp:
                    total_finished += 1
                    cur_outstanding.pop()

                    if cur_outstanding.empty():
                        break

            # Register stats about this window:
            outstanding[window_idx] = cur_outstanding.size()
            throughput[window_idx] = total_finished/0.1
            
            if len(latencies) == 0:
                latencies = [0]

            # Compute latency percentiles for this window:
            p99_latency[window_idx] = np.percentile(latencies, 99)
            p90_latency[window_idx] = np.percentile(latencies, 90)
            p50_latency[window_idx] = np.percentile(latencies, 50)

            # Clear latency list
            latencies = []

            # Reset counter for the next window:
            total_finished = 0

            # Update window index and current window timestamp
            window_idx += 1
            window_timestamp += STEP_NANO

        if window_idx == N_WINDOWS:
            break


    seconds = [i*STEP_NANO/(10**9) for i in range(N_WINDOWS)]

    aggregate_data = {
        "latency": {
            "p99": float(np.percentile(total_latencies, 99)),
            "p90": float(np.percentile(total_latencies, 90)),
            "p50": float(np.percentile(total_latencies, 50))
        },

        "throughput": {
            "p50": float(np.percentile(throughput, 50)),
            "p10": float(np.percentile(throughput, 10)),
            "p1": float(np.percentile(throughput, 1))
        },

        "outstanding": {
            "p99": float(np.percentile(outstanding, 99)),
            "p90": float(np.percentile(outstanding, 90)),
            "p50": float(np.percentile(outstanding, 50)),
        }
    }

    data = {
        "outstanding": [int(n) for n in outstanding],
        "seconds": [float(n) for n in seconds],
        "throughput": [float(n) for n in throughput],
        "p99": [float(n) for n in p99_latency],
        "p90": [float(n) for n in p90_latency],
        "p50": [float(n) for n in p50_latency]
    }

    return data, aggregate_data

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

def main():
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

            # The "naming convention" (if you can call it that) is pretty dumb 
            # here. We just name the trace after the workload-specific flags 
            # that were used to create it.
            exp_name = "_".join([flag + str(value) for flag, value in workload_config.items()])
            with open("{}/traces/{}.txt".format(name, exp_name), "w") as trace:
                trace.writelines(lines)

            # map of request ids to request start and finish times
            requests = {}
            
            with open("start.txt") as started_file:
                for line in started_file:
                    req_id, ts = line.split()
                    requests[int(req_id)] = [int(ts), np.Inf]
            
            with open("end.txt") as finished_file:
                for line in finished_file:  
                    req_id, ts = line.split()
                    requests[int(req_id)][1] = int(ts)

            exp_start = requests[1][0]
            for req_id, ts_pair in requests.items():
                start, finish = ts_pair
            
            # IDs are ALMOST sorted by time. Unfortunately, concurrency is cruel
            # and sometimes a goroutine ends up determining its ID, pausing, and
            # only taking the timestamp later, after another goroutine. So we 
            # have to sort.
            requests = list(requests.values())
            requests.sort(key=lambda x: x[0])
            requests = np.array(requests)
            
            os.system("rm start.txt end.txt")
            
            # Process the trace into a YAML file:
            timeseries, aggregate_data = process_timeseries(requests, duration)
            with open("{}/{}.yaml".format(name, exp_name), "w") as data_file:

                exp_data = {
                    "name": name,
                    "flags": flags,
                    "aggregate": aggregate_data,
                    "ts": timeseries
                }
                yaml.dump(exp_data, data_file, default_flow_style=None, width=80)

            kill_cluster(n_nodes)
            time.sleep(2)


if __name__ == "__main__":
    main()