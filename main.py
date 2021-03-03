import yaml
import os
import numpy as np
import time
import numpy as np
from pqueue import PriorityQueue
from tqdm import tqdm

def list_stoi(l):
    return [int(s) for s in l]

# We shouldn't even need to do this. 
def trim(timestamps, ignore_start=0, ignore_end=120):
    print("trimming with parameters", ignore_start, ignore_end)
    print("trace starts", len(timestamps), "lines long")

    start_timestamp_index = 0
    end_timestamp_index = 0
    experiment_start = timestamps[0][0]

    for i in range(0, len(timestamps)):
        if (timestamps[i][0] - experiment_start)/(10**9) > ignore_start:
            start_timestamp_index = i
            break

    for i in range(len(timestamps) - 1, 0, -1):
        if (timestamps[i][1] - experiment_start)/(10**9) >= ignore_end:
            end_timestamp_index = i
            break

    timestamps = timestamps[start_timestamp_index:end_timestamp_index]
    print("trace ends", len(timestamps), "lines long")
    return timestamps

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

    for i in tqdm(range(N_REQUESTS)):

        start = timestamps[i][0]
        finish = timestamps[i][1]
        latency = (finish - start)/(10**6)
        total_latencies.append(latency)

        # Check the request's start time. Is it outside our current window? If 
        # yes, then we should collect all data about this interval and increment 
        # the window.
        if start > window_timestamp:
			
            if cur_outstanding.head() is not None:
                while cur_outstanding.head() < window_timestamp:
                    total_finished += 1
                    cur_outstanding.pop()
                    if cur_outstanding.empty():
                        break

            # Register stats about this window:
            outstanding[window_idx] = cur_outstanding.size()
            throughput[window_idx] = total_finished/0.1
            
            # Reset counter for the next window:
            total_finished = 0
            if len(latencies) == 0:
                latencies = [0]

            # Compute latency percentiles for this window:
            p99_latency[window_idx] = np.percentile(latencies, 99)
            p90_latency[window_idx] = np.percentile(latencies, 90)
            p50_latency[window_idx] = np.percentile(latencies, 50)


            # Clear latency list
            latencies = []

            # Update window index and current window timestamp
            window_idx += 1
            window_timestamp += STEP_NANO

            if window_idx == N_WINDOWS:
                break

        # Will the request finish in this window? (Probably not, since STEP_NANO 
        # is small) If not, we consider it to be outstanding.
        if finish > window_timestamp:
            cur_outstanding.push(finish)
        else:
            total_finished += 1
            latencies.append(latency) # convert to ms


    seconds = [i/(10**9/STEP_NANO) for i in range(N_WINDOWS)]

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
    time.sleep(2)

def kill_cluster(n_nodes):
    for i in range(n_nodes):
        node_name = "node-"+str(i)
        ssh_cmd(node_name, "pkill -9 cockroach")
        ssh_cmd(node_name, "pkill -9 cockroach")
        ssh_cmd(node_name, "sudo rm -r /mnt/sda4/node")

def main():
    with open("config.yaml") as conf_file:

        conf = yaml.full_load(conf_file)
        name = conf["name"]
        n_nodes = conf["nodes"]

        defaults = conf["defaults"]

        # List of dictionaries containing workload-specific flags
        configs = conf["configs"]

        # SQL statements that should be executed against the cluster:
        sql_stmts = conf["sql-statements"]

        # Figure out connection strings. These stay the same during all workloads.
        # This assumes all nodes are named node-0 to node-(n_nodes-1), and that the
        # listen-addr flag was set to the node's 26257 port.
        conn_strings = ["'postgresql://root@node-" + str(i) + ":26257?sslmode=disable'" for i in range(n_nodes)]
        
        os.system("mkdir -p " + name + "/traces")

        for workload_config in configs:
            cmd = "cockroach workload run kv "
            flags = defaults

            start_cluster(n_nodes)

            for stmt in sql_stmts:
                print(f"cockroach sql --insecure --host=node-0:26257 --execute '{stmt}'")
                os.system(f"cockroach sql --insecure --host=node-0:26257 --execute '{stmt};'")

            # Overwrite default flags with flags from workload config:
            for key in workload_config.keys():
                flags[key] = workload_config[key]

            for flag, value in flags.items():
                print(flag, value)
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
            os.system("cockroach workload init kv " + " ".join(conn_strings))
            os.system(cmd)
            
            # TODO(jstankevicius): The reason we get more timestamps than we 
            # want is because the histogram registry gets ticked one last time
            # at the end of the experiment, emitting some more timestamps that
            # came back.

            with open("requests.txt", "r") as infile:
                lines = infile.readlines()
                timestamps = np.array([list_stoi(line.split()) for line in sorted(lines)])
                
                duration = flags["duration"] # "duration" is actually a string, like "30s"

                # This means don't run workloads under 1 second!
                time_unit = duration[-1]
                if time_unit == "s":
                    duration = int(duration[:-1])
                elif time_unit == "m":
                    duration = int(duration[:-1])*60

                timestamps = trim(timestamps, 0, duration)

            # The "naming convention" (if you can call it that) is pretty dumb here. We just name the trace
            # after the workload-specific flags that were used to create it.
            exp_name = "_".join([flag + str(value) for flag, value in workload_config.items()])
            with open("{}/traces/{}.txt".format(name, exp_name), "w") as trace:
                trace.writelines(lines)
            
            os.system("rm requests.txt")
            
            # Process the trace into a YAML file:
            timeseries, aggregate_data = process_timeseries(timestamps, duration)
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