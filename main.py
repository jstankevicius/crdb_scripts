import yaml
import os
import numpy as np
import time
import queue
import numpy as np
from tqdm import tqdm

def list_stoi(l):
    return [int(s) for s in l]

def trim(lines, ignore_start=0, ignore_end=120):
    print("Trimming with parameters", ignore_start, ignore_end)
    print("Trace starts", len(lines), "lines long")

    # 'lines' is a sorted list of strings where reach string is a pair of timestamps,
    # one for when the request started and one for when the request ended. 
    timestamp_ints = np.array([list_stoi(line.split()) for line in lines])
    start_timestamp_index = 0
    end_timestamp_index = 0

    experiment_start = timestamp_ints[0][0]
    experiment_end = timestamp_ints[-1][1]

    for i in range(0, len(lines)):
        if (timestamp_ints[i][0] - experiment_start)/(10**9) > ignore_start:
            start_timestamp_index = i
            break

    for i in range(0, len(lines)):
        if (timestamp_ints[i][0] - experiment_start)/(10**9) > ignore_end:
            end_timestamp_index = i
            break

    lines = lines[start_timestamp_index:end_timestamp_index]
    print("Trace ends", len(lines), "lines long")
    return lines

def process_timeseries(timestamps):

    # TODO: throughput should probably be a trailing window.
    timestamps = np.array([list_stoi(ts.split()) for ts in timestamps])

    # Constants:
    N_REQUESTS = len(timestamps)
    START_NANO = timestamps[0][0]
    STEP_NANO = 10**8 # The length of the step window in nanoseconds.

    # cur_window is the current maximum start timestamp we will consider before deciding that
    # we should move on to the next interval.
    cur_window = START_NANO + STEP_NANO

    # Outstanding request timeseries data:
    outstanding      = []

    # Load timeseries data:
    provided_load    = []

    # Latency timeseries data:
    # Internally, if we're calling  numpy.percentile() on a list, numpy will take it and convert
    # it into a numpy array, which takes a bit of time. It's easier to just fill in values.
    latencies        = np.zeros(shape=(N_REQUESTS,))
    p99_latency      = []
    p90_latency      = []
    p50_latency      = []

    # Number of requests finished in the current window:
    total_finished   = 0
    
    # Used to store all requests we consider to be outstanding at a given point in time.
    cur_outstanding  = queue.PriorityQueue()

    # Index of the last request that caused the window to increase.
    last_window_increase = 0

    for i in tqdm(range(N_REQUESTS)):

        start = timestamps[i][0]
        finish = timestamps[i][1]
        latencies[i] = (finish - start)/(10**6) # convert to ms

        # Check the request's start time. Is it outside our current window? If yes, then we should
        # collect all data about this interval and increment the window.
        if start > cur_window:
			
            # This is awful. I wish there were some way to view the head of the priority queue.
            # Maybe write a custom class?
            try:
                request = cur_outstanding.get(block=False)
                while request < cur_window:
                    total_finished += 1
                    request = cur_outstanding.get(block=False)
            except queue.Empty:
                pass

            outstanding.append(cur_outstanding.qsize())
            provided_load.append(total_finished/0.1)
            
            # Reset counter for the next window:
            total_finished = 0

            # Compute latency percentiles for this window:        
            p99_latency.append(np.percentile(latencies[last_window_increase:i], 99))
            p90_latency.append(np.percentile(latencies[last_window_increase:i], 90))
            p50_latency.append(np.percentile(latencies[last_window_increase:i], 50))

            last_window_increase = i
            cur_window += STEP_NANO

        # Will the request finish in this window? (Probably not, since STEP_NANO is small)
        # If not, we consider it to be outstanding.
        if finish > cur_window:
            cur_outstanding.put(finish)
        else:
            total_finished += 1

    seconds = [i/(10**9/STEP_NANO) for i in range(len(outstanding))]

    aggregate_data = {
        "latency": {
            "p99": float(np.percentile(latencies, 99)),
            "p90": float(np.percentile(latencies, 90)),
            "p50": float(np.percentile(latencies, 50))
        },

        "throughput": {
            "p50": float(np.percentile(provided_load, 50)),
            "p10": float(np.percentile(provided_load, 10)),
            "p1": float(np.percentile(provided_load, 1))
        },

        "outstanding": {
            "p99": float(np.percentile(outstanding, 99)),
            "p90": float(np.percentile(outstanding, 90)),
            "p50": float(np.percentile(outstanding, 50)),
        }

    }

    data = {
        # I COULD just save everything as a numpy scalar/int, but that would be hard to read.
        "outstanding": [int(n) for n in outstanding],
        "seconds": [float(n) for n in seconds],
        "throughput": [float(n) for n in provided_load],
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

    # Set rate limit
    #ssh_cmd("node-0", "cockroach sql --insecure --host=node-0:26257 --execute 'SET CLUSTER SETTING sql.rate_limiter.limit=65000'")
    #ssh_cmd("node-0", "cockroach sql --insecure --host=node-0:26257 --execute 'SET CLUSTER SETTING sql.rate_limiter.application_name=kv'")
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

        # Figure out connection strings. These stay the same during all workloads.
        # This assumes all nodes are named node-0 to node-(n_nodes-1), and that the
        # listen-addr flag was set to the node's 26257 port.
        conn_strings = ["'postgresql://root@node-" + str(i) + ":26257?sslmode=disable'" for i in range(n_nodes)]
        
        os.system("mkdir -p " + name + "/traces")

        for workload_config in configs:
            cmd = "./cockroach workload run kv "
            flags = defaults

            start_cluster(n_nodes)

            # Overwrite default flags with flags from workload config:
            for key in workload_config.keys():
                if key in flags.keys():
                    flags[key] = workload_config[key]

            for flag, value in flags.items():
                cmd += "--" + flag

                # Some flags are booleans (ex: --drop and --tolerate-errors), so those don't have
                # values associated with them. Numeric values should also be converted to strings
                # before we add them to the command.
                if type(value) is bool:
                    pass
                elif type(value) is int:
                    cmd += "=" + str(value)
                else:
                    cmd += "=" + value
                
                cmd += " "

            cmd += " ".join(conn_strings)

            # Run workload:
            print("RUNNING EXPERIMENT")
            print("FLAGS:")
            for flag, value in flags.items():
                print("\t" + flag, value)
            
            os.system(cmd)

            # The workload generator produces a file called "requests.txt" in the directory from which 
            # it was called. This file contains many duplicate timestamp lines that are also not sorted
            # by start time (as we would like). It is also frequently the case that the timestamps indicate
            # an experiment longer than was specified by --duration, so we trim the trace.

            # (please do not try this on your home computer)
            with open("requests.txt", "r") as infile:
                lines = sorted(list(set(infile.readlines())))
                duration = flags["duration"] # "duration" is actually a string, like "30s"

                # This means don't run workloads under 1 second!
                time_unit = duration[-1]
                if time_unit == "s":
                    duration = int(duration[:-1])
                elif time_unit == "m":
                    duration = int(duration[:-1])*60

                lines = trim(lines, 0, duration)

            # The "naming convention" (if you can call it that) is pretty dumb here. We just name the trace
            # after the workload-specific flags that were used to create it.
            exp_name = "_".join([flag + str(value) for flag, value in workload_config.items()])
            with open("{}/traces/{}.txt".format(name, exp_name), "w") as trace:
                trace.writelines(lines)
            
            os.system("rm requests.txt")
            
            # Process the trace into a YAML file:
            timeseries, aggregate_data = process_timeseries(lines)
            with open("{}/{}.yaml".format(name, exp_name), "w") as data_file:

                exp_data = {
                    "flags": flags,
                    "aggregate": aggregate_data,
                    "ts": timeseries
                }
                yaml.dump(exp_data, data_file, default_flow_style=None, width=80)

            # Wait 30 seconds for things to quiet down.
            time.sleep(5)
            kill_cluster(n_nodes)
            time.sleep(30)


if __name__ == "__main__":
    main()