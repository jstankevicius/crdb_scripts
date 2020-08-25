import argparse
import yaml
import time
import queue
import numpy as np
import os
import matplotlib
import matplotlib.pyplot as plt

from os.path import isfile, join
from tqdm import tqdm

matplotlib.rcParams.update({"font.size": 16})
plt.rcParams["figure.figsize"] = (12, 6)

def list_stoi(l):
    """Converts every entry in a list of strings into an integer."""
    return [int(s) for s in l]

def sorted_no_dupes(l):
    """Takes a list, converts it into a set, converts it back to a list, and sorts it. Useful
    since timestamp data 1. is not sorted by start time and 2. contains many duplicate timestamps."""
    return sorted(list(set(l)))

def main():
    parser = argparse.ArgumentParser()

    # Arguments:
    parser.add_argument("dir", help="directory with timestamp files")
    #parser.add_argument("outfile", help="name of YAML file to which data will be written")
    #parser.add_argument("--duration", help="duration of the experiment in seconds")

    args = parser.parse_args()
    
    p99_latency_list = []
    p90_latency_list = []
    p50_latency_list = []
    utilization_list = []
    provided_load_list = []

    p99_or_list = []
    p90_or_list = []
    p50_or_list = []


    files = [f for f in os.listdir(args.dir) if isfile(join(args.dir, f))]
    for fname in files:
        with open(args.dir + "/" + fname, "r") as f:

            # Read contents of timestamp file, remove duplicates, sort by start time, convert 
            # everything into ints, and finally put it all into a numpy array:
            timestamps = np.array([list_stoi(ts.split()) for ts in sorted_no_dupes(f.readlines())])

        # Go through every request:
        N_REQUESTS = len(timestamps)
        
        # We'll consider the "start" of the experiment to be when the first request was sent out.
        START_NANO = timestamps[0][0]

        # The length of the step window in nanoseconds. 10^6 nanoseconds is a millisecond.
        STEP_NANO = 10**6

        # cur_window is the current maximum start timestamp we will consider before deciding that
        # we should move on to the next millisecond interval.
        cur_window = START_NANO + STEP_NANO

        # Outstanding request timeseries data:
        outstanding      = []

        # Used to store all requests we consider to be outstanding at a given point in time.
        cur_outstanding  = queue.PriorityQueue()

        for i in tqdm(range(N_REQUESTS)):

            start = timestamps[i][0]
            finish = timestamps[i][1]

            # Check the request's start time. Is it outside our current window? If yes, then we should
            # collect all data about this millisecond interval and update the window.
            if start > cur_window:

                # This is awful. I wish there were some way to view the head of the priority queue.
                request = cur_outstanding.get()
                if request < cur_window:
                    while request < cur_window:
                        request = cur_outstanding.get()
                #else:
                #    cur_outstanding.put(request)

                outstanding.append(cur_outstanding.qsize())

                # Move on to next window:
                cur_window += STEP_NANO

            # Will the request finish in this window? (Probably not, since STEP_NANO is small)
            # If not, we consider it to be outstanding.
            if finish > cur_window:
                cur_outstanding.put(finish)

        latencies = np.subtract(timestamps[..., 1], timestamps[..., 0]) # subtract start from finish
        latencies = np.divide(latencies, 10**6) # convert from ns to ms

        p99_latency = np.percentile(latencies, 99)
        p90_latency = np.percentile(latencies, 90)
        p50_latency = np.percentile(latencies, 50)

        p99_or_list.append(np.percentile(outstanding, 99))
        p90_or_list.append(np.percentile(outstanding, 90))
        p50_or_list.append(np.percentile(outstanding, 50))

        avg_latency = np.average(latencies) # milliseconds per request

        arrival_rate = [timestamps[i][0] - timestamps[i - 1][0] for i in range(1, len(timestamps))]
        arrival_rate = np.divide(arrival_rate, 10**6)
        arrival_rate = np.average(arrival_rate)

        provided_load = len(timestamps)/(timestamps[-1][1] - timestamps[0][0])*(10**9)

        utilization = avg_latency / arrival_rate
        
        #print(fname, utilization, p99_latency, p90_latency, p50_latency, provided_load)

        p99_latency_list.append(p99_latency)
        p90_latency_list.append(p90_latency)
        p50_latency_list.append(p50_latency)

        utilization_list.append(utilization)
        provided_load_list.append(provided_load)


    c = sorted(zip(utilization_list, provided_load_list, p99_latency_list, p90_latency_list, p50_latency_list, p99_or_list, p90_or_list, p50_or_list), key=lambda t: t[0])
    utilization_list, provided_load_list, p99_latency_list, p90_latency_list, p50_latency_list, p99_or_list, p90_or_list, p50_or_list = zip(*c)

    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3)

    fig.suptitle("Throughput & latency of kv0 workloads with varied concurrency")

    ax1.plot(utilization_list, provided_load_list)
    ax1.set_title("observed throughput vs. offered load", fontsize=14)
    ax1.set(xlabel="offered load (erlangs)", ylabel="throughput (avg. requests/sec)")

    p99_line, = ax2.plot(utilization_list, p99_latency_list)
    p90_line, = ax2.plot(utilization_list, p90_latency_list)
    p50_line, = ax2.plot(utilization_list, p50_latency_list)

    p99_line.set_label("p99")
    p90_line.set_label("p90")
    p50_line.set_label("p50")
    ax2.legend()

    ax2.set(xlabel="offered load (erlangs)", ylabel="latency (ms)")
    ax2.set_title("latency percentiles vs. offered load", fontsize=14)

    p99_or_line, = ax3.plot(utilization_list, p99_or_list)
    p90_or_line, = ax3.plot(utilization_list, p90_or_list)
    p50_or_line, = ax3.plot(utilization_list, p50_or_list)

    p99_or_line.set_label("p99")
    p90_or_line.set_label("p90")
    p50_or_line.set_label("p50")

    ax3.legend()
    ax3.set(xlabel="offered load (erlangs)", ylabel="number of outstanding requests")
    ax3.set_title("number of outstanding requests vs. offered load", fontsize=14)

    plt.show()      
        
if __name__ == "__main__":
    main()