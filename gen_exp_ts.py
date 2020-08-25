import argparse
import yaml
import time
import queue
import numpy as np
from tqdm import tqdm

import matplotlib
import matplotlib.pyplot as plt
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
    parser = argparse.ArgumentParser(description="Process raw timestamp data into readable YAML.")

    # Arguments:
    parser.add_argument("infile", help="Text file with timestamp data.")
    parser.add_argument("--outfile", help="Name of YAML file to dump data to.")

    # "c" for "clean", I guess
    parser.add_argument("-c", nargs='?', const=True, default=False, help="Remove duplicates from and sort the input file")
    args = parser.parse_args()

    with open(args.infile, "r") as f:

        # Read contents of timestamp file, remove duplicates, sort by start time, convert 
        # everything into ints, and finally put it all into a numpy array:
        timestamps = f.readlines()
        if args.c:
            timestamps = np.array([list_stoi(ts.split()) for ts in sorted_no_dupes(timestamps)])
        else:
            timestamps = np.array([list_stoi(ts.split()) for ts in timestamps])
        
    # Constants:
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

    # Go through every request:
    for i in tqdm(range(N_REQUESTS)):

        start = timestamps[i][0]
        finish = timestamps[i][1]
        latencies[i] = (finish - start)/(10**6)

        # Check the request's start time. Is it outside our current window? If yes, then we should
        # collect all data about this millisecond interval and update the window.
        if start > cur_window:

            # This is awful. I wish there were some way to view the head of the priority queue.
            request = cur_outstanding.get()
            if request < cur_window:
                while request < cur_window:
                    total_finished += 1
                    request = cur_outstanding.get()

            outstanding.append(cur_outstanding.qsize())

            # Hacky, but latency is actually very expensive to compute, especially if we're doing
            # it every millisecond. If you actually *want* it computed every millisecond, just 
            # remove the following line. You will also get unbelievably noisy data.
            if len(outstanding) % 1000 == 0:

                if i > 0:
                    provided_load.append(total_finished)

                    # This should probably be done more intelligently. Alternatively, we can save
                    # the value of "i" that caused the window limit to increase last time, and only
                    # compute from that value to here.
                    p99_latency.append(np.percentile(latencies[i-10000:i], 99))
                    p90_latency.append(np.percentile(latencies[i-10000:i], 90))
                    p50_latency.append(np.percentile(latencies[i-10000:i], 50))

                    # Reset counter for the next window:
                    total_finished = 0

            # Move on to next window:
            cur_window += STEP_NANO

        # Will the request finish in this window? (Probably not, since STEP_NANO is small)
        # If not, we consider it to be outstanding.
        if finish > cur_window:
            cur_outstanding.put(finish)

    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3)
    #fig.suptitle("Timeseries graphs of outstanding requests, throughput, and latency percentiles")

    # TODO: move this into another script.

    # Plot outstanding requests vs time:
    ax1.set(xlabel="time (s)", ylabel="outstanding requests")
    ax1.set_title("outstanding requests vs. time", fontsize=16)
    #out_avg = [sum(outstanding[n-1000:n])/1000 for n in range(len(outstanding))]
    seconds = np.divide(range(len(outstanding)), 1000)
    ax1.plot(seconds, outstanding)

    # Offered vs provided load:
    ax2.set(xlabel="time (s)", ylabel="provided load (avg. requests/sec)")
    ax2.set_title("throughput vs. time", fontsize=16)
    ax2.plot(range(len(provided_load)), provided_load)

    # Latency vs offered load:
    ax3.set(xlabel="time(s)", ylabel="latency (ms)")
    ax3.set_title("latency percentiles vs. time", fontsize=16)

    p99_line, = ax3.plot(range(len(p99_latency)), p99_latency)
    p90_line, = ax3.plot(range(len(p90_latency)), p90_latency)
    p50_line, = ax3.plot(range(len(p50_latency)), p50_latency)

    p99_line.set_label("p99")
    p90_line.set_label("p90")
    p50_line.set_label("p50")

    ax3.legend()

    if args.outfile is not None:
        # TODO: fix this. It's disgusting.
        with open(args.outfile, "w") as yaml_file:
            data = {

                # So I guess it can't take lists of numpy scalars? I had to convert everything to
                # a regular Python float to get it to save properly.
                "outstanding": [float(n) for n in outstanding],
                "seconds": [float(n) for n in seconds],
                #"offered_load": [float(n) for n in offered_load],
                "provided_load": [float(n) for n in provided_load],
                "p99": [float(n) for n in p99_latency],
                "p90": [float(n) for n in p90_latency],
                "p50": [float(n) for n in p50_latency]
            }

            yaml.dump(data, yaml_file)

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()