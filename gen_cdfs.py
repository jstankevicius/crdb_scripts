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
    parser.add_argument("infile", help="text file with timestamp data")
    #parser.add_argument("outfile", help="name of YAML file to which data will be written")
    #parser.add_argument("--duration", help="duration of the experiment in seconds")

    args = parser.parse_args()

    # TODO: make this optional (you ca#pin clean the file up remotely before processing it here)
    with open(args.infile, "r") as f:

        # Read contents of timestamp file, remove duplicates, sort by start time, convert 
        # everything into ints, and finally put it all into a numpy array:
        timestamps = np.array([list_stoi(ts.split()) for ts in sorted_no_dupes(f.readlines())])

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

    # Latency timeseries data:
    # Internally, if we're calling  numpy.percentile() on a list, numpy will take it and convert
    # it into a numpy array, which takes a bit of time. It's easier to just fill in values 
    latencies = np.subtract(timestamps[..., 1], timestamps[..., 0]) # subtract start from finish
    latencies = np.divide(latencies, 10**6) # convert from ns to ms
    
    # Used to store all requests we consider to be outstanding at a given point in time.
    cur_outstanding  = queue.PriorityQueue()

    # Go through every request:
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

            outstanding.append(cur_outstanding.qsize())

            # Move on to next window:
            cur_window += STEP_NANO

        # Will the request finish in this window? (Probably not, since STEP_NANO is small)
        # If not, we consider it to be outstanding.
        if finish > cur_window:
            cur_outstanding.put(finish)

    # Generate the actual cdf:
    counts, edges = np.histogram(outstanding, bins=500, normed=True)
    cdf = np.cumsum(counts)
    xs = edges[:-1]
    xs = xs.tolist()
    ys = [np.asscalar(x*(edges[1]-edges[0])) for x in cdf]

    fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2)

    ax1.set(xlabel="outstanding requests", ylabel="CDF")
    ax1.set_title("outstanding request CDF for max-rate=16384", fontsize=16)
    ax1.plot(xs, ys)

    counts, edges = np.histogram(latencies, bins=500, normed=True)
    cdf = np.cumsum(counts)
    xs = edges[:-1]
    xs = xs.tolist()
    ys = [np.asscalar(x*(edges[1]-edges[0])) for x in cdf]
    ax2.set(xlabel="latency (ms)", ylabel="CDF")
    ax2.set_title("latency CDF for max-rate=16384", fontsize=16)
    ax2.plot(xs, ys)

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()