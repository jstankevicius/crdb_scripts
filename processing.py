import os
import numpy as np
import subprocess
import time
import yaml

from pqueue import PriorityQueue
from tqdm import tqdm

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

    mean_delay = 0

    for i in range(N_REQUESTS):

        start = timestamps[i][0]
        finish = timestamps[i][1]
        latency = (finish - start)/(10**6)
        total_latencies.append(latency)

        if i > 1:
            delay = start - timestamps[i-1][0]
            mean_delay = (mean_delay*(i - 1) + delay)/i 

        # Check the request's start time. Is it outside our current window? If 
        # yes, then we should collect all data about this interval and increment 
        # the window. We should also be writing data out if we run out of 
        # requests.
        if start > window_timestamp or i == N_REQUESTS - 1:
            
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

        # If we are here, that means we're still inside our original window.
        # Then we should consider if a request will finish inside the window or
        # not. If not, we consider it to be outstanding.
        if finish > window_timestamp:
            cur_outstanding.push(finish)
        else:
            total_finished += 1
            latencies.append(latency) # convert to ms

        if window_idx == N_WINDOWS:
            break

    # If we fell out with window_idx != windows, copy everything from the last
    # window w/ recorded data to other windows. This only occurs when we run
    # out of requests to process early.
    diff = N_WINDOWS - window_idx


    if window_idx < N_WINDOWS:
        np.copyto(outstanding[window_idx-1:], outstanding[window_idx-1])

    print(f"mean offered load: {10**9/mean_delay} req/s")
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
