import argparse
import yaml
import numpy as np
import os
import matplotlib
import matplotlib.pyplot as plt
from os.path import isfile, join

matplotlib.rcParams.update({"font.size": 12})
plt.rcParams["figure.figsize"] = (10, 4)

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
    args = parser.parse_args()
    
    p99_latency_list = []
    p90_latency_list = []
    p50_latency_list = []
    offered_load_list = []

    p99_or_list = []
    p90_or_list = []
    p50_or_list = []

    p99_throughput_list = []
    p90_throughput_list = []
    p50_throughput_list = []

    files = [f for f in os.listdir(args.dir) if isfile(join(args.dir, f))]
    
    for fname in files:
        with open(args.dir + "/" + fname, "r") as infile:
            experiment = yaml.full_load(infile)
            timeseries = experiment["ts"]
            flags = experiment["flags"]
            aggregate = experiment["aggregate"]
            p99_latency_list.append(aggregate["latency"]["p99"])
            p90_latency_list.append(aggregate["latency"]["p90"])
            p50_latency_list.append(aggregate["latency"]["p50"])

            offered_load_list.append(flags["max-rate"])

            p99_throughput_list.append(aggregate["throughput"]["p50"])
            p90_throughput_list.append(aggregate["throughput"]["p10"])
            p50_throughput_list.append(aggregate["throughput"]["p1"])

            p99_or_list.append(aggregate["outstanding"]["p99"])
            p90_or_list.append(aggregate["outstanding"]["p90"])
            p50_or_list.append(aggregate["outstanding"]["p50"])
            
    c = sorted(zip(offered_load_list, 
    p99_latency_list, p90_latency_list, p50_latency_list, 
    p99_or_list, p90_or_list, p50_or_list,
    p99_throughput_list, p90_throughput_list, p50_throughput_list), key=lambda t: t[0])

    # vomit
    offered_load_list, p99_latency_list, p90_latency_list, p50_latency_list, p99_or_list, p90_or_list, p50_or_list, p99_throughput_list, p90_throughput_list, p50_throughput_list = zip(*c)
    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3)

    fig.suptitle("Per-experiment metrics on 4-node cluster w/ rate-limit=65k")

    p99_throughput_line, = ax1.plot(offered_load_list, p99_throughput_list)
    p90_throughput_line, = ax1.plot(offered_load_list, p90_throughput_list)
    p50_throughput_line, = ax1.plot(offered_load_list, p50_throughput_list)

    p99_throughput_line.set_label("p50")
    p90_throughput_line.set_label("p10")
    p50_throughput_line.set_label("p1")
    ax1.legend()

    ax1.set_title("throughput vs. offered load", fontsize=12)
    ax1.set(xlabel="offered load (avg. requests/sec)", ylabel="throughput (requests/sec)")

    p99_line, = ax2.plot(offered_load_list, p99_latency_list)
    p90_line, = ax2.plot(offered_load_list, p90_latency_list)
    p50_line, = ax2.plot(offered_load_list, p50_latency_list)

    p99_line.set_label("p99")
    p90_line.set_label("p90")
    p50_line.set_label("p50")
    ax2.legend()

    ax2.set(xlabel="offered load (avg. requests/sec)", ylabel="latency (ms)")
    ax2.set_title("latency vs. offered load", fontsize=12)

    p99_or_line, = ax3.plot(offered_load_list, p99_or_list)
    p90_or_line, = ax3.plot(offered_load_list, p90_or_list)
    p50_or_line, = ax3.plot(offered_load_list, p50_or_list)

    p99_or_line.set_label("p99")
    p90_or_line.set_label("p90")
    p50_or_line.set_label("p50")

    ax3.legend()
    ax3.set(xlabel="offered load (avg. requests/sec)", ylabel="number of outstanding requests")
    ax3.set_title("outstanding requests vs. offered load", fontsize=12)

    ax1.grid()
    ax2.grid()
    ax3.grid()
    plt.tight_layout()
    plt.show()      
        
if __name__ == "__main__":
    main()
