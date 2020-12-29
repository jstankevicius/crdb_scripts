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

def main():
    parser = argparse.ArgumentParser()

    # Arguments:
    parser.add_argument("infile", help="YAML file with experiment data and configuration.")
    args = parser.parse_args()

    with open(args.infile, "r") as infile:
        experiment = yaml.full_load(infile)
        timeseries = experiment["ts"]
        flags = experiment["flags"]

    outstanding = timeseries["outstanding"]
    seconds = timeseries["seconds"]
    p99_latency = timeseries["p99"]
    p90_latency = timeseries["p90"]
    p50_latency = timeseries["p50"]
    provided_load = timeseries["throughput"]
    fig, (ax1, ax2, ax3) = plt.subplots(nrows=1, ncols=3)

    fig.suptitle("Timeseries graphs of outstanding requests, throughput, and latency percentiles")

    # Plot outstanding requests vs time:
    outstanding[0] = 0
    ax1.set(xlabel="time (s)", ylabel="outstanding requests")
    ax1.set_title("outstanding requests vs. time", fontsize=16)
    ax1.plot(seconds, outstanding)

    # Offered vs provided load:
    provided_load[0] = 0
    ax2.set(xlabel="time (s)", ylabel="provided load (avg. requests/sec)")
    ax2.set_title("throughput vs. time", fontsize=16)
    ax2.plot(seconds, provided_load)

    # Latency vs offered load:
    ax3.set(xlabel="time(s)", ylabel="latency (ms)")
    ax3.set_title("latency percentiles vs. time", fontsize=16)

    p99_latency[0] = 0
    p90_latency[0] = 0
    p50_latency[0] = 0
    p99_line, = ax3.plot(seconds, p99_latency, linestyle="solid")
    p90_line, = ax3.plot(seconds, p90_latency, linestyle="--")
    p50_line, = ax3.plot(seconds, p50_latency, linestyle="-.")

    p99_line.set_label("p99")
    p90_line.set_label("p90")
    p50_line.set_label("p50")

    ax3.legend()

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    main()
