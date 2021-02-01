import argparse
import datetime
import glob
import random
import matplotlib
import matplotlib.pyplot as plt
import os
import re
import string
import yaml
from pprint import pprint
from os.path import isfile, join

STAT_LABELS = {
    "throughput": "throughput (requests/sec)",
    "latency": "latency (ms)",
    "outstanding": "# outstanding requests"
}

# In case this script is invoked and graphs are supposed to be saved, we'll take
# a timestamp. This will be the prefix for all the graph image files.
TIMESTR = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_filepaths(rx):
    # Return all files matching the given regular expression.
    return glob.glob(os.path.join("", rx))


def plot_ts_stat(workloads, ts_stat, save=False):

    # We support plotting multiple experiments at the same time.
    for workload in workloads.values():

        fig = plt.figure()
        axes = plt.axes()

        seconds = workload["ts"]["seconds"]
        rate = workload["flags"]["max-rate"]
        concurrency = workload["flags"]["concurrency"]

        if ts_stat == "latency":

            # Force latency to begin at 0.
            # TODO(jstankevicius): This is (probably) because of an 
            # off-by-one bug in the processing script.
            workload["ts"]["p99"][0] = 0
            workload["ts"]["p90"][0] = 0
            workload["ts"]["p50"][0] = 0

            line99, = axes.plot(seconds, workload["ts"]["p99"])
            line99.set_label("p99")
            line90, = axes.plot(seconds, workload["ts"]["p90"])
            line90.set_label("p90")
            line50, = axes.plot(seconds, workload["ts"]["p50"])
            line50.set_label("p50")
            axes.set(xlabel="time (seconds)", ylabel=STAT_LABELS["latency"])
            axes.set_title(f"latency vs. time for max-rate={rate}")
            axes.legend(loc="upper left", fontsize="xx-small")

        else:
            ts = workload["ts"][ts_stat]
            ts[0] = 0
            line, = axes.plot(seconds, ts)

            axes.set_title(f"{ts_stat} vs. time for max-rate={rate}")
            axes.set(xlabel="time (s)", ylabel=STAT_LABELS[ts_stat])

        axes.grid()
        plt.tight_layout()

        if save:
            # We might have two different max-rate flags. In that case, it can
            # be useful to identify experiments by random strings. I should 
            # start including experiment names inside each YAML file.
            # rstr = ''.join(random.choice(string.ascii_letters) for _ in range(6))
            imgname = f"{rate}_{ts_stat}.png"
            fig.savefig(imgname)
            print(f"graph saved as {imgname}")


def main():
    parser = argparse.ArgumentParser(description="Utility for producing per-experiment"
    + " timeseries graphs.")

    parser.add_argument("dirs", nargs="+",
    help="one or more experiment YAML files or regular expressions")

    parser.add_argument("-throughput", action="store_true",
    help="produce a throughput graph")

    parser.add_argument("-latency", action="store_true",
    help="produce a latency graph")

    parser.add_argument("-outstanding", action="store_true",
    help="produce a graph of outstanding requests")

    parser.add_argument("--show", action="store_true",
    help="show graphs on the screen")

    parser.add_argument("--save", action="store_true",
    help="save graphs in current directory as .png files")

    args = parser.parse_args()

    # By default, if neither -l or -o are provided, we treat it as if we're just
    # graphing throughput (since that's the most common use-case).
    latency = args.latency
    outstanding = args.outstanding
    throughput = args.throughput or not (latency or outstanding)
    show = args.show
    save = args.save

    if not show and not save:
        show = True # ...because otherwise what's the point of invoking this?

    exp_files = []
    for rx in args.dirs:
        for result in get_filepaths(rx):
            exp_files.append(result)

    workloads = {}
    for file in exp_files:
        with open(file, "r") as infile:
            experiment = yaml.full_load(infile)
            workloads[file] = {
                "ts": experiment["ts"],
                "flags": experiment["flags"]
            }

    if throughput:
        plot_ts_stat(workloads, "throughput", save)

    if latency:
        plot_ts_stat(workloads, "latency", save)

    if outstanding:
        plot_ts_stat(workloads, "outstanding", save)

    if show:
        plt.show()


if __name__ == "__main__":
    main()