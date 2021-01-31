# This is not an extensible module. plot.py is a general-purpose tool for
# plotting multiple experiments together. As such, almost none of the functions
# in this file are usable outside of it. Code quality is also not particularly
# good.

import argparse
import datetime
import glob
import matplotlib
import matplotlib.pyplot as plt
import os
import re
import yaml
from pprint import pprint
from os.path import isfile, join

STYLES = ["o", "s", "v", "d", "x", "h"]

# In case this script is invoked and graphs are supposed to be saved, we'll take
# a timestamp. This will be the prefix for all the graph image files.
TIMESTR = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# These can be changed to flags. But generally we don't really care about any
# percentiles besides the ones below. We only plot a select few percentiles per
# experiment because the graph would otherwise become very cluttered.
DEFAULT_PERCENTILES = {
    "throughput": ["p50", "p1"],
    "latency": ["p99"],
    "outstanding": ["p99"]
}

STAT_LABELS = {
    "throughput": "throughput (requests/sec)",
    "latency": "latency (ms)",
    "outstanding": "# outstanding requests"
}

def get_filepaths(rx):
    # Return all files matching the given regular expression.
    return glob.glob(os.path.join("", rx))

def get_flags_and_aggregate(file):
    # Return a YAML file's 'flags' and 'aggregate' fields.
    with open(file) as f:
        y = yaml.full_load(f)
        return (y["flags"], y["aggregate"])

def plot_exp_aggregate_stats(experiments, aggregate_stat, save=False):
    fig = plt.figure()

    axes = plt.axes()
    lines = {}

    for exp_name, exp_data in experiments.items():

        # Grab x-axis for experiment
        rates = exp_data.keys()
        
        for rate, aggregate in exp_data.items():
            for percentile in DEFAULT_PERCENTILES[aggregate_stat]:
                exp_fullname = f"{exp_name} ({percentile})"
                if exp_fullname not in lines.keys():
                    lines[exp_fullname] = ([0] + list(rates), [0])

                lines[exp_fullname][1].append(aggregate[aggregate_stat][percentile])
    
    # Now plot everything:
    style_idx = 0
    for line_name, line_data in lines.items():
        line_x, line_y = line_data
        line, = axes.plot(line_x, line_y, marker=STYLES[style_idx])
        line.set_label(line_name)
        style_idx += 1

        if style_idx == len(STYLES):
            style_idx = 0

    axes.legend(loc="upper left", fontsize="xx-small")
    axes.set(xlabel="offered load (avg. requests/sec)", ylabel=STAT_LABELS[aggregate_stat])
    axes.grid()
    plt.tight_layout()

    if save:
        imgname = f"{TIMESTR}_{aggregate_stat}.png"
        fig.savefig(imgname)
        print(f"graph saved as {imgname}")

def main():
    parser = argparse.ArgumentParser(description="Utility for producing multi-experiment graphs.")

    parser.add_argument("dirs", nargs="+",
    help="one or more experiment directories or regular expressions")

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

    exp_dirs = []
    for rx in args.dirs:
        for result in get_filepaths(rx):
            exp_dirs.append(result)

    # For every experiment, we'll want to know:
    #   1. its name (this will be the index)
    #   2. the max-rate flags that were used for it
    #   3. aggregate data for latency/throughput/outstanding
    experiments = {}
    
    # For now, we'll just assume that we're only plotting against max-rate.
    for d in exp_dirs:
        data = [get_flags_and_aggregate(d + "/" + f) for f in os.listdir(d) if isfile(join(d, f))]

        # max-rate is hardcoded. if we want to plot against other flags, we 
        # might want to make this into a cmdline arg of its own.
        data.sort(key=lambda f: f[0]["max-rate"])
        rates = [f[0]["max-rate"] for f in data]
        aggregate = [f[1] for f in data]

        experiments[d] = {rates[i]: aggregate[i] for i in range(len(rates))}

    # Now plot (conditionally)
    if throughput:
        plot_exp_aggregate_stats(experiments, "throughput", save)
    
    if latency:
        plot_exp_aggregate_stats(experiments, "latency", save)

    if outstanding:
        plot_exp_aggregate_stats(experiments, "outstanding", save)

    if show:
        plt.show()
        
if __name__ == "__main__":
    main()