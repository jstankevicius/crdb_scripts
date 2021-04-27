# This is not an extensible module. plot.py is a general-purpose tool for
# plotting multiple experiments together. As such, almost none of the functions
# in this file are usable outside of it. Code quality is also not particularly
# good.

import argparse
from cycler import cycler
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
    "throughput": ["p50", "p10", "p1"],
    "latency": ["p99", "p90", "p50"],
    "outstanding": ["p99", "p90", "p50"]
}

STAT_LABELS = {
    "throughput": "throughput (requests/sec)",
    "latency": "latency (ms)",
    "outstanding": "# outstanding requests"
}

DOMAIN_LABELS = {
    "max-rate": "target offered load (requests/sec)",
    "concurrency": "# client threads"
}

plt.rcParams["axes.prop_cycle"] = cycler(color=[
    "#000000", "#CD0000", "#00CD00", "#0000EE", "#CD00CD", "#00CDCD", "#7F7F7F", "#75507B"])

def get_filepaths(rx):
    # Return all files matching the given regular expression.
    return glob.glob(os.path.join("", rx))

# TODO: this is stupid
def get_flags_and_aggregate(file):
    # Return a YAML file's 'flags' and 'aggregate' fields.
    with open(file) as f:
        y = yaml.full_load(f)
        return (y["flags"], y["aggregate"])

def plot_exp_aggregate_stats(experiments, domain_stat, aggregate_stat, percentiles):
    fig = plt.figure(figsize=(4, 4))

    axes = plt.axes()
    lines = {}

    for exp_name, exp_data in experiments.items():

        # Grab x-axis for experiment
        rates = exp_data.keys()
        
        for rate, aggregate in exp_data.items():
            for percentile in percentiles:

                if len(experiments) > 1:
                    exp_fullname = f"{exp_name} ({percentile})"
                else:
                    exp_fullname = percentile
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

    axes.legend(loc="upper right", fontsize="small")
    axes.set(xlabel=DOMAIN_LABELS[domain_stat], ylabel=STAT_LABELS[aggregate_stat])
    axes.grid()

    return fig


def main():
    parser = argparse.ArgumentParser(description="Utility for producing multi-experiment graphs.")

    parser.add_argument("dirs", nargs="+",
    help="one or more experiment directories or regular expressions")

    parser.add_argument("-throughput", nargs="*", help="throughput percentiles")

    parser.add_argument("-latency", nargs="*", help="latency percentiles")

    parser.add_argument("-outstanding", nargs="*", help="outstanding request percentiles")

    parser.add_argument("--show", action="store_true",
    help="show graphs on the screen")

    parser.add_argument("--save", action="store_true",
    help="save graphs in current directory as .png files")

    parser.add_argument("--x", nargs="?", default="max-rate",
    help="what to plot t/l/o against")

    parser.add_argument("--title", nargs="?", default="", help="plot title")
    args = parser.parse_args()

    # If either any of the throughput/latency/outstanding flags are not passed in, we do not graph
    # them. If any are passed in with no arguments, we graph the default percentiles. If they are
    # passed in with a list of percentiles, graph those percentiles.
    graph_latency = args.latency is not None
    graph_outstanding = args.outstanding is not None
    graph_throughput = args.throughput is not None or not (graph_latency or graph_outstanding)

    latency_percentiles = args.latency
    throughput_percentiles = args.throughput
    outstanding_percentiles = args.outstanding

    if graph_latency:
        if len(latency_percentiles) == 0:
            latency_percentiles = DEFAULT_PERCENTILES["latency"]

    if graph_outstanding:
        if len(outstanding_percentiles) == 0:
            outstanding_percentiles = DEFAULT_PERCENTILES["outstanding"]

    if graph_throughput:
        if len(throughput_percentiles) == 0:
            throughput_percentiles = DEFAULT_PERCENTILES["throughput"]

    show = args.show
    save = args.save

    if not show and not save:
        save = True # ...because otherwise what's the point of invoking this?

    exp_dirs = []
    for rx in args.dirs:
        for result in get_filepaths(rx):
            exp_dirs.append(result)

    # For every experiment, we'll want to know:
    #   1. its name (this will be the index)
    #   2. the max-rate flags that were used for it
    #   3. aggregate data for latency/throughput/outstanding
    experiments = {}
    
    # Go through each experiment directory.
    for directory in exp_dirs:
        data = []
        
        # Obtain everything inside the experiment directory.
        for f in os.listdir(directory):

            if isfile(join(directory, f)):
                data.append(get_flags_and_aggregate(directory + "/" + f))

        # Sort experiments by args.x (which should be a flag) in ascending order. Then pull out each
        # experiment's "domain" value (its place on the x-axis of the graph) and its "aggregate"
        # statistic (its place on the y-axis).
        data.sort(key=lambda f: f[0][args.x])
        x = [f[0][args.x] for f in data]
        y = [f[1] for f in data]

        experiments[directory] = {x[i]: y[i] for i in range(len(x))}

        # Now experiments looks like
        # {
        #     ["experiment-name"]: [x1: y1, x2: y2...]
        # }

    # Plot
    if graph_throughput:
        fig = plot_exp_aggregate_stats(experiments, args.x, "throughput", throughput_percentiles)
        plt.title(args.title, wrap=True)
        plt.tight_layout()

        if args.save:
            imgname = f"{TIMESTR}_throughput.png"
            fig.savefig(imgname)
            print(f"graph saved as {imgname}")
    
    if graph_latency:
        fig = plot_exp_aggregate_stats(experiments, args.title, args.x, "latency", latency_percentiles)
        plt.title(args.title, wrap=True)
        plt.tight_layout()

        if args.save:
            imgname = f"{TIMESTR}_latency.png"
            fig.savefig(imgname)
            print(f"graph saved as {imgname}")

    if graph_outstanding:
        fig = plot_exp_aggregate_stats(experiments, args.title, args.x, "outstanding", outstanding_percentiles)
        plt.title(args.title, wrap=True)
        plt.tight_layout()

        if args.save:
            imgname = f"{TIMESTR}_outstanding.png"
            fig.savefig(imgname)
            print(f"graph saved as {imgname}")

    if show:
        plt.show()
        
if __name__ == "__main__":
    main()
