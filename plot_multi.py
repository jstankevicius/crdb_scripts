import argparse
import yaml
import numpy as np
import os
import matplotlib
import matplotlib.pyplot as plt
from os.path import isfile, join
from pprint import pprint

def main():
    dirs = ["ratelimit25", "ratelimit35", "ratelimit55", "ratelimit65", "ratelimit75"]
    lines = dict()

    # Hacky
    offered_load = set()
    offered_load.add(0)
    for d in dirs:
        files = [f for f in os.listdir(d) if isfile(join(d, f))]

        # Sort by the max-rate flag
        # (this probably keeps the files open, but that's okay because they're small.)
        files.sort(key=lambda name: yaml.full_load(open(d + "/" + name, "r"))["flags"]["max-rate"])

        print(d)
        lines[d] = {
            "p99": [0],
            "p90": [0]
        }

        for fname in files:
            with open(d + "/" + fname, "r") as infile:
                experiment = yaml.full_load(infile)
                aggregate = experiment["aggregate"]
                timeseries = experiment["ts"]
                flags = experiment["flags"]

                # Append throughput percentiles
                lines[d]["p99"].append(aggregate["latency"]["p99"])
                lines[d]["p90"].append(aggregate["latency"]["p90"])
                offered_load.add(flags["max-rate"])

    # Convert offered-load into list and sort
    offered_load = sorted(list(offered_load))

    # Start plotting:
    fig, ax1 = plt.subplots(nrows=1, ncols=1)    
    ax1.set_title("90th/99th latency percentiles for various rate limits")
    ax1.set(xlabel="offered load (avg. requests/sec)", ylabel="latency (ms)")

    style_idx = 0
    styles = ["o", "s", "v", "d", "x", "h"]
    for name, datatype in lines.items():
        for percentile, data in datatype.items():
            line, = ax1.plot(offered_load, data, marker=styles[style_idx])
            line.set_label(name + "_" + percentile)

        style_idx += 1

    ax1.legend(loc="upper left", fontsize="xx-small")
    ax1.grid()
    plt.show()


if __name__ == "__main__":
    main()