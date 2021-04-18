from pprint import pprint
import datetime as dt
from datetime import datetime, timezone, timedelta
import matplotlib.pyplot as plt
import numpy as np

def parse_cockroach_log(path, start_ts):

    # Cockroach logs are in GMT+0 time, our request timestamps are in local
    # UNIX time. The total difference is 6 hours. This is dumb.
    start_dt = datetime.fromtimestamp(start_ts/10**9) + timedelta(hours=6)

    with open(path) as log:

        # We care about lines in the following format:
        # W210416 04:25:30.341930 170828 sql/stats/automatic_stats.go:384
        # W and E are what we really want, since those are warnings or errors
        
        sources = {}
        for line in log:
            s = line.split()
            if len(s) > 0:
                if len(s[0]) == 7 and s[0][0] in ("W", "E"):

                    # Figure out UNIX timestamp of this log entry
                    # Following the example above, we get the following:
                    date = s[0][1:] # 210416
                    timeofday = s[1] # 04:25:30.341930
                    dt = datetime.strptime(f"{date} {timeofday}", "%y%m%d %H:%M:%S.%f")
                    t = (dt - start_dt).total_seconds()
                    
                    # file the entry came from
                    source = s[3] # sql/stats/automatic_stats.go:384
                    
                    if t >= 0:
                        if source not in sources.keys():
                            sources[source] = []

                        sources[source].append(t)

        cmap = plt.get_cmap("tab10")
        i = 0
        for source, timestamps in sources.items():
            print(f"{source}: {len(timestamps)}")
            plt.vlines(x=timestamps, ymin=0, ymax=1, colors=cmap(i), label=source)
            i += 1

        plt.legend(loc="center right")
        plt.savefig("asdf.png")

parse_cockroach_log("open-loop/logs/max-rate80000/node-0/logs/cockroach.log", 1618547085418210044)