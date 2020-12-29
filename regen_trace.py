# In case I realize I messed something up with data processing
# after the experiment is over. This lets me generate a new YAML 
# file from every trace for that experiment.

import argparse
import os
from os.path import isfile, join
import yaml
from main import process_timeseries

def main():
    parser = argparse.ArgumentParser()

    # Arguments:
    parser.add_argument("dir", help="experiment directory")
    args = parser.parse_args()
    tracedir = args.dir + "/traces"

    trace_files = [f for f in os.listdir(tracedir) if isfile(join(tracedir, f))]
    yaml_files = [f for f in os.listdir(args.dir) if isfile(join(args.dir, f))]


    regen_dir = args.dir + "_regen"
    os.system("mkdir " + regen_dir)

    for exp_data in yaml_files:
        name = exp_data.split(".")[0]

        print(name)
        with open(args.dir + "/" + exp_data) as yaml_file:
            data = yaml.full_load(yaml_file)
            flags = data["flags"]
        
        with open(args.dir + "/traces/" + name + ".txt") as trace:
            timeseries, aggregate_data = process_timeseries(trace.readlines())
        
        # Now dump it into a new YAML file:
        with open(regen_dir + "/" + exp_data, "w") as regen_file:
            exp_data = {
                    "flags": flags,
                    "aggregate": aggregate_data,
                    "ts": timeseries
                }
            yaml.dump(exp_data, regen_file, default_flow_style=None, width=80)

if __name__ == "__main__":
    main()