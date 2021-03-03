# crdb_scripts: Scripts for benchmarking CockroachDB on CloudLab

## Usage
1. Instantiate the `crdb_cluster` profile on CloudLab. Wait for the nodes to finish booting.
2. SSH into `workload_node`.
3. Set up the repo:
```
git clone https://github.com/jstankevicius/crdb_scripts
cd crdb_scripts
./setup.sh
```
This will install all the Python libraries required to run the scripts, mount `/dev/sda4` onto your Go directory, and clone `https://github.com/jstankevicius/cockroach`. `setup.sh` also sets the git username to `jstankevicius`, so you might want to override that.

If you encounter a permission error, try `chmod +x setup.sh`.

4. Log out of your SSH session and log back in.

5. 
```
cd ~/go/src/github.com/cockroachdb/cockroach
git checkout open-loop-workload
build/builder.sh make vendor_rebuild
build/builder.sh make build
sudo mv cockroach /usr/local/bin
```
It is possible that `build/builder.sh make vendor_rebuild` will fail. Ignore the failure.

6. Somehow install CockroachDB on the other nodes in the cluster. You can do this by building another binary and `scp`'ing it over. There are root SSH keys installed between every node, so you have access to `sudo ssh` and `sudo scp`. 
7. Configure `config.yaml` to your liking.
8. Run `python3 main.py`. This will run all the workloads you have defined inside `config.yaml`. All experiment data will be saved inside a directory that is named after your experiment. The `traces` directory contains all the raw data.
9. Use `tsplot.py` on YAML files or `plot.py` on experiment directories to create plots of your data. For example, if you have an experiment named `exp` and want throughput timeseries plots for every workload in the experiment, you can run `python3 tsplot.py exp/*.yaml -t --save`. This will save a PNG file for every workload. If you are using VS Code with the SSH extension, you can view these images directly in the editor.


A lot of these scripts/methodologies are things I've found to just "work," so they might not be very elegant or precise. I might further streamline the benchmarking process in the future.
