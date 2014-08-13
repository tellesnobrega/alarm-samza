#!/usr/bin/python

import sys, getopt
import subprocess

path = sys.argv[1]
latency = sys.argv[2]

base_value = 420
values = [800, 1800, 2500, 4200, 8000, 16800, 25200, 33600]
for value in values:
    for i in range(10):
        storage_path=path+"/%s/%s" % (str(value), str(i))
        process = subprocess.Popen(["./run_topology.sh %s 4 %s %s ~/Downloads/telles.pem" % (str(value), str(latency),  storage_path)], shell=True)
        process.wait()
