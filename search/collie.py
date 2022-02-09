# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information

import argparse
import json
from anneal import Director


if __name__ == "__main__":
    # start of user parameters input
    # [NOTICE] Set the usernames and IPs of the hosts to test below
    # TODO: parse user's configurations from JSON
    
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument("--config", action="store", help="The configuration file for Collie")
    args = parser.parse_args()
    config = {}
    with open(args.config, "r") as f:
        config = json.load(f)
    username = config["username"]
    iplist = config["iplist"]
    logpath = config["logpath"]  # The log path. e.g., /tmp/
    # The collie_engine path. e.g., ./traffic_engine/collie_engine. Absolute path is recommended
    collie_engine = config["engine"]
    iters = config["iters"]  # How many test cases Collie would run.
    # Users should set their expectation/anomaly threshold here.
    bars = config["bars"]
    # end of user parameters input
    config = {}
    config["bars"] = bars
    diag_counters = [
        # Diagnostic counters are not publicly available.
        # If vendor provides, should add here and modify hardware/monitor.py
    ]
    config["counters"] = diag_counters
    director = Director(traffic_binary=collie_engine,
                        hwmon_binary="",
                        config=config, ip_A=iplist[0], ip_B=iplist[-1],
                        usr_A=username, usr_B=username, logpath=logpath)
    ret = director.simulated_annealing(iters=iters)
