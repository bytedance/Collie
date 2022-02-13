# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information


import subprocess
from numpy.lib.stride_tricks import _maybe_view_as_subclass


def Init(path=None):
    if path == None:
        return
    else:
        if path[-1] != '/':
            path += '/'
        # Remove old directories
        # reproduce
        cmdline = "rm -rf " + path + "reproduce/*"
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        # result
        cmdline = "rm -rf " + path + "result/*"
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)

        # Create new directories for running status and results.
        cmdline = "mkdir -p {}".format(path + "reproduce")
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p {}".format(path + "result")
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p {}".format(path + "target")
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p {}".format(path + "result/mfs_help")
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p {}".format(path +
                                       "reproduce/mfs_help")
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "rm -rf /tmp/collie/"
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p /tmp/collie"
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
    return


def LOG(message: str, mode="INFO"):
    if mode == "INFO":
        print("{}".format(message))
    elif mode == "BLOCK":
        print("\033[0;32;40m{}\033[0m".format(message))
    elif mode == "WARNING":
        print("\033[1;33;40m{}\033[0m".format(message))
    elif mode == "ERROR":
        print("\033[1;31;40m{}\033[0m".format(message))


def printKV(d: dict, mode="INFO"):
    keys = d.keys()
    for key in keys:
        message = "{}:{}".format(key, d[key])
        LOG(message, mode)
