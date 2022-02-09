# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information

import time
import space
import subprocess
import logger


class Engine(object):
    # @binary: the absolute path of the traffic_engine
    def __init__(self, binary, ip_to_host={}):
        super(Engine).__init__()
        self._binary = binary
        self._global_port = 3000
        self._commands = {}
        self._ip_to_host = ip_to_host
        self._scripts_path = "/tmp/collie-scripts/"
        cmd = "rm -rf {}".format(self._scripts_path)
        print (cmd)
        subprocess.check_output(cmd, shell=True)
        cmd = "mkdir -p {}".format(self._scripts_path)
        print (cmd)
        subprocess.check_output(cmd, shell=True)
    # translate Point in search space into the params to set up traffic engine

    # return the number of RTS queue pairs generated by Collie.
    def check_run(self, expected_n):
        cmd = "rdma res show qp | grep 'RTS.*collie_engine' | wc -l"
        for i in range(10):
            try:
                ret = int(subprocess.check_output(
                    cmd, shell=True).decode().rstrip('\n'))
                if ret == expected_n:
                    return 0
            except Exception as e:
                print(e)
                return -1
            time.sleep(1)
        return -1

    def translate(self, point: space.Point):
        self.clean()
        for traffic in point._traffics:
            server = traffic.get_server()
            server_ip = server.get_ip()
            if server_ip not in self._commands:
                self._commands[server_ip] = {"server": [], "client": []}
            client = traffic.get_client()
            client_ip = client.get_ip()
            if client_ip not in self._commands:
                self._commands[client_ip] = {"server": [], "client": []}
            for i in range(traffic.get_process_num()):
                # Set up server first
                server_numa_node = server.get_numa()
                server_cmd = "numactl -N {} -m {} {} {} {} --server --port={} --use_cuda={} --tos=105 --share_mr --gpu_id=0 2>/dev/null &".format(
                    server_numa_node, server_numa_node,
                    self._binary, server.to_cmd(), traffic.to_cmd(),
                    self._global_port, bool(server._use_gpu))
                self._commands[server_ip]["server"].append(server_cmd)
                # Then, the client.
                client_numa_node = client.get_numa()
                client_cmd = "numactl -N {} -m {} {} {} {} --connect={} --port={} --use_cuda={} --tos=105  --share_mr --gpu_id=0 --run_infinitely 2>/dev/null &".format(
                    client_numa_node, client_numa_node,
                    self._binary, client.to_cmd(), traffic.to_cmd(),
                    server_ip, self._global_port, bool(client._use_gpu))
                self._commands[client_ip]["client"].append(client_cmd)
                self._global_port += 1
        return self._commands

    def gen_scripts(self):
        path = self._scripts_path
        ips = list(self._commands.keys())
        for ip in ips:
            with open(path + "{}_server.sh".format(ip), "w") as f:
                for cmd in self._commands[ip]["server"]:
                    f.write(cmd + "\n")
            with open(path + "{}_client.sh".format(ip), "w") as f:
                for cmd in self._commands[ip]["client"]:
                    f.write(cmd + "\n")
        return

    def log_scripts(self, point, path: str):
        self.translate(point)
        ips = list(self._commands.keys())
        if not path.endswith(".sh"):
            path += ".sh"
        with open(path, "w") as f:
            for ip in ips:
                for cmd in self._commands[ip]["server"]:
                    f.write(
                        "ssh {}@{} -n -f \"{}\"".format(self._ip_to_host[ip], ip, cmd) + "\n")
                f.write("sleep 1\n")
            for ip in ips:
                for cmd in self._commands[ip]["client"]:
                    f.write(
                        "ssh {}@{} -n -f \"{}\"".format(self._ip_to_host[ip], ip, cmd) + "\n")
                f.write("sleep 1\n")
        self.clean()

    def save_err_scripts(self):
        ips = list(self._commands.keys())
        cmd = "mkdir -p {}/err_scripts/".format(self._scripts_path)
        subprocess.check_output(cmd, shell=True)
        for ip in ips:
            cmd = "mv {}/{}_server.sh {}/err_scripts/".format(
                self._scripts_path, ip, self._scripts_path)
            subprocess.check_output(cmd, shell=True)
            cmd = "mv {}/{}_client.sh {}/err_scripts/".format(
                self._scripts_path, ip, self._scripts_path)
            subprocess.check_output(cmd, shell=True)
        return

    def run_scripts(self):
        ips = list(self._commands.keys())
        for ip in ips:
            # scp /tmp/ip_server.sh
            cmd = ["scp", "{}{}_server.sh".format(self._scripts_path, ip), "{}{}_client.sh".format(self._scripts_path, ip),
                   "{}@{}:/tmp/".format(self._ip_to_host[ip], ip)]
            try:
                subprocess.check_output(cmd)
            except Exception as e:
                print(e)
                return -1
            setup_cmd = ["ssh", "-n", "-f", "{}@{}".format(self._ip_to_host[ip], ip),
                         "bash /tmp/{}_server.sh".format(ip)]
            try:
                subprocess.run(setup_cmd, stdout=subprocess.DEVNULL)
            except Exception as e:
                print(e)
                return -1
        # We have copied the scripts to all hosts and have set up servers.
        # Now we only need to set up clients
        for ip in ips:
            cmd = ["ssh", "-n", "-f", "{}@{}".format(self._ip_to_host[ip], ip),
                   "bash /tmp/{}_client.sh".format(ip)]
            try:
                subprocess.run(cmd, stdout=subprocess.DEVNULL)
            except Exception as e:
                print(e)
                return -1
        return 0

    def set_up_traffic(self, point):
        # return 0 means success
        self.translate(point)
        self.gen_scripts()
        self.run_scripts()
        n = self.check_run(point.get_total_qps())
        if (n != 0):
            self.save_err_scripts()
            return -1
        return 0

    def clean(self, global_port=3000):
        self._global_port = global_port
        self._commands.clear()

    def killall(self):
        ips = self._ip_to_host.keys()
        clean_cmds = []
        for ip in ips:
            cmd = ["ssh", "{}@{}".format(self._ip_to_host[ip], ip),
                   "killall", "{}".format(self._binary)]
            try:
                subprocess.run(cmd, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            except Exception as e:
                print(e)
                return -1
            clean_cmds.append(cmd)
        with open("./clean.sh", "w") as f:
            for cmd in clean_cmds:
                str = ""
                for i in cmd:
                    str += i + " "
                f.write(str + "\n")
        return 0

    def clean_process(self):
        # return 0 means success
        if self.killall():
            print("Error when killing all engines")
            return -1
        n = self.check_run(0)
        if (n != 0):
            print("Checking Killall failed.")
            return -1
        self.clean()
        return 0