# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information

import os.path
import numpy as np
import time
import random
import json
import copy
import subprocess

from numpy.core.arrayprint import format_float_scientific
import hardware
import bone
from space import MTU_TO_REQ, QP_TO_NAME, Endhost, Point, Space, Traffic
from engine import Engine
import logger

kStageIterations = 2
kStartTemp = 1
kAlpha = 0.99
kPerfInitValue = 1.0


def log_reproduce(path: str, point: Point, engine: Engine):
    engine.log_scripts(point, path)
    return


def log_result(path: str, point: Point, bone_results, hw_results):
    with open(path, "w") as f:
        logs = point.log_to_dict()
        json.dump(logs, f)
        f.write('\n\n')
        for key in hw_results:
            f.write("{}:  {}\n" .format(key, hw_results[key]))
        f.write('\n')
        for key in bone_results:
            f.write("{}:  {}\n".format(key, bone_results[key]))
        f.write('\n')


class Director(object):
    def __init__(self,
                 traffic_binary: str,
                 hwmon_binary: str,
                 config: dict,
                 ip_A: str,
                 ip_B: str,
                 usr_A: str,
                 usr_B: str,
                 A_numarange=(0, 1),
                 B_numarange=(0, 1),
                 use_gpu=False,
                 A_cudarange=(0, 0),
                 B_cudarange=(0, 0),
                 ibdev_A="mlx5_0",
                 ibdev_B="mlx5_0",
                 bonedev_A="rdma0",
                 bonedev_B="rdma0",
                 identity_A="0000:18:00.0",
                 identity_B="0000:18:00.0",
                 mode="random",
                 logpath="/tmp/"):
        super(Director).__init__()
        # config["counters"] is the diagnosic counters to collect
        # config["bars"] is the bars dictionary. (e.g., pps_bar, bps_bar, tx_pfc_bar)
        keys = list(config.keys())
        if "bars" not in keys or "counters" not in keys:
            print("Director needs bars and counters for further step.")
            print("Exit...")
            return None
        self._target_counter = None
        self._err_pts_cnt = 0
        self._traffic_binary = str(traffic_binary)
        self._hwmon_binary = str(hwmon_binary)
        self._mode = mode
        self._ip_A = ip_A
        self._ip_B = ip_B
        self._usr_A = usr_A
        self._usr_B = usr_B
        self._ibdev_A = ibdev_A
        self._ibdev_B = ibdev_B
        self._identity_A = identity_A
        self._identity_B = identity_B
        self._bonedev_A = bonedev_A
        self._bonedev_B = bonedev_B
        # Class below
        self._config = config
        self._hwmon = hardware.MlnxHwMon(
            self._hwmon_binary, config["counters"])
        self._bonemon = bone.MlnxBoneMon(**config["bars"])
        self._engine = Engine(self._traffic_binary,
                              ip_to_host=self.get_ip_to_usr())
        self._space = Space(usr_A=usr_A, usr_B=usr_B, ip_A=ip_A, ip_B=ip_B,
                            ibdev_A=ibdev_A, ibdev_B=ibdev_B, A_numarange=A_numarange, B_numarange=B_numarange,
                            use_gpu=use_gpu, A_cudarange=A_cudarange, B_cudarange=B_cudarange)
        if logpath[-1] != '/':
            logpath += '/'
        self._log_path = logpath
        self._global_log_idx = 1
        logger.Init(logpath)

    def get_ip_to_usr(self):
        return {self._ip_A: self._usr_A, self._ip_B: self._usr_B}

    def error_point(self, point, bone_results, hw_results):
        logger.LOG("Print Anomalous point {}".format(
            self._err_pts_cnt), "BLOCK")
        logs = point.log_to_dict()
        logger.LOG(logs, "ERROR")
        logger.printKV(bone_results, "ERROR")
        logger.printKV(hw_results, "WARNING")
        logger.LOG("Print Anomalous point {} over".format(
            self._err_pts_cnt), "BLOCK")
        print("")
        self._err_pts_cnt += 1
        return

    # Collie run a set of random points and select which counter has higher priority
    def sort_target(self, records: list) -> list:
        hw_counters = {key: [] for key in records[0][1].keys()}
        for record in records:
            for counter in record[1].keys():
                hw_counters[counter].append(record[1][counter])
        for counter in hw_counters.keys():
            mean = np.mean(hw_counters[counter])
            var = np.var(hw_counters[counter])
            logger.LOG("counter:{}, mean:{}, var:{}".format(
                counter, mean, var), "WARNING")
            if mean == 0.0:
                hw_counters[counter] = 0.0
            else:
                hw_counters[counter] = var / mean
        print(hw_counters)
        sorted_counters = dict(
            sorted(hw_counters.items(), key=lambda item: item[1], reverse=True))
        logger.printKV(sorted_counters)
        return list(sorted_counters.keys())

    def set_target(self, target: str):
        self._target_counter = target

    def get_target(self, hw_results):
        return float(hw_results[self._target_counter])

    def get_perf_bps(self, point: Point, bone_results):
        forward = 0
        backward = 0
        for req in point._traffics[0]._reqs:
            if 'r' in req:
                backward = 1
            else:
                forward = 1
        if point._traffics[1]._server._ip == point._traffics[1]._client._ip:
            backward = forward = 1
        else:
            for req in point._traffics[1]._reqs:
                if 'r' in req:
                    forward = 1
                else:
                    backward = 1
        # PCIe bandwidth is the real upper bound
        return float((forward + backward) * 128.0 - bone_results["tx_vport_rdma_unicast_bytes"] - bone_results["rx_vport_rdma_unicast_bytes"])

    # This is less useful in reality since ML cares bandwidth
    def get_perf_pps(self, point: Point, bone_results):
        cnt = 1
        if point._traffics[1]._client._ip != point._traffics[1]._server._ip:
            cnt += 1  # Not loopback
        return float(2 * self._config["bars"]["pps_bar"] - bone_results["tx_vport_rdma_unicast_packets"] - bone_results["rx_vport_rdma_unicast_packets"])

    def init_mutate_space(self):
        # Modify here
        # numa_node
        endhost_list = ["mr_num", "buf_num", "numa_node"]
        # endhost_list.append("use_gpu")
        traffic_list = ["process_num", "qp_num",
                        "mtu", "qp_type", "reqs", "recvs"]
        self._mutate_space = {"traffic": traffic_list.copy(),
                              "client": endhost_list.copy() + ["send_wq_depth", "send_batch"],
                              "server": endhost_list.copy() + ["recv_wq_depth", "recv_batch"]}

    def mutate_point(self, prev_point):
        point = copy.deepcopy(prev_point)
        traffic_id = random.randint(0, len(point._traffics) - 1)
        mutate_object = "_" + random.choice(list(self._mutate_space.keys()))
        # "_traffic" => [1:] (traffic)
        mutate_dim = "_" + random.choice(self._mutate_space[mutate_object[1:]])
        oldvalue = copy.deepcopy(point.get_dim_value(
            traffic_id, mutate_object, mutate_dim))
        point.mutate(traffic_id, mutate_object, mutate_dim)
        newvalue = copy.deepcopy(point.get_dim_value(
            traffic_id, mutate_object, mutate_dim))
        self._last_step = (mutate_object[1:],
                           mutate_dim[1:], oldvalue, newvalue)
        return point

    def get_point_from_record(self, records, target):
        best_point = None
        max_target_val = 0.0
        for record in records:
            if record[1][target] >= max_target_val:
                best_point = record[0]
                max_target_val = record[1][target]
        return best_point

    def sample(self, iters=10, record=False):
        point = Point(space=self._space, usr_A=self._usr_A, usr_B=self._usr_B,
                      ip_A=self._ip_A, ip_B=self._ip_B, ibdev_A=self._ibdev_A, ibdev_B=self._ibdev_B)
        ret = []
        for i in range(iters):
            # For Simulated Annealing, the only different is how point mutate.
            point.random()
            if (self._engine.clean_process()):
                return []
            if (self._engine.set_up_traffic(point)):
                continue
            bone_results = self._bonemon.monitor(self._bonedev_A)
            hw_results = self._hwmon.monitor(self._identity_A)
            log_result(self._log_path + "result/{}".format(self._global_log_idx),
                       point, bone_results, hw_results)
            log_reproduce(
                self._log_path + "reproduce/{}".format(self._global_log_idx), point, self._engine)
            self._global_log_idx += 1
            if self._bonemon.check_bone(bone_results):
                self.error_point(point, bone_results, hw_results)
            if record:
                ret.append(
                    [copy.deepcopy(point), hw_results.copy(), bone_results.copy()])
        self._engine.clean_process()
        return ret

    def random(self, iters=1000):
        point = Point(space=self._space, usr_A=self._usr_A, usr_B=self._usr_B,
                      ip_A=self._ip_A, ip_B=self._ip_B, ibdev_A=self._ibdev_A, ibdev_B=self._ibdev_B)
        point.random()
        self.init_mutate_space()
        logger.LOG("Random Approach Started", "BLOCK")
        for i in range(iters):
            point = self.mutate_point(point)
            logger.LOG("Randomly search point {}".format(self._global_log_idx))
            point.print_dict_log()
            if self._engine.clean_process():
                return -1
            if (self._engine.set_up_traffic(point)):
                continue
            bone_results = self._bonemon.monitor(self._bonedev_A)
            # Diagnostic counters version is not available for public
            # hw_results = self._hwmon.monitor(self._identity_A)
            hw_results = {}
            log_result(self._log_path + "result/{}".format(self._global_log_idx),
                       point, bone_results, hw_results)
            log_reproduce(
                self._log_path + "reproduce/{}".format(self._global_log_idx), point, self._engine)
            self._global_log_idx += 1
            if self._bonemon.check_bone(bone_results):
                self.error_point(point, bone_results, hw_results)
        return

    def simulated_annealing(self, iters=1000):
        self._mfs_engine = MinimalFeatureSet(
            self._engine, self._bonemon, self._space, self._bonedev_A, self._log_path)
        self._mfs_set = []
        point = Point(space=self._space, usr_A=self._usr_A, usr_B=self._usr_B,
                      ip_A=self._ip_A, ip_B=self._ip_B, ibdev_A=self._ibdev_A, ibdev_B=self._ibdev_B)
        point.random()
        prev_point = copy.deepcopy(point)
        prev_target_value = kPerfInitValue
        self.init_mutate_space()
        logger.LOG("Simulated Annealing Started", "BLOCK")
        anomaly_flag = kStageIterations
        temp = kStartTemp
        # Alpha and T decides the iterations. We can also set the "fixed" value
        for i in range(iters):
            if self._engine.clean_process():
                return -1
            if anomaly_flag <= 0:
                prev_point.random()
                point = copy.deepcopy(prev_point)
                prev_target_value = kPerfInitValue
                anomaly_flag = kStageIterations
            else:
                point = self.mutate_point(prev_point)
                # Attention: mutate_point does not modify prev_point
            # If the point matches found MFS, we jump out.
            while True:
                if self._mfs_engine.match_mfs(point):
                    point.random()  # TODO: shrink the search space to avoid redundant loop random iterations
                else:
                    prev_point = copy.deepcopy(point)
                    break
            if (self._engine.set_up_traffic(point)):
                continue
            bone_results = self._bonemon.monitor(self._bonedev_A)
            hw_results = {}
            # Diagnostic counters are now publicly available
            #hw_results = self._hwmon.monitor(self._identity_A)
            log_result(self._log_path + "result/{}".format(self._global_log_idx),
                       point, bone_results, hw_results)
            log_reproduce(
                self._log_path + "reproduce/{}".format(self._global_log_idx), point, self._engine)
            self._global_log_idx += 1
            anomaly_flag -= 1
            ret = self._bonemon.check_bone(bone_results)
            if ret != 0:
                self.error_point(point, bone_results, hw_results)
                # Because we add one before
                if ret == -1:
                    logger.LOG("An anomaly found. Starts to compute MFS...", "BLOCK")
                    self._mfs_engine.set_mfs_id(self._global_log_idx - 1)
                    mfs = self._mfs_engine.generate_mfs_from_point(
                        self._global_log_idx - 1, point)
                    if mfs != {}:
                        logger.LOG("MFS is generated: {}".format(mfs), "BLOCK")
                        self._mfs_set.append(mfs)
                anomaly_flag = -1
            # generate_mfs_from_point will clean processes before execute.
            # However, it may leave some processes. So we should clean right here.
            target_value = self.get_perf_bps(point, bone_results)
            if target_value >= prev_target_value:
                # We find a better or equal one
                prev_target_value = target_value
                prev_point = copy.deepcopy(point)
            else:
                delta = (prev_target_value - target_value) * \
                    1.0 / prev_target_value
                probability = delta * temp
                if (random.random() > probability):
                    prev_point = copy.deepcopy(point)
            # Temperature decreases. Maybe should be eliminated since we have multiple solutions.
            temp = temp * kAlpha
            logger.LOG(point.log_to_dict(), "INFO")
            print("")
            print("")
        self._engine.clean_process()
        return 0

    def diag_simulated_annealing(self, iters=1000):
        # Old version using diagnostic counters
        # 1% for random initialization
        random_iters = max(1, int(0.01 * iters))
        records = self.sample(iters=random_iters, record=True)
        targets = self.sort_target(records)
        logger.LOG("Sorted lists are {}".format(targets), "BLOCK")
        iter_per_target = int(iters / len(targets))
        self._mfs_engine = MinimalFeatureSet(
            self._engine, self._bonemon, self._space, self._bonedev_A, self._log_path)
        self._mfs_set = []
        for target in targets:
            self.set_target(target=target)
            # For each counter, we run iters
            prev_point = self.get_point_from_record(records, target)
            prev_target_value = 0.0
            self.init_mutate_space()
            logger.LOG("Simulated Annealing Started", "BLOCK")
            anomaly_flag = kStageIterations
            temp = kStartTemp
            for i in range(iter_per_target):
                if self._engine.clean_process():
                    return -1
                if anomaly_flag <= 0:
                    if anomaly_flag == 0:
                        logger.LOG(
                            "SA stage period encountered, start random sampling....")
                    new_random_records = self.sample(
                        iters=random_iters, record=True)
                    prev_point = self.get_point_from_record(
                        new_random_records, target)
                    point = copy.deepcopy(prev_point)
                    prev_target_value = 0.0
                    anomaly_flag = kStageIterations
                else:
                    point = self.mutate_point(prev_point)
                    # Attention: mutate_point does not modify prev_point
                # If the point matches found MFS, we jump out.
                while True:
                    if self._mfs_engine.match_mfs(point):
                        point.random()  # TODO: shrink the search space to avoid redundant loop random iterations
                        logger.LOG(
                            "DEBUG: MFS matched, pass this random point", "BLOCK")
                    else:
                        prev_point = copy.deepcopy(point)
                        break
                if (self._engine.set_up_traffic(point)):
                    continue
                bone_results = self._bonemon.monitor(self._bonedev_A)
                hw_results = self._hwmon.monitor(self._identity_A)
                log_result(self._log_path + "result/{}".format(self._global_log_idx),
                           point, bone_results, hw_results)
                log_reproduce(
                    self._log_path + "reproduce/{}".format(self._global_log_idx), point, self._engine)
                self._global_log_idx += 1
                anomaly_flag -= 1
                ret = self._bonemon.check_bone(bone_results)
                if ret != 0:
                    self.error_point(point, bone_results, hw_results)
                    # Because we add one before
                    if ret == -1:
                        logger.LOG("Starts to compute MFS", "BLOCK")
                        self._mfs_engine.set_mfs_id(self._global_log_idx - 1)
                        mfs = self._mfs_engine.generate_mfs_from_point(
                            self._global_log_idx - 1, point)
                        if mfs != {}:  # Not decided by all the features we suspect
                            self._mfs_set.append(mfs)
                    anomaly_flag = -1
                # generate_mfs_from_point will clean processes before execute.
                # However, it may leave some processes. So we should clean right here.
                target_value = self.get_target(hw_results)
                if target_value >= prev_target_value:
                    # We find a better or equal one
                    prev_target_value = target_value
                    prev_point = copy.deepcopy(point)
                else:
                    delta = (prev_target_value - target_value) * \
                        1.0 / prev_target_value
                    probability = delta * temp
                    if (random.random() > probability):
                        prev_point = copy.deepcopy(point)
                        logger.LOG("SA's shift is triggered")
                # Temperature decreases. Maybe should be eliminated since we have multiple solutions.
                temp = temp * kAlpha
                logger.LOG("Last step is {}-{}, from old value {} to new value {}".format(
                    self._last_step[0], self._last_step[1], self._last_step[2], self._last_step[3]), "WARNING")
                logger.LOG("Current Iteration, {} is {}, best target is {}".format(
                    target, target_value, prev_target_value), "WARNING")
                point.print_dict_log()
                # logger.WARNING(point.log_to_dict())
                print("")
        return 0


class MinimalFeatureSet(object):
    def __init__(self, engine: Engine, bonemon: bone.MlnxBoneMon, space: Space, bonedev: str, logpath: str):
        super(MinimalFeatureSet).__init__()
        self._current_set = []
        self._engine = engine
        self._space = space
        self._bonemon = bonemon
        self._bonedev = bonedev
        self._endhost_dim_space = ["mr_num", "buf_num",
                                   "send_wq_depth", "recv_wq_depth", "send_batch", "recv_batch"]
        self._traffic_dim_space = ["process_num",
                                   "qp_num", "mtu", "qp_type", "reqs", "recvs"]
        self._log_path = logpath

    def set_mfs_id(self, idx: int):
        self._id = idx
        cmdline = "mkdir -p {}".format(self._log_path +
                                       "result/mfs_help/{}".format(self._id))
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)
        cmdline = "mkdir -p {}".format(self._log_path +
                                       "reproduce/mfs_help/{}".format(self._id))
        print(cmdline)
        subprocess.check_output(cmdline, shell=True)

    def is_anomalous(self, point: Point, name=None):
        if self._engine.clean_process():
            logger.LOG("Generate MFS error: Clean before running err.", "ERROR")
        if self._engine.set_up_traffic(point):
            return -1
        result = self._bonemon.monitor(self._bonedev)
        ret = self._bonemon.check_bone(result)
        log_result(
            self._log_path + "result/mfs_help/{}/{}".format(self._id, name), point, result, {})
        log_reproduce(
            self._log_path + "reproduce/mfs_help/{}/{}".format(self._id, name), point, self._engine)
        if self._engine.clean_process():
            logger.LOG("Generate MFS error: Clean after running err.", "ERROR")
        if name != None:
            ret_to_err = {0: "Free of Anomaly",
                          -1: "Pause Frames",
                          -2: "Reduced Throughput"}
            if ret == 0:
                logger.LOG("[MFS Test] {}; result is {}".format(
                    name, ret_to_err[ret]), "BLOCK")
            else:
                logger.LOG("[MFS Test] {}; result is {}".format(
                    name, ret_to_err[ret]), "ERROR")
        return ret == -1

    def test_bounds_for_each(self, point: Point, traffic_idx: int, end_type: str,
                             attribute: str, lower_bound=None, upper_bound=None):
        # The attribute is without "_", e.g., "mr_num"
        # This function only work for generate_mfs_from_endhost().
        if end_type == "client" and "recv" in attribute:
            return {}
        if end_type == "server" and "send" in attribute:
            return {}
        test_point = copy.deepcopy(point)
        test_endhost = test_point.get_traffic(
            traffic_idx).__getattribute__("_" + end_type)
        bounds = self._space._bounds[attribute]
        value = test_endhost.__getattribute__("_" + attribute)
        # Lower bound test
        if upper_bound != None:
            upper_bound = min(upper_bound, bounds[1])
        else:
            upper_bound = bounds[1]
        if lower_bound != None:
            lower_bound = max(lower_bound, bounds[0])
        else:
            lower_bound = bounds[0]
        test_endhost.__setattr__("_"+attribute, lower_bound)
        lower_result = self.is_anomalous(
            test_point, str(traffic_idx) + "_" + end_type + "_" + attribute + ":lower_bound")
        test_endhost.__setattr__("_"+attribute, upper_bound)
        upper_result = self.is_anomalous(
            test_point, str(traffic_idx) + "_" + end_type + "_" + attribute + ":upper_bound")
        if lower_result and upper_result:
            # This dimension doesn't contribute, we simplified the point
            point.get_traffic(traffic_idx).__getattribute__(
                "_"+end_type).__setattr__("_"+attribute, lower_bound)
            print("DEBUG: point's {} traffic's {}'s {} is set to {}".format(
                traffic_idx, end_type, attribute, lower_bound))
            return {}
        if lower_result:
            return {attribute: (lower_bound, value)}
        if upper_result:
            return {attribute: (value, upper_bound)}
        return {attribute: (value, value)}

    def test_gpu(self, test_point: Point, traffic_idx: int) -> dict:
        test_traffic = test_point.get_traffic(traffic_idx)
        server_use_gpu = test_traffic._server._use_gpu
        client_use_gpu = test_traffic._client._use_gpu
        if client_use_gpu or server_use_gpu:
            test_traffic._server._use_gpu = False
            test_traffic._client._use_gpu = False
            if not self.is_anomalous(test_point, "Non-GPU"):
                test_traffic._server._use_gpu = server_use_gpu
                test_traffic._client._use_gpu = client_use_gpu
                return {"GDR": True}
        return {}

    def test_numa_node(self, test_point: Point, traffic_idx: int) -> dict:
        test_traffic = test_point.get_traffic(traffic_idx)
        if (test_traffic._server._numa_node == self._space._best_numa_node[test_traffic._server._ip] and
                test_traffic._client._numa_node == self._space._best_numa_node[test_traffic._client._ip]):
            return {}
        server_numa_node_ori = test_traffic._server._numa_node
        client_numa_node_ori = test_traffic._client._numa_node
        test_traffic._server._numa_node = self._space._best_numa_node[test_traffic._server._ip]
        test_traffic._client._numa_node = self._space._best_numa_node[test_traffic._client._ip]
        if not self.is_anomalous(test_point, "NUMA:Good"):
            test_traffic._server._numa_node = server_numa_node_ori
            test_traffic._client._numa_node = client_numa_node_ori
            return {"NUMA": True}
        return {}

    def generate_mfs_from_endhost(self, test_point: Point, traffic_idx: int) -> dict:
        ret_mfs = {}
        test_traffic = test_point.get_traffic(traffic_idx)
        for key in ["client", "server"]:
            mfs = {}
            test_endhost = test_traffic.__getattribute__("_" + key)
            test_attributes = ["mr_num", "buf_num", "send_batch", "recv_batch"]
            for attribute in test_attributes:
                tmp_result = self.test_bounds_for_each(
                    test_point, traffic_idx, key, attribute)
                mfs = {**mfs, **tmp_result}
            tmp_result = self.test_bounds_for_each(test_point, traffic_idx, key,
                                                   "send_wq_depth", lower_bound=test_endhost._send_batch)
            mfs = {**mfs, **tmp_result}
            tmp_result = self.test_bounds_for_each(test_point, traffic_idx, key,
                                                   "recv_wq_depth", lower_bound=test_endhost._recv_batch)
            mfs = {**mfs, **tmp_result}
            ret_mfs[key] = mfs.copy()
        return ret_mfs

    def test_optimal_for_each(self, point: Point, traffic_idx: int,
                              attribute: str, lower_bound):
        test_point = copy.deepcopy(point)
        traffic = point.get_traffic(traffic_idx)

        test_traffic = test_point.get_traffic(traffic_idx)
        value = test_traffic.__getattribute__("_"+attribute)
        upper_bound = self._space._bounds[attribute][1]
        if value > lower_bound:
            test_traffic.__setattr__("_"+attribute, lower_bound)
            if not self.is_anomalous(test_point, "{}_{}".format(traffic_idx, attribute)):
                return {attribute: (value, upper_bound)}
        # This attribute doesn't contribute, we set it to the optimal (lower_bound)
        traffic.__setattr__("_"+attribute, lower_bound)
        return {}

    def test_qp_type(self, point: Point, traffic_idx: int):
        test_point = copy.deepcopy(point)
        test_traffic = test_point.get_traffic(traffic_idx)
        traffic = point.get_traffic(traffic_idx)
        has_read = False
        has_write = False
        for req in traffic._reqs:
            if "r" in req:
                has_read = True
            if "w" in req:
                has_write = True
        test_types = [0, 1, 2]
        test_types.remove(traffic._qp_type)
        mfs = {}
        for test_type in test_types:
            # Three conditions that won't allow UD to test:
            # 1. Large requests/receives
            # 2. Contains read or 3. write requests
            if test_type == 0 and (has_read or has_write or
                                   traffic._max_req_size > MTU_TO_REQ[traffic._mtu] or traffic._max_recv_size > MTU_TO_REQ[traffic._mtu]):
                continue
            if test_type == 1 and has_read:
                continue
            test_traffic._qp_type = test_type
            if not self.is_anomalous(test_point, "{}_qp_type:{}".format(traffic_idx, QP_TO_NAME[test_type])):
                mfs["qp_type"] = traffic._qp_type
        return mfs

    # This test_opcode_type is based on static_opcode=True situations.
    # For those traffic with mix opcodes, we simply shape them into the same type.
    def test_opcode_type(self, point: Point, traffic_idx: int):
        test_point = copy.deepcopy(point)
        test_traffic = test_point.get_traffic(traffic_idx)
        traffic = point.get_traffic(traffic_idx)
        has = [False, False, False]  # send, write, read
        tests = [False, False, False]  # send, write, read
        for req in traffic._reqs:
            if "r" in req:
                has[2] = True
            if "w" in req:
                has[1] = True
            if "s" in req:
                has[0] = True

        if has[2]:
            # must be RC so we test with write and send
            test_traffic.shape_reqs_type("w")
            if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=write"):
                return {"opcode": "r"}
            tests[1] = True
            test_traffic.shape_reqs_type("s")
            if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=send"):
                return {"opcode": "r"}
            tests[0] = True
        if has[1]:
            # UC/RC using write
            if not tests[0]:
                test_traffic.shape_reqs_type("s")
                if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=send"):
                    return {"opcode": "w"}
            if not tests[2] and traffic._qp_type == 2:
                # haven't tested yet and is RC, we can test read
                test_traffic.shape_reqs_type("r")
                if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=read"):
                    return {"opcode": "w"}
        if has[0]:
            # UD/UC/RC using send
            if not tests[1] and traffic._qp_type > 0:
                test_traffic.shape_reqs_type("w")
                if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=write"):
                    return {"opcode": "s"}
            if not tests[2] and traffic._qp_type == 2:
                test_traffic.shape_reqs_type("r")
                if not self.is_anomalous(test_point, str(traffic_idx) + "_opcode=read"):
                    return {"opcode": "s"}
        return {}

    def test_sges(self, test_point: Point, traffic_idx: int):
        test_traffic = test_point.get_traffic(traffic_idx)
        origin_reqs = copy.deepcopy(test_traffic._reqs)
        sge_num = test_traffic.standard_reqs_sge()
        ret = {}
        if not self.is_anomalous(test_point, str(traffic_idx) + "_req-sge=1"):
            ret = {"req-sge": sge_num}
            test_traffic._reqs = origin_reqs  # restore to test another
        origin_recvs = copy.deepcopy(test_traffic._recvs)
        sge_num = test_traffic.standard_recvs_sge()
        if not self.is_anomalous(test_point, str(traffic_idx) + "_recv-sge=1"):
            ret = {"recv-sge": sge_num}
            test_traffic._recvs = origin_recvs
        return ret

    def test_reqs_size(self, test_point: Point, traffic_idx: int):
        # if the requests size doesn't make things worse. We should let it be more "mild"
        test_traffic = test_point.get_traffic(traffic_idx)
        origin_reqs = copy.deepcopy(test_traffic._reqs)
        origin_buf = copy.deepcopy(test_traffic._client._buf_size)
        small_size = self._space._bounds["req_size"][0]
        large_size = min(test_traffic._server._buf_size,
                         test_traffic._client._buf_size)
        test_traffic._client._buf_size = small_size
        test_traffic.standard_reqs_size(small_size)
        pattern = 0
        if not self.is_anomalous(test_point, str(traffic_idx) + "_reqs={}".format(small_size)):
            lower_bound = test_traffic._max_req_size
            test_traffic._reqs = origin_reqs
            test_traffic._client._buf_size = origin_buf
            pattern += 1
        else:
            lower_bound = small_size

        test_traffic._client._buf_size = large_size
        test_traffic.standard_reqs_size(large_size)
        if not self.is_anomalous(test_point, str(traffic_idx) + "_reqs={}".format(large_size)):
            upper_bound = test_traffic._max_req_size
            test_traffic._reqs = origin_reqs
            test_traffic._client._buf_size = origin_buf
            pattern += 1
        else:
            upper_bound = self._space._bounds["req_size"][1]
        mix = False
        if pattern == 2:
            mix = True
        if pattern == 0:
            test_traffic.standard_reqs_size(large_size)
            test_traffic._client._buf_size = large_size
            return {}
        return {"reqs-size": (lower_bound, upper_bound), "req-mix": mix}

    def test_recvs_size(self, test_point: Point, traffic_idx: int):
        test_traffic = test_point.get_traffic(traffic_idx)
        origin_recvs = copy.deepcopy(test_traffic._recvs)
        origin_buf = copy.deepcopy(test_traffic._server._buf_size)

        small_size = test_traffic.get_cur_max_req()
        large_size = 2 * small_size
        # test_traffic._server._buf_size

        test_traffic._server._buf_size = small_size
        test_traffic.standard_recvs_size(small_size)
        pattern = 0
        if not self.is_anomalous(test_point, str(traffic_idx) + "_recvs={}".format(small_size)):
            lower_bound = test_traffic._max_recv_size
            test_traffic._recvs = origin_recvs
            test_traffic._server._buf_size = origin_buf
            pattern += 1
        else:
            lower_bound = small_size

        test_traffic._server._buf_size = large_size
        test_traffic.standard_recvs_size(large_size)
        if not self.is_anomalous(test_point, str(traffic_idx) + "_recvs={}".format(large_size)):
            upper_bound = test_traffic._max_recv_size
            test_traffic._recvs = origin_recvs
            test_traffic._server._buf_size = origin_buf
            pattern += 1
        else:
            upper_bound = large_size
        mix = False
        if pattern == 2:
            mix = True
        if pattern == 0:
            test_traffic.standard_recvs_size(large_size)
            test_traffic._server._buf_size = large_size
            return {}
        return {"recvs-size": (lower_bound, upper_bound), "recv-mix": mix}

    def generate_mfs_from_traffic(self, test_point: Point) -> dict:
        # Attention: The point only has one traffic in its traffics
        ret_mfs = {}
        traffic_num = test_point.get_traffic_nums()
        print("The point has {} traffics".format(traffic_num))
        # Here is the trick: we modify each dim of each traffic
        # If the dim does not contribute to anomaly (not is_anomaly()),
        # then we let the dim be simplified. So, we need to set test_point out of the loop,
        # to make sure those changed dimensions remain effective.
        for i in range(traffic_num):
            print("Generating MFS for {} th traffic".format(i))
            mfs = {}
            test_traffic = test_point.get_traffic(i)
            test_attributes = ["process_num", "qp_num"]
            for attribute in test_attributes:
                tmp_result = self.test_optimal_for_each(
                    test_point, i, attribute, self._space._bounds[attribute][0])
                mfs = {**mfs, **tmp_result}
            # NUMA test
            tmp_result = self.test_numa_node(test_point, i)
            mfs = {**mfs, **tmp_result}
            # Opcode Test
            tmp_result = self.test_opcode_type(test_point, i)
            mfs = {**mfs, **tmp_result}
            # QP Type Test
            tmp_result = self.test_qp_type(test_point, i)
            mfs = {**mfs, **tmp_result}
            # Sge batching test
            tmp_result = self.test_sges(test_point, i)
            mfs = {**mfs, **tmp_result}
            # Request pattern test
            tmp_result = self.test_reqs_size(test_point, i)
            mfs = {**mfs, **tmp_result}
            # Receive pattern test
            tmp_result = self.test_recvs_size(test_point, i)
            mfs = {**mfs, **tmp_result}
            # MTU
            mtu_set = [3, 4, 5]
            ori_mtu = test_traffic._mtu
            mtu_set.remove(test_traffic._mtu)
            for mtu in mtu_set:
                test_traffic._mtu = mtu
                if not self.is_anomalous(test_point, "{}_mtu_{}".format(i, mtu)):
                    test_traffic._mtu = ori_mtu
                    mfs["mtu"] = (ori_mtu, ori_mtu)
                    break
            endhost = self.generate_mfs_from_endhost(test_point, i)
            ret_mfs[i] = {**mfs, **endhost}
        # The simplified test_point is right here.
        test_point.print_dict_log("WARNING")
        self._engine.log_scripts(
            test_point, self._log_path + "reproduce/simplifed_{}".format(self._id))
        if self._engine.clean_process():
            logger.LOG(
                "REDO simplified MFS error: Clean before running err.", "ERROR")
            return {}
        if self._engine.set_up_traffic(test_point):
            logger.LOG("REDO simplified MFS error: Setup err.", "ERROR")
            self._engine.clean_process()
            return {}
        result = self._bonemon.monitor(self._bonedev)
        if not self._bonemon.check_bone(result):
            # No anomaly. This can be sometimes due to false positive(setup/kill) err
            return {}
        log_result(
            self._log_path + "result/simplified_{}".format(self._id), test_point, result, {})
        return ret_mfs

    def get_size_of_mfs(self, mfs: dict) -> int:
        size_of_mfs = 0
        for key in mfs:
            for k in mfs[key]:
                if k == "client" or k == "server":
                    size_of_mfs += len(list(mfs[key][k].keys()))
                else:
                    size_of_mfs += 1
        return size_of_mfs

    def log_mfs(self, idx: int, mfs: dict) -> None:
        self._current_set.append(mfs)
        with open(self._log_path + "result/mfs.txt", "a") as f:
            f.write("{}: ".format(idx))
            json.dump(mfs, f)
            f.write("\n\n")
        logger.LOG("MFS logged to result/mfs.txt")

    def generate_mfs_from_point(self, idx: int, point: Point) -> dict:
        mfs = {}
        test_point = copy.deepcopy(point)
        # Direction: forward, or backward
        # Only Forward(0) Traffic
        test_point._traffics = [copy.deepcopy(point.get_traffic(0))]
        if self.is_anomalous(test_point, "Forward(0)"):
            # Check with Forward(0) Traffic, that would be enough
            logger.LOG(
                "No loopback or bi-directional traffic is enough", mode="BLOCK")
            mfs = self.generate_mfs_from_traffic(test_point)
            size_of_mfs = self.get_size_of_mfs(mfs)
            if size_of_mfs > 0:
                self.log_mfs(idx, mfs)
                return mfs
            return {}

        test_point._traffics = [copy.deepcopy(point.get_traffic(1))]
        if test_point._traffics[0]._client._ip != test_point._traffics[0]._server._ip:
            # Backward
            logger.LOG("Test Backward traffic. (No loopback)", mode="BLOCK")
            if self.is_anomalous(test_point, "Backward(1)"):
                # Check with Backward(1) Traffic, that would be enough
                mfs = self.generate_mfs_from_traffic(test_point)
                size_of_mfs = self.get_size_of_mfs(mfs)
                if size_of_mfs > 0:
                    self.log_mfs(idx, mfs)
                    return mfs
                return {}

        # need to check with both Tx & Rx traffic
        test_point._traffics = copy.deepcopy(point._traffics)
        mfs = self.generate_mfs_from_traffic(test_point)
        size_of_mfs = self.get_size_of_mfs(mfs)
        if size_of_mfs > 0:
            if test_point._traffics[1]._client._ip == test_point._traffics[1]._server._ip:
                mfs[1]["loopback"] = True
            else:
                mfs[1]["bidirectional"] = True
            self.log_mfs(idx, mfs)
            return mfs
        return {}

    def _mfs_match_reqs_size(self, reqs, match_reqs, mix):
        lower_bound = int(match_reqs[0])
        upper_bound = int(match_reqs[1])
        # _reqs = "s_2_608_192,s_3_640_192_64,s_2_1408_384"
        # In reality, we find that split by MTU is enough.. RDMA NIC is so fragile...
        small = False
        large = False
        fail = False
        for request in reqs:
            req_size = 0
            sizes = request.split('_')
            for i in range(len(sizes) - 2):
                req_size += int(sizes[i+2])
            # We need all small or all large
            if req_size > upper_bound or req_size < lower_bound:
                fail = True
            if mix:
                if req_size < 1024:
                    small = True
                else:
                    large = True
        if mix:
            return not (small and large)
        return not fail

    def _mfs_match_recvs_size(self, recvs, match_recvs, mix):
        lower_bound = int(match_recvs[0])
        upper_bound = int(match_recvs[1])
        # In reality, we find that split by MTU is enough.. RDMA NIC is so fragile...
        small = False
        large = False
        for receive in recvs:
            recv_size = 0
            sizes = receive.split('_')
            for i in range(len(sizes) - 1):
                recv_size += int(sizes[i+1])
            # We need all small or all large
            if recv_size > upper_bound or recv_size < lower_bound:
                return False
            if mix:
                if recv_size < 1024:
                    small = True
                else:
                    large = True
        if mix:
            return not (small and large)
        return True

    def _mfs_match_opcode(self, reqs, opcode):
        for req in reqs:
            if req.startswith(opcode):
                return True
        return False

    def _mfs_match_qptype(self, traffic: Traffic, qp_type):
        if traffic._qp_type == qp_type:
            return True
        return False

    def _mfs_match_endhost(self, endhost: Endhost, mfs_endhost: dict):
        for key in mfs_endhost:
            lower_bound = mfs_endhost[key][0]
            upper_bound = mfs_endhost[key][1]
            val = endhost.__getattribute__("_"+key)
            if val < lower_bound or val > upper_bound:
                return False
        return True

    def _mfs_match_recv_sge(self, recvs: str, mfs_sge: int):
        for i in range(len(recvs)-1):
            value = int(recvs[i].split('_')[0])
            if value < mfs_sge:
                return False
        return True

    def _mfs_match_req_sge(self, reqs: str, mfs_sge: int):
        for i in range(len(reqs)-1):
            value = int(reqs[i].split('_')[1])
            if value < mfs_sge:
                return False
        return True

    def _mfs_match_gdr(self, traffic: Traffic):
        if traffic._server._use_gpu or traffic._client._use_gpu:
            return True
        return False

    def _mfs_match_numa(self, traffic: Traffic):
        if traffic._server._numa_node != self._space._best_numa_node[traffic._server._ip]:
            return True
        if traffic._client._numa_node != self._space._best_numa_node[traffic._client._ip]:
            return True
        return False

    def _mfs_match_traffic(self, traffic: Traffic, mfs_traffic: dict):
        for K in mfs_traffic:
            if K in ["client", "server"]:
                if not self._mfs_match_endhost(traffic.__getattribute__("_"+K), mfs_traffic[K]):
                    return False
            else:
                if K == "reqs-size":
                    if not self._mfs_match_reqs_size(traffic._reqs, mfs_traffic[K], mfs_traffic["req-mix"]):
                        return False
                elif K == "recvs-size":
                    if not self._mfs_match_recvs_size(traffic._recvs, mfs_traffic[K], mfs_traffic["recv-mix"]):
                        return False
                elif K == "opcode":
                    if not self._mfs_match_opcode(traffic._reqs, mfs_traffic[K]):
                        return False
                elif "mix" in K:  # Done in reqs
                    continue
                elif K == "recv-sge":
                    mfs_sge = int(mfs_traffic[K])
                    if not self._mfs_match_recv_sge(traffic._recvs, mfs_sge):
                        return False
                elif K == "req-sge":
                    mfs_sge = int(mfs_traffic[K])
                    if not self._mfs_match_req_sge(traffic._reqs, mfs_sge):
                        return False
                elif K == "NUMA":
                    if not self._mfs_match_numa(traffic):
                        return False
                elif K == "qp_type":
                    if not self._mfs_match_qptype(traffic, mfs_traffic[K]):
                        return False
                elif K == "GDR":
                    if not self._mfs_match_gdr(traffic):
                        return False
                elif K == "loopback":
                    if traffic._client._ip != traffic._server._ip:
                        return False
                elif K == "bidirectional":
                    if traffic._client._ip == traffic._server._ip:
                        return False
                else:
                    lower_bound = int(mfs_traffic[K][0])
                    upper_bound = int(mfs_traffic[K][1])
                    value = int(traffic.__getattribute__("_"+K))
                    if value < lower_bound or value > upper_bound:
                        return False
        return True

    def _match_mfs(self, point: Point, mfs: dict) -> bool:
        match = [False, False, False, False]
        # Forward-forward, backward-backward
        to_match = len(mfs.keys())
        cnt = 0
        for mfs_idx in mfs:
            for traffic in point._traffics:
                if self._mfs_match_traffic(traffic, mfs[mfs_idx]):
                    match[cnt] = True
                cnt += 1
        if to_match == 1:
            return match[0] or match[1]
        if to_match == 2:
            return (match[0] or match[1]) and (match[2] or match[3])

    def match_mfs(self, point) -> bool:
        for tomatch in self._current_set:
            if self._match_mfs(point, tomatch):
                return True
        return False
