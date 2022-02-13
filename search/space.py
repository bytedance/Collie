# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information



import random
import subprocess
import logger

MTU_TO_REQ = {3: 1024, 4: 2048, 5: 4096}
OPCODE_TO_OP = {0: 's', 1: "w", 2: 'r'}
TAG_TO_QPTYPE = {0: 4, 1: 3, 2: 2}
QP_TO_NAME = {0: "UD", 1: "UC", 2: "RC"}


def shape_reqs_recv(req: int, align=32):
    return int((req + align - 1) / align) * align


class Space:
    '''
        The search space described in Collie.
        As described in paper, we construct the search space based on how verbs APIs are called.
        The space consists of three parts (four dims):
        (1) MR related: Device like NUMA/GPU (Dim 1), number/size of MR and alignment (Dim 2)
        (2) QP related: number of connections, mtu, WQE batching, SG batching, WQ depth, etc. (Dim 3)
        (3) Traffic related: request vector, opcode.

        There contains dependency and relationship among these parameters that should be carefully handled:
        e.g., QP type decides opcode and request size of the request vector.

        Param (1) decides a memory pool for RDMA. However, how user access the memory also plays a role.
        We randomly access a piece of this memory pool each time (to maximize the pressure we push to the RNIC)
        TODO[low priority]: a parameterized memory pick mechanism.
    '''

    def __init__(self, usr_A, usr_B, ip_A, ip_B, ibdev_A, ibdev_B, A_numarange=(0, 1), B_numarange=(0, 1),
                 use_gpu=False, A_cudarange=(0, 0), B_cudarange=(0, 0), filename=None):
        if filename == None:
            # No configuration file is designated
            self._bounds = {}
            # Which NUMA to use
            # Modify NUMA range here
            self._bounds["numa_node"] = A_numarange
            self._bounds["{}_numa_node".format(ip_A)] = A_numarange
            self._bounds["{}_numa_node".format(ip_B)] = B_numarange
            self._bounds["use_gpu"] = (0, 1)
            # Number of processes for each side traffic.
            self._bounds["process_num"] = (4, 6)
            # MTU
            self._bounds["mtu"] = (3, 5)
            # QP Type
            self._bounds["qp_type"] = (0, 2)
            # QP number that single CPU handles
            self._bounds["qp_num"] = (1, 4)
            # MR number for each side, and for send/recv
            self._bounds["mr_num"] = (1, 4)
            self._bounds["buf_num"] = (1, 4)
            # We don't take the buf_size as variable.
            # mr_size = buf_num * buf_size (where buf_size <- req_size & recv_size)
            # total memory used = mr_size * mr_num
            self._bounds["req_length"] = (1, 8)
            self._bounds["recv_length"] = (1, 4)
            self._bounds["req_size"] = (32, 4096 * 32)
            self._bounds["recv_size"] = (32, 4096 * 32)
            # WQE batching size and SG batching size
            self._bounds["send_batch"] = (1, 64)
            self._bounds["recv_batch"] = (1, 64)
            self._bounds["send_sge_batch_size"] = (1, 4)
            self._bounds["recv_sge_batch_size"] = (1, 4)
            # work queue depth
            self._bounds["send_wq_depth"] = (1, 1024)
            self._bounds["recv_wq_depth"] = (1, 1024)
            self._best_numa_node = {ip_A: self.update_numa(
                usr_A, ip_A, ibdev_A), ip_B: self.update_numa(usr_B, ip_B, ibdev_B)}
        else:
            pass

    def update_numa(self, usr, ip, ibdev):
        cmd = ["ssh", "{}@{}".format(usr, ip),
               "cat", "/sys/class/infiniband/{}/device/numa_node".format(ibdev)]
        try:
            best_numa_node = int(
                subprocess.check_output(cmd).decode().rstrip('\n'))
            return best_numa_node
        except Exception as e:
            print(e)
            return 0

    def randint(self, key):
        ret = -1
        try:
            ret = random.randint(self._bounds[key][0], self._bounds[key][1])
        except Exception as e:
            print(e)
        return ret


class Endhost(object):
    '''
        Endhost describes endpoints of the traffic.
        Traffic from host A to host B.
        then we use two endhosts to describe A & B.
        e.g., mr_num, buf_size and buf_num
    '''

    def __init__(self, space: Space, usr, ip, ibdev):
        super(Endhost).__init__()
        self._space = space
        self._ip = str(ip)
        self._numa_node = 0
        self._mr_num = 1
        self._buf_num = 1
        self._buf_size = 65536
        self._send_wq_depth = 1
        self._recv_wq_depth = 1
        self._send_batch = 1
        self._recv_batch = 1
        self._use_gpu = False
        self._gpu_id = 0  # Should be the right connected GPU to the RNIC. e.g., PIX/PXB
        self._ibdev = ibdev
        self._usr = usr

    def random(self, max_req_size):
        self._numa_node = self._space.randint("{}_numa_node".format(self._ip))
        self._mr_num = self._space.randint("mr_num")
        self._buf_num = self._space.randint("buf_num")
        self._buf_size = max(max_req_size, 4096)
        self._send_wq_depth = self._space.randint("send_wq_depth")
        self._recv_wq_depth = self._space.randint("recv_wq_depth")
        self._send_batch = self._space.randint("send_batch")
        self._recv_batch = self._space.randint("recv_batch")

    def to_cmd(self):
        cmd = "--dev={} --mr_num={} --buf_num={} --buf_size={} --send_batch={} --recv_batch={} --send_wq_depth={} --recv_wq_depth={} ".format(
            self._ibdev, self._mr_num, self._buf_num, self._buf_size, self._send_batch, self._recv_batch, self._send_wq_depth, self._recv_wq_depth
        )
        return cmd

    def get_numa(self):
        return self._numa_node

    def get_ip(self):
        return self._ip

    def display(self):
        print("Endhost ip is {}@{}".format(self._usr, self._ip))
        print("Device to use {}".format(self._ibdev))
        print("Use GPU {}".format(self._use_gpu))
        print("NUMA to use {}".format(self._numa_node))
        print("Number of mr {}".format(self._mr_num))
        print("Number of buf {}".format(self._buf_num))
        print("Size of each MR {}".format(self._buf_size))
        print("Send WQ depth is {}".format(self._send_wq_depth))
        print("Receive WQ depth is {}".format(self._recv_wq_depth))
        print("Send WQE batch is {}".format(self._send_batch))
        print("Receive WQE batch is {}".format(self._recv_batch))
        print("")

    def log_to_lists(self):
        log = []
        log.append(
            "Endhost ip is        : {}@{}".format(self._usr, self._ip))
        log.append("Device to use        : {}".format(self._ibdev))
        log.append("Use GPU              : {}".format(self._use_gpu))
        log.append("NUMA to use          : {}".format(self._numa_node))
        log.append("Number of mr         : {}".format(self._mr_num))
        log.append("Number of buf        : {}".format(self._buf_num))
        log.append("Size of each MR      : {}".format(self._buf_size))
        log.append("Send WQ depth is     : {}".format(self._send_wq_depth))
        log.append("Recv WQ depth is     : {}".format(self._recv_wq_depth))
        log.append("Send WQE batch is    : {}".format(self._send_batch))
        log.append("RecvWQE batch is     : {}".format(self._recv_batch))
        log.append("")
        return log

    def log_to_dict(self):
        log = {"usr": self._usr,
               "ip": self._ip,
               "ibdev": self._ibdev,
               "use_gpu": self._use_gpu,
               "numa_node": self._numa_node,
               "mr_num": self._mr_num,
               "buf_num": self._buf_num,
               "buf_size": self._buf_size,
               "send_wq_depth": self._send_wq_depth,
               "recv_wq_depth": self._recv_wq_depth,
               "send_batch": self._send_batch,
               "recv_batch": self._recv_batch,
               }
        return log


class Traffic(object):
    '''
        Traffic basically describe the QPs between A and B.
        As well as how each QP generate request to the other end.
    '''

    def __init__(self, space: Space, client_usr, server_usr,
                 client_ip, server_ip,
                 client_dev, server_dev, static_opcode=True):
        super(Traffic).__init__()
        self._space = space
        self._client = Endhost(
            space, usr=client_usr, ip=client_ip, ibdev=client_dev)
        self._server = Endhost(
            space, usr=server_usr, ip=server_ip, ibdev=server_dev)
        self._process_num = 1
        self._qp_num = 1
        self._mtu = 3
        self._qp_type = 2
        self._req_length = 1
        self._recv_length = 1
        self._max_req_size = 65536
        self._reqs = ["s_1_65536"]
        self._recvs = ["1_65536"]
        self._static_opcode = static_opcode
        self._opcode = 's'

    def shape_reqs_type(self, type: str):
        if len(type) > 1:
            print("Error: shape type should be one character (w/s/r)")
            return
        for i in range(len(self._reqs)):
            self._reqs[i] = type + self._reqs[i][1:]

    def standard_reqs_sge(self):
        l = len(self._reqs)
        ret_sge_num = 0
        for i in range(l):
            req_size = 0
            reqs = self._reqs[i].split('_')
            opcode = reqs[0]
            sge_num = int(reqs[1])
            ret_sge_num = max(int(sge_num), int(ret_sge_num))
            for j in range(sge_num):
                req_size += int(reqs[j+2])
            self._reqs[i] = "{}_1_{}".format(opcode, req_size)
        return ret_sge_num

    def standard_recvs_sge(self):
        l = len(self._recvs)
        ret_sge_num = 0
        for i in range(l):
            recv_size = 0
            recvs = self._recvs[i].split('_')
            sge_num = int(recvs[0])
            ret_sge_num = max(int(sge_num), int(ret_sge_num))
            for j in range(sge_num):
                recv_size += int(recvs[j+1])
            self._recvs[i] = "1_{}".format(recv_size)
        return ret_sge_num

    def standard_reqs_size(self, size):
        if self._qp_type == 0:  # UD
            size = min(size, MTU_TO_REQ[self._mtu])
        self._reqs = ["{}_1_{}".format(self._opcode, size)]

    def standard_recvs_size(self, size):
        self._recvs = ["1_{}".format(size)]

    def get_cur_max_req(self):
        max_size = 0
        for req in self._reqs:
            sg_sizes = req.split('_')
            size = 0
            for i in range(len(sg_sizes) - 2):
                size += int(sg_sizes[i+2])
            max_size = max(max_size, size)
        return max_size

    def get_cur_max_recv(self):
        max_size = 0
        for recv in self._recvs:
            sg_sizes = recv.split('_')
            size = 0
            for i in range(len(sg_sizes) - 1):
                size += int(sg_sizes[i+1])
            max_size = max(max_size, size)
        return max_size
    # Request is like: we want to post n1, n2, n3... size message to remote end.
    # Each n1 should be a continuous buffer.
    # We can post several ni in a sge_batch, and several sge in a wqe_batch
    # So the size of buffer should be max(C_N_M n_i) <= M * n_i

    def random_reqs(self):
        self._reqs.clear()
        max_req_size = 0
        req_size_upper_bound = self._space._bounds["req_size"][1]
        if self._qp_type == 0:
            req_size_upper_bound = MTU_TO_REQ[self._mtu]
        opcode = random.randint(0, self._qp_type)
        self._opcode = OPCODE_TO_OP[opcode]
        for i in range(self._req_length):
            if not self._static_opcode:
                opcode = random.randint(0, self._qp_type)
            align = 32
            req_size = shape_reqs_recv(random.randint(self._space._bounds["req_size"][0],
                                                      req_size_upper_bound), align=align)
            # An implicit requirement is taht the req_size_upper_bound should be aligned with
            # alignment (default: 32)
            max_req_size = max(max_req_size, req_size)
            partition = req_size / align
            num_sge = int(min(self._space.randint(
                "send_sge_batch_size"), partition))

            req_str = "{}_{}".format(OPCODE_TO_OP[opcode], num_sge)
            for j in range(num_sge - 1):
                try:
                    sg_partition = random.randint(
                        1, partition - (num_sge - j - 1))
                except Exception as e:
                    print(e)
                    print("num_sge = {}, partition = {}, j = {}".format(
                        num_sge, partition, j))
                sg_size = align * sg_partition
                partition -= sg_partition
                req_size -= sg_size
                req_str += '_{}'.format(sg_size)
            req_str += '_{}'.format(req_size)
            self._reqs.append(req_str)
        self._max_req_size = max_req_size
        return max_req_size

    def random_recvs(self):
        self._recvs.clear()
        max_req_size = self._max_req_size
        max_recv_size = 0
        for i in range(self._recv_length):
            align = 32
            recv_size = shape_reqs_recv(
                self._space.randint("recv_size"), align=align)
            recv_size = max(recv_size, max_req_size)
            max_recv_size = max(max_recv_size, recv_size)
            partition = recv_size / align
            num_sge = int(min(self._space.randint(
                "recv_sge_batch_size"), partition))
            recv_str = "{}".format(num_sge)
            for j in range(num_sge - 1):
                sg_partition = random.randint(1, partition - (num_sge - j - 1))
                sg_size = align * sg_partition
                partition -= sg_partition
                recv_size -= sg_size
                recv_str += "_{}".format(sg_size)
            recv_str += "_{}".format(recv_size)
            self._recvs.append(recv_str)
        self._max_recv_size = max_recv_size
        return max_recv_size

    def random(self):
        self._process_num = self._space.randint("process_num")
        self._qp_num = self._space.randint("qp_num")
        self._mtu = self._space.randint("mtu")
        self._qp_type = self._space.randint("qp_type")
        self._req_length = self._space.randint("req_length")
        self._recv_length = self._space.randint("recv_length")
        self._reqs = []
        self._recvs = []
        max_req_size = self.random_reqs()
        max_recv_size = self.random_recvs()
        self._client.random(max_req_size)
        # Client won't receive
        self._client._recv_batch = 0
        self._client._recv_wq_depth = 1
        # Server won't send
        self._server.random(max_recv_size)
        self._server._send_batch = 0
        self._server._send_wq_depth = 1
        return max_req_size

    def mutate_req_recv(self):
        self.random_reqs()
        self.random_recvs()
        self._client._buf_size = max(self._max_req_size, 4096)
        self._server._buf_size = max(self._max_recv_size, 4096)

    def req_to_str(self, reqs: list):
        req_str = ""
        for req in reqs:
            req_str += req + ","
        req_str.rstrip(',')
        return req_str

    def to_cmd(self):
        req_str = self.req_to_str(self._reqs)
        recv_str = self.req_to_str(self._recvs)
        cmd = "--qp_num={} --mtu={} --qp_type={} --request={} --receive={}".format(
            self._qp_num, self._mtu, TAG_TO_QPTYPE[self._qp_type], req_str, recv_str
        )
        return cmd

    def get_numqps(self):
        if self._client._ip == self._server._ip:
            return 2 * int(self._qp_num * self._process_num)
        return int(self._qp_num * self._process_num)

    def get_server(self):
        return self._server

    def get_client(self):
        return self._client

    def get_process_num(self):
        return self._process_num

    def display(self):
        self._client.display()
        self._server.display()
        print("There are {} processes".format(self._process_num))
        print("The number of QP is {}".format(self._qp_num))
        print("The MTU is {}".format(MTU_TO_REQ[self._mtu]))
        print("The qp type is {}".format(QP_TO_NAME[self._qp_type]))
        print("The sending requests look like: {}".format(self._reqs))
        print("The receiving requests look like: {}".format(self._recvs))
        print("")

    def log_to_lists(self):
        log = []
        log.append("Number of processes: {}".format(self._process_num))
        log.append("Number of QP       : {}".format(self._qp_num))
        log.append("MTU size is        : {}".format(
            MTU_TO_REQ[self._mtu]))
        log.append("QP type is         : {}".format(
            QP_TO_NAME[self._qp_type]))
        log.append("Requests Pattern   :")
        for req in self._reqs:
            log.append("            {}".format(req))
        log.append("Receive Pattern    :")
        for recv in self._recvs:
            log.append("            {}".format(recv))
        log.append("")
        log.append("Client Configuration: ")
        log += self._client.log_to_lists()
        log.append("Server Configuration: ")
        log += self._server.log_to_lists()
        return log

    def log_to_dict(self):
        log = {"process_num": self._process_num,
               "qp_num": self._qp_num,
               "mtu": self._mtu,
               "qp_type": QP_TO_NAME[self._qp_type],
               "reqs": self.req_to_str(self._reqs),
               "recvs": self.req_to_str(self._recvs),
               "client": self._client.log_to_dict(),
               "server": self._server.log_to_dict()
               }
        return log

    def print_dict_log(self, mode):
        dict = self.log_to_dict()
        for key in dict.keys():
            if key == "server" or key == "client":
                logger.LOG("{}:".format(key), mode)
                for subkey in dict[key].keys():
                    logger.LOG("    {}:  {}".format(
                        subkey, dict[key][subkey]), mode)
            else:
                logger.LOG("{}: {}".format(key, dict[key]), mode)


class Point(object):
    '''
        Each Point in the search space consists of two endhosts and one/many traffics.
    '''

    def __init__(self, space: Space, usr_A, usr_B,
                 ip_A, ip_B,
                 ibdev_A, ibdev_B):
        super(Point).__init__()
        self._space = space
        forward = Traffic(space, client_usr=usr_A, server_usr=usr_B,
                          client_ip=ip_A, server_ip=ip_B,
                          client_dev=ibdev_A, server_dev=ibdev_B)
        backward = Traffic(space, client_usr=usr_B, server_usr=usr_A,
                           client_ip=ip_B, server_ip=ip_A,
                           client_dev=ibdev_B, server_dev=ibdev_A)
        self._traffics = [forward, backward]
        self._ip_A = ip_A
        self._ip_B = ip_B
        self._usr_A = usr_A
        self._usr_B = usr_B
        self._ibdev_A = ibdev_A
        self._ibdev_B = ibdev_B

    def get_traffic_nums(self):
        return len(self._traffics)

    def get_traffic(self, idx: int):
        return self._traffics[idx]

    def random(self, max_num_traffics=2):
        self._traffics.clear()
        # We need at least one forward traffic
        usrs = [self._usr_A, self._usr_B]
        ips = [self._ip_A, self._ip_B]
        devs = [self._ibdev_A, self._ibdev_B]
        # We are testing A so forward is a must
        forward = Traffic(self._space, client_usr=self._usr_A, server_usr=self._usr_B,
                          client_ip=self._ip_A, server_ip=self._ip_B,
                          client_dev=self._ibdev_A, server_dev=self._ibdev_B)
        forward.random()
        src = random.randint(0, 1)  # Loopback or bi-directional
        dst = 0
        backward = Traffic(self._space, client_usr=usrs[src], server_usr=usrs[dst],
                           client_ip=ips[src], server_ip=ips[dst],
                           client_dev=devs[src], server_dev=devs[dst])
        backward.random()
        self._traffics.append(forward)
        self._traffics.append(backward)

    def mutate(self, traffic_id: int, mutate_object: str, dim: str):
        traffic = self._traffics[traffic_id]
        if "reqs" in dim or "recvs" in dim:
            traffic.mutate_req_recv()
            return
        bound = self._space._bounds[dim[1:]]
        cur_val = self.get_dim_value(traffic_id, mutate_object, dim)
        delta_value = 0
        while delta_value == 0:
            delta_value = random.randint(
                bound[0] - cur_val, bound[1] - cur_val)
        if mutate_object == "_traffic":
            # MTU, reqs, recvs will affect
            traffic.__setattr__(dim, cur_val + delta_value)
            if "MTU" in dim and delta_value < 0:  # The MTU gets smaller
                traffic.mutate_req_recv()
            if "qp_type" in dim and traffic._qp_type != 2:  # opcode and size should be modified
                traffic.mutate_req_recv()
        else:
            endhost = traffic.__getattribute__(mutate_object)
            endhost.__setattr__(dim, cur_val + delta_value)
            endhost._send_wq_depth = max(
                endhost._send_wq_depth, endhost._send_batch)
            endhost._recv_wq_depth = max(
                endhost._recv_wq_depth, endhost._recv_batch)
        return

    def get_dim_value(self, traffic_id: int, mutate_object: str, dim: str):
        traffic = self._traffics[traffic_id]
        if mutate_object == "_traffic":
            value = traffic.__getattribute__(dim)
        else:
            value = traffic.__getattribute__(
                mutate_object).__getattribute__(dim)
        return value

    def get_total_qps(self):
        ret = 0
        for traffic in self._traffics:
            ret += traffic.get_numqps()
        return ret

    def display(self):
        for traffic in self._traffics:
            traffic.display()
            print("")

    def log_to_lists(self):
        log = []
        cnt = 0
        for traffic in self._traffics:
            log.append("Traffic {}".format(cnt))
            cnt += 1
            log += traffic.log_to_lists()
        return log

    def log_to_dict(self):
        log = {"Traffics": []}
        cnt = 0
        for traffic in self._traffics:
            traffic_log = traffic.log_to_dict()
            log["Traffics"].append(traffic_log)
        return log

    def print_dict_log(self, mode="INFO"):
        logger.LOG("Traffics:", mode)
        for traffic in self._traffics:
            traffic.print_dict_log(mode)
        logger.LOG("")
