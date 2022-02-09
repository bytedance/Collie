# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information


# Bone is what Collie likes! (Say, the anomaly of the RDMA NIC)

import subprocess


class BaseBoneMon(object):
    '''
        BoneMon should collects throughput and pause framesor any other anomaly signal.
        @bps_bar: the throughput threshold in bits per sec. (float)
        @pps_bar: the throughput threshold in pkts per sec. (float)
        @intf: the interface BoneMon to monitor. (str)

    '''

    def __init__(self, bps_bar, pps_bar):
        super(BaseBoneMon, self).__init__()
        self._bps_bar = float(bps_bar)
        self._pps_bar = float(pps_bar)

    def monitor(self, intf):
        raise NotImplementedError

    def check_bone(self, intf):
        raise NotImplementedError


class MlnxBoneMon(BaseBoneMon):
    '''
        Mellanox Bone Monitor
    '''

    def __init__(self, bps_bar, pps_bar, tx_pfc_bar, rx_pfc_bar):
        super(MlnxBoneMon, self).__init__(bps_bar, pps_bar)
        self._tx_pfc_bar = float(tx_pfc_bar)
        self._rx_pfc_bar = float(rx_pfc_bar)
        self._metrics = [
            # bits per second (TX)      in Mbps
            "tx_vport_rdma_unicast_bytes",
            # bits per second (RX)      in Mbps
            "rx_vport_rdma_unicast_bytes",
            # packets per second (TX)   in pps
            "tx_vport_rdma_unicast_packets",
            # packets per second (RX)   in pps
            "rx_vport_rdma_unicast_packets",
            "tx_prio3_pause_duration",  # pfc duration per second (TX)  in us
            "rx_prio3_pause_duration"   # pfc duration per second (RX)  in us
        ]

    def monitor(self, intf):
        result = {key: 0.0 for key in self._metrics}
        cmd = "mlnx_perf -i {} -c 1".format(intf)
        try:
            output = subprocess.check_output(cmd, shell=True)
        except Exception as e:
            print(e)
            return {key: -1.0 for key in self._metrics}
        output = output.decode().split('\n')
        for line in output:
            for metric in self._metrics:
                if metric in line:
                    line = line.strip(' ').split(' ')
                    if "bytes" in metric:
                        result[metric] = float(
                            line[-2].replace(',', '')) / 1000.0
                    else:
                        result[metric] = float(line[-1].replace(',', ''))
        return result

    def check_bone(self, result):
        # First, we check pause duration
        if (result["tx_prio3_pause_duration"] > self._tx_pfc_bar or
                result["rx_prio3_pause_duration"] > self._rx_pfc_bar):
            return -1

        if (result["tx_vport_rdma_unicast_bytes"] < self._bps_bar and
                result["rx_vport_rdma_unicast_bytes"] < self._bps_bar):
            # bps does not achieve, chk with pps
            # However, pps_bar is pretty hard to set accurately.
            # In production, we chk with bps for most scenarios.
            if (result["tx_vport_rdma_unicast_packets"] < self._pps_bar and
                    result["rx_vport_rdma_unicast_packets"] < self._pps_bar):
                return -2
        return 0
