# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information


import subprocess


class BaseHwMon(object):
    '''
        HwMon collects diagnostic counters. 
        Users should overwrite this class for their own diagnostic counters collector.
        Collie uses HwMon for Mellanox (works well with CX5/6) 
        and Broadcom (works well with P2100) in RoCEv2 environment

        @binary: the binary helps to collect diag counters (string)
                 should be the absolute address
        @counters: the name list of counters to collect  (list)
        @identity: the identity of the NIC to monitor, e.g., PCIe address, IB name. (string)
    '''

    def __init__(self, binary, counters):
        super(BaseHwMon, self).__init__()
        self._binary = str(binary)
        self._counters = counters

    def monitor(self, identity):
        raise NotImplementedError


'''
    Hardware monitor collects diagnostic counters.
    No diagnostic counters are currently publicly available. We remove them for confidentiality.
'''


class MlnxHwMon(BaseHwMon):
    ''' 
        Mellanox HwMon
        Rely on NeoHost to collect diagnostic counters that indicate anomalies.
    '''

    def __init__(self, binary, counters):
        super(MlnxHwMon, self).__init__(binary, counters)

    def monitor(self, identity):
        return {}


'''
    BrcmHwMon Implementation
'''
