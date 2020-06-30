# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ----------------------------------------------------------------------------
# Description  : OSSDFaultInfo.py is utility for SSD default info
#############################################################################
import sys


class SSDFaultInfo():
    def __init__(self):
        pass

    Failurelibs = {
        "0x11": ["hioerr", "Data recovery failure",
                 "GC and CM read, RAID recovery failed",
                 "data error and failed to recover",
                 "contact support for repairs"],
        "0x2b": ["hioerr", "Read back failure after programming failure",
                 "write failure read back failure",
                 "4KB data error and failed to recover",
                 "contact support for repairs"],
        "0x2e": ["hioerr", "No available reserved blocks",
                 "No available blocks", "result in timeout",
                 "contact support for repairs"],
        "0x300": ["hioerr", "CMD timeout", "CMD timeout",
                  "CMD executive timeout", "contact support for repairs"],
        "0x307": ["hioerr", "I/O error", "IO error",
                  "Read/write error, return by read and write return value",
                  "contact support for repairs"],
        "0x30a": ["hioerr", "DDR init failed", "DDR initialization failure",
                  "Drive loading failed", "contact support for repairs"],
        "0x30c": ["hioerr", "Controller reset sync hioerr",
                  "Controller reset status out of sync ",
                  "Drive loading failed", "contact support for repairs"],
        "0x30d": ["hioerr", "Clock fault", "abnormals found in clock testing",
                  "If happened during the process of loading, it may cause "
                  "drive loading failed, during operation may cause SSD work "
                  "abnormally",
                  "contact support for repairs"],
        "0x312": ["hiowarn", "CAP: voltage fault",
                  "Capacitance voltage alarming",
                  "cause power-fail protection failure, without affecting IO "
                  "functions",
                  "contact support for repairs"],
        "0x313": ["hiowarn", "CAP: learn fault",
                  "Capacitance self learning error.",
                  "Unable to get accurate capacitance, may cause power fail "
                  "protection failure; without affecting IO functions",
                  "contact support for repairs"],
        "0x314": ["hionote", "CAP status",
                  "Capacitance is in self learning status", "None", "Ignore"],
        "0x31a": ["hiowarn", "CAP: short circuit",
                  "Capacitance voltage is zero, possibly short circuit",
                  "may cause power-fail protection failure; without "
                  "affecting IO functions",
                  "contact support for repairs"],
        "0x31b": ["hiowarn", "Sensor fault", "Sensor access error",
                  "If occurred during the process of loading ,it may cause "
                  "drive loading failed; capacitor voltage could not be "
                  "monitored during operation",
                  "contact support for repairs"],
        "0x39": ["hioerr", "Init: PBMT scan failure",
                 "initialization scanning PBMT read error",
                 "part of data mapping relationship missing, 64MB at most "
                 "can not find",
                 "contact support for repairs"],
        "0x3b": ["hioerr", "Init: first page scan failure",
                 "initialization scan home page read error",
                 "part of data mapping relationship missing, 64MB at most "
                 "can not find (occur during initialization)",
                 "contact support for repairs"],
        "0x3c": ["hioerr", "Init: scan unclosed block failure",
                 "Init:reset pointer, data page read error",
                 "4KB data mapping relationship missing, the 4KB data can "
                 "not be found",
                 "contact support for repairs"],
        "0x40": ["hioerr", "Init: PMT recovery: data page read failure",
                 "Init: PMT recovery, data page read error",
                 "4KB data mapping relationship missing, the 4KB data can "
                 "not be found",
                 "contact support for repairs"],
        "0x43": ["hioerr", "too many unclosed blocks",
                 "scan to the third unfulfilled block ",
                 "Split from original 0x3c scenario. Part of data mapping "
                 "relationship missing, 64MB at most can not find (occur "
                 "when initialization)",
                 "contact support for repairs"],
        "0x45": ["hioerr", "Init: more than one PDW block found",
                 "PDW Initialization abnormal: found two and more than two "
                 "PWD",
                 "abnormal, may cause data missing",
                 "contact support for repairs"],
        "0x47": ["hionote", "Init: PDW block not found",
                 "initialization abnormal: PDW is not found when "
                 "initialization",
                 "data may be incomplete", "contact support for repairs"],
        "0x50": ["hioerr", "Cache: hit error data", "Cache hit data error",
                 "4KB data error and failed to recover",
                 "contact support for repairs"],
        "0x51": ["hioerr", "Cache: read back failure",
                 "Cache completion and reading back error",
                 "4KB data error and failed to recover",
                 "contact support for repairs"],
        "0x53": ["hioerr", "GC/WL read back failure",
                 "GC and WL read, data error",
                 "4KB data error and failed to recover",
                 "contact support for repairs"],
        "0x7": ["hioerr", "No available blocks",
                "no available block, free list is empty",
                "data failed to write normally",
                "contact support for repairs"],
        "0x7e": ["hionote", "Read blank page", "read blank page",
                 "IO return successfully, but read wrong data",
                 "contact support for repairs"],
        "0x7f": ["hiowarn", "Access flash timeout", "access flash timeout",
                 "without affecting data correctness, but access Flash "
                 "timeout",
                 "Ignore"],
        "0x8a": ["hiowarn", "Warning: Bad Block close to limit",
                 "bad block level 1 alarming (exceed 11%)",
                 "bad block level 1 alarming (exceed 11%)", "Ignore"],
        "0x8b": ["hioerr", "Error: Bad Block over limit",
                 "bad block level 2 alarming (exceed 14%)",
                 "bad block level 2 alarming (exceed 14%)",
                 "contact support for repairs"],
        "0x8c": ["hiowarn", "Warning: P/E cycles close to limit",
                 "P/E cycles Level 1 alarming", "P/E cycles Level 1 alarming",
                 "Ignore"],
        "0x8d": ["hioerr", "Error: P/E cycles over limit",
                 "P/E cycles Level 2 alarming", "P/E cycles Level 2 alarming",
                 "Scrapped"],
        "0x90": ["hionote", "Over temperature",
                 "temperature value exceed limitation: current defined 90 "
                 "centi degrees",
                 "High temperature may cause SSD abnormal, if found this "
                 "alarm should test server fan speed etc. then drive will "
                 "run protection mechanism, limit IO speed (shut down this "
                 "function by API)",
                 "Suggest to check radiator"],
        "0x91": ["hionote", "Temperature is OK",
                 "Temperature goes back to normal", "None", "Ignore"],
        "0x92": ["hiowarn", "Battery fault", "Super-capacitor status alarming",
                 "Super-capacitor working status is abnormal",
                 "contact support for repairs"],
        "0x93": ["hioerr", "SEU fault", "logical found SEU fault",
                 "May cause logical working abnormally",
                 "Power up and down on the SSD"],
        "0x94": ["hioerr", "DDR error",
                 "data error found in controller plug-in DDR",
                 "May cause controller work abnormally (data may have been "
                 "in disorder status)",
                 "contact support for repairs"],
        "0x95": ["hioerr", "Controller serdes error",
                 "Controller serdes test transmission error",
                 "May cause controller work abnormally(data may have been in "
                 "disorder status)",
                 "contact support for repairs"],
        "0x96": ["hioerr", "Bridge serdes 1 error",
                 "Bridge controller serdes 1 test transmission error",
                 "May cause controller work abnormally(data may have been in "
                 "disorder status)",
                 "contact support for repairs"],
        "0x97": ["hioerr", "Bridge serdes 2 error",
                 "Bridge controller serdes 2 test transmission error",
                 "May cause controller work abnormally(data may have been in "
                 "disorder status)",
                 "contact support for repairs"],
        "0x98": ["hioerr", "SEU fault (corrected)",
                 "SEU fault (correctable error)",
                 "Split from original 0x3c scenario. May cause logical "
                 "working abnormally (10 seconds time-delay from error to "
                 "correct process)",
                 "Reset SSD"],
        "0x9a": ["hionote", "Over temperature",
                 "temperature value exceed limitation: current defined 90 "
                 "centi degrees",
                 "High temperature may cause SSD abnormal, if found this "
                 "alarm should test server fan speed etc. then drive will "
                 "run protection mechanism, limit IO speed (shut down this "
                 "function by API)",
                 "Suggest to check radiator"],
        "0xf1": ["hioerr", "Read failure without recovery",
                 "IOR read can not recover",
                 "4KB data error and failed to recover",
                 "contact support for repairs"],
        "0xf7": ["hioerr", "Init: RAID not complete",
                 "Init: RAID not complete",
                 "RAID line data not complete before power failed",
                 "contact support for repairs"]
    }
