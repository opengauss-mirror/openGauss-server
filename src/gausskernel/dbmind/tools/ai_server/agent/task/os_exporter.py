#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : os_exporter.py
# Version      :
# Date         : 2021-4-7
# Description  : get system information
#############################################################################

try:
    import os
    import sys
    import time

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
    from common.utils import Common
except ImportError as err:
    sys.exit("os_exporter.py: Failed to import module: %s." % str(err))


class OSExporter:
    def __init__(self, logger):
        try:
            collection_info = Common.acquire_collection_info()
        except Exception as err_msg:
            logger.error(str(err_msg))
            raise Exception(str(err_msg))
        self.logger = logger
        self.ip = collection_info["ip"]
        self.port = collection_info["port"]

    def cpu_usage(self):
        """
        Obtaining the CPU Usage of the GaussDB
        :return: current cpu usage of the GaussDb
        """
        proc_pid = Common.get_proc_pid(self.ip, self.port)
        cmd = "ps -ux | awk '{if ($2==\"%s\")print}' |awk '{print $3}'" % proc_pid
        std, _ = Common.execute_cmd(cmd)
        if not std:
            return "0.0"
        return std.decode("utf-8").strip()

    def memory_usage(self):
        """
        Obtaining the memory Usage of the GaussDB
        :return: current memory usage of the GaussDb
        """
        proc_pid = Common.get_proc_pid(self.ip, self.port)
        cmd = "ps -ux | awk '{if ($2==\"%s\")print}' |awk '{print $4}'" % proc_pid
        std, _ = Common.execute_cmd(cmd)
        if not std:
            return "0.0"
        return std.decode("utf-8").strip()

    @staticmethod
    def io_wait():
        """
        Obtaining the system io_wait
        :return: io_wait info
        """
        std, _ = Common.execute_cmd("iostat")
        if not std:
            return "0.0"
        usage = std.decode("utf-8").split("\n")[3].split()[3]
        return usage

    def io_write(self):
        """
        Obtaining the io_write info of the GaussDB
        :return: io_write info
        """
        proc_pid = Common.get_proc_pid(self.ip, self.port)
        cmd = "pidstat -d | awk '{if ($4==\"%s\")print}' | awk '{print $6}'" % proc_pid
        std, _ = Common.execute_cmd(cmd)
        if not std:
            return "0.0"
        return std.decode("utf-8").strip()

    def io_read(self):
        """
        Obtaining the io_read info of the GaussDB
        :return: io_read info
        """
        proc_pid = Common.get_proc_pid(self.ip, self.port)
        cmd = "pidstat -d | awk '{if ($4==\"%s\")print}' | awk '{print $5}'" % proc_pid
        std, _ = Common.execute_cmd(cmd)
        if not std:
            return "0.0"
        return std.decode("utf-8").strip()

    def disk_used_size(self):
        """
        Obtaining the system disk used size
        :return: current disk used size of the GaussDb
        """
        proc_pid = Common.get_proc_pid(self.ip, self.port)
        get_data_path = "ps -ux | awk '{if ($2==\"%s\")print}'" % proc_pid
        std, _ = Common.execute_cmd(get_data_path)
        if not std:
            self.logger.warn("There is no process of: %s." % proc_pid)
            return "0.0M"
        std = std.decode()
        data_dir = std.split()[std.split().index("-D") + 1]
        if not os.path.isdir(data_dir):
            self.logger.warn("The data dir does not exist: %s." % data_dir)
            return "0.0M"
        disk_info, _ = Common.execute_cmd("du -sh %s" % data_dir.strip())
        usage = Common.unify_byte_unit(disk_info.decode("utf-8").split()[0])
        return usage

    def __call__(self, *args, **kwargs):
        self.logger.info("Start to collect os data.")
        cpu_usage = self.cpu_usage()
        memory_usage = self.memory_usage()
        disk_used_size = self.disk_used_size()
        io_wait = self.io_wait()
        io_read = self.io_read()
        io_write = self.io_write()
        os_data = {"cpu_usage": cpu_usage, "memory_usage": memory_usage,
                   "disk_usage": disk_used_size, "io_wait": io_wait,
                   "io_read": io_read, "io_write": io_write,
                   "timestamp": "%s" % time.strftime("%Y-%m-%dT%H:%M:%SZ")}
        self.logger.info("Successfully collected os data：%s." % os_data)
        return os_data
