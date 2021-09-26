#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : database_exporter.py
# Version      :
# Date         : 2021-4-7
# Description  : get gaussDb database information
#############################################################################

try:
    import os
    import sys
    import time
    import re

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../"))
    from common.utils import Common, DBAgent
except ImportError as err:
    sys.exit("database_exporter.py: Failed to import module: %s." % str(err))


class DatabaseExporter:
    # all the metrics can be acquired by gsql

    def __init__(self, logger):
        try:
            self.port = Common.acquire_collection_info()["port"]
        except Exception as err_msg:
            logger.error(str(err_msg))
            raise Exception(str(err_msg))
        self.logger = logger
        self.cursor = DBAgent(port=self.port, database="postgres")

    def guc_parameter(self):
        """
        get database guc parameter
        :return: {work_mem: value, shared_buffers: value, max_connections: value}
        """
        guc_params = ["work_mem", "shared_buffers", "max_connections"]
        # get work_mem, shared_buffers, max_connections guc parameter
        guc_values = []
        for param in guc_params:
            try:
                std = self.cursor.fetch_one_result("show %s;" % param, self.logger)
                guc_values.append(std[0])
            except Exception as err_msg:
                raise Exception("Failed go get guc parameter, Error:%s." % err_msg)
        result = dict(zip(guc_params, guc_values))
        return result

    def current_connections(self):
        """
        get current connections
        :return:
        """
        # get current_connections
        sql_cmd = "select count(1) from pg_stat_activity where client_port is not null;"
        try:
            std = self.cursor.fetch_one_result(sql_cmd, self.logger)
        except Exception as err_msg:
            raise Exception("Failed go get current connections, Error:%s." % err_msg)
        return std[0]

    def qps(self):
        """
        Calculates QPS information.
        :return:
        """
        sql = "select sum(select_count+update_count+insert_count+delete_count) from gs_sql_count;"
        try:
            # get first execute result
            t1 = time.time()
            std = self.cursor.fetch_one_result(sql, self.logger)
            n1 = int(std[0])
            time.sleep(1)
            # get second execute result
            t2 = time.time()
            std = self.cursor.fetch_one_result(sql, self.logger)
            n2 = int(std[0])
        except Exception as err_msg:
            raise Exception("Failed to execute cmd: %s, \nError: %s." % (sql, err_msg))
        qps = (n2 - n1) / (t2 - t1)
        return qps

    def __call__(self, *args, **kwargs):
        self.logger.info("Start to collect database data.")
        retry_times = 0
        while True:
            try:
                if retry_times >= 1:
                    self.cursor = DBAgent(port=self.port, database="postgres")
                guc_parameter = self.guc_parameter()
                current_connection = self.current_connections()
                qps = self.qps()
                break
            except Exception as connect_err:
                retry_times += 1
                self.logger.error("Failed to collect database data, retry times: %d." % retry_times)
                if retry_times == 10:
                    raise Exception(str(connect_err))
                time.sleep(5)
        data = {'current_connection': current_connection,
                'qps': qps, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ")}
        data.update(guc_parameter)
        self.logger.info("Successfully collected database data: %s." % data)
        return data
