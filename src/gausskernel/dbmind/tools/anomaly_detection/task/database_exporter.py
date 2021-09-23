"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

import os
import re
import shlex
import subprocess
import time

from utils import DBAgent, convert_to_mb


class DatabaseExporter:
    __tablename__ = 'database_exporter'

    def __init__(self, db_port):
        self.port = db_port

    def guc_parameter(self):
        """
        get database guc parameter
        :return: {work_mem: value, shared_buffers: value, max_connections: value}
        """
        result = []
        guc_names = ['work_mem', 'shared_buffers', 'max_connections']
        sql = "select setting, unit from pg_settings where name = '{guc_name}';"
        with DBAgent(port=self.port, database='postgres') as db:
            for guc_name in guc_names:
                res = db.fetch_all_result(sql.format(guc_name=guc_name))
                if guc_name != 'max_connections':
                    res = convert_to_mb(str(res[0][0]) + res[0][1])
                    result.append(res)
                else:
                    result.append(res[0][0])
        result = ",".join(map(lambda x: str(x), result))
        return result

    def current_connections(self):
        """
        Get current connections
        :return:
        """
        # Get current_connections:
        sql = "select count(1) from pg_stat_activity;"
        with DBAgent(port=self.port, database='postgres') as db:
            result = db.fetch_all_result(sql)[0][0]
        return result - 1

    def qps(self):
        sql = "select select_count+update_count+insert_count+delete_count from gs_sql_count;"
        with DBAgent(port=self.port, database='postgres') as db:
            num1 = db.fetch_all_result(sql)
            time.sleep(0.1)
            num2 = db.fetch_all_result(sql)
            result = (num2[0][0] - num1[0][0] - 1) * 10 if num2[0][0] > num1[0][0] else 0
        return result

    def process(self):
        result = {}
        child1 = subprocess.Popen(shlex.split("ps -aux"), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(shlex.split("sort -k3nr"), stdin=child1.stdout, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=False)
        child3 = subprocess.Popen(shlex.split("grep -v gaussdb"), stdin=child2.stdout, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=False)
        child4 = subprocess.Popen(shlex.split("awk '{print $2,$3,$4,$11}'"), stdin=child3.stdout,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=False)
        child5 = subprocess.Popen(shlex.split("head -4"), stdin=child4.stdout, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=False)
        stream = child5.communicate()
        if not stream[1]:
            res = stream[0].decode('utf-8').strip()
            res = res.split('\n')
            for item in res:
                match_res = re.match(r'(\d+) (\d+(?:\.\d+)) (\d+(?:\.\d+)) (.+)', item, re.DOTALL)
                if not match_res:
                    continue
                pid = match_res.group(1)
                cpu_usage = match_res.group(2)
                memory_usage = match_res.group(3)
                process = match_res.group(4)
                key = str(pid) + '_' + process
                result[key] = str(cpu_usage) + ':' + str(memory_usage)
            return str(result)
        else:
            return str(result)

    def temp_file(self):
        sql = 'select datapath from pg_node_env;'
        with DBAgent(port=self.port, database='postgres') as db:
            datapath = db.fetch_all_result(sql)[0][0]
            pgsql_tmp = os.path.join(datapath, 'base/pgsql_tmp')
            if not os.path.exists(pgsql_tmp):
                return 'f'
            if len(os.listdir(pgsql_tmp)) > 0:
                return 't'
            else:
                return 'f'

    def output(self):
        result = [self.guc_parameter(), self.current_connections(), self.qps(), self.process(), self.temp_file()]
        return result
