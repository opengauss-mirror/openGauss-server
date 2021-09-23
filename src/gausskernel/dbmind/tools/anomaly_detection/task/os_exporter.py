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
import subprocess

from utils import DBAgent, convert_to_mb


class OSExporter:
    __tablename__ = 'os_exporter'

    def __init__(self, db_port):
        self.port = db_port

    @staticmethod
    def cpu_usage():
        child1 = subprocess.Popen(['ps', '-ux'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, shell=False)
        sub_chan = child2.communicate()
        if not sub_chan[0]:
            result = 0.0
        else:
            result = sub_chan[0].split()[2].decode('utf-8')
        return result

    @staticmethod
    def io_read():
        child1 = subprocess.Popen(['pidstat', '-d'], stdout=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
        sub_chan = child2.communicate()
        if not sub_chan[0]:
            result = 0.0
        else:
            result = sub_chan[0].split()[3].decode('utf-8')
        return result

    @staticmethod
    def io_write():
        child1 = subprocess.Popen(['pidstat', '-d'], stdout=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
        sub_chan = child2.communicate()
        if not sub_chan[0]:
            result = 0.0
        else:
            result = sub_chan[0].split()[4].decode('utf-8')
        return result

    @staticmethod
    def io_wait():
        child1 = subprocess.Popen(['iostat'], stdout=subprocess.PIPE, shell=False)
        sub_chan = child1.communicate()
        if not sub_chan[0]:
            result = 0.0
        else:
            result = sub_chan[0].decode("utf-8").split("\n")[3].split()[3]
        return result

    @staticmethod
    def memory_usage():
        child1 = subprocess.Popen(['ps', '-ux'], stdout=subprocess.PIPE, shell=False)
        child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
        sub_chan = child2.communicate()
        if not sub_chan[0]:
            result = 0.0
        else:
            result = sub_chan[0].split()[3].decode('utf-8')
        return result

    def disk_space(self):
        sql = 'select datapath from pg_node_env;'
        with DBAgent(port=self.port, database='postgres') as db:
            datapath = db.fetch_all_result(sql)[0][0]
        pg_data = os.path.realpath(datapath)
        child = subprocess.Popen(['du', '-sh', pg_data], stdout=subprocess.PIPE, shell=False)
        sub_chan = child.communicate()
        if sub_chan[1] is not None:
            raise ValueError('error when get disk usage of openGauss: {error}'.
                             format(error=sub_chan[1].decode('utf-8')))
        if not sub_chan[0]:
            result = '0.0'
        else:
            result = str(convert_to_mb(sub_chan[0].decode('utf-8')))
        return result

    def output(self):
        result = [self.cpu_usage(), self.io_wait(), self.io_read(),
                  self.io_write(), self.memory_usage(), self.disk_space()]
        return result
