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
import importlib
from ssh import ExecutorFactory

local_ssh = ExecutorFactory() \
    .set_host('127.0.0.1') \
    .get_executor()


def get_benchmark_instance(name):
    bm = importlib.import_module('benchmark.bm_{}'.format(name))

    def wrapper(server_ssh):
        res = bm.run(server_ssh, local_ssh)  # implement your own run() function.
        return res
    return wrapper
