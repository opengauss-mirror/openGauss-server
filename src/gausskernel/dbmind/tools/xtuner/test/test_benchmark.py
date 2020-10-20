# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# -------------------------------------------------------------------------
#
# test_benchmark.py
#
# IDENTIFICATION
#    src/gausskernel/dbmind/xtuner/test/test_benchmark.py
#
# -------------------------------------------------------------------------


import sys

sys.path.append('../tuner')
from ssh import ExecutorFactory
import benchmark


def main():
    executor = ExecutorFactory()
    executor.set_host('')  # padding your information
    executor.set_user('')
    executor.set_pwd('')
    executor.set_port(22)
    fun = benchmark.get_benchmark_instance('sysbench')  # tpcc/tpch/tpcds/sysbench
    res = fun(executor.get_executor())
    print(res)


if __name__ == "__main__":
    main()
