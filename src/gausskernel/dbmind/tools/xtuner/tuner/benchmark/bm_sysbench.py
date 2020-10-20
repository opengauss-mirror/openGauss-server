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
from exceptions import ExecutionError


def run(remote_server, local_host):
    # you should install sysbench and place it in environment in advance.
    lua_path = "/home/opengauss/project/sysbench_install/share/sysbench/tests/include/oltp_legacy/oltp.lua"
    cmd = "sysbench --test={lua_path} --db-driver=pgsql " \
          "--pgsql-db=sysbench --pgsql-user=user1 --pgsql-password=passwd --pgsql-port=5000 --pgsql-host=127.0.0.1 " \
          "--oltp-tables-count=20 --oltp-table-size=1000 --max-time=30 --max-requests=0" \
          " --num-threads=20 --report-interval=3 --forced-shutdown=1 run" \
        .format(lua_path=lua_path)
    stdout, stderr = remote_server.exec_command_sync(cmd)
    if len(stderr) > 0:
        raise ExecutionError(stderr) 
    try:
        res = float(stdout.split('queries:')[1].split('(')[1].split('per')[0].strip())
    except Exception:
        raise ExecutionError('error occur when parse sysbench result..')
    return res
