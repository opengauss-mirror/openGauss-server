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
from tuner.exceptions import ExecutionError

# WARN: You should first install the sysbench test tool on the system,
# then fill in the following path and cmd.

path = "/path/to/sysbench_install/share/sysbench/tests/include/oltp_legacy/oltp.lua"
cmd = "sysbench --test=%s --db-driver=pgsql " \
      "--pgsql-db={db} --pgsql-user={user} --pgsql-password={password} --pgsql-port={port} --pgsql-host=127.0.0.1 " \
      "--oltp-tables-count=20 --oltp-table-size=1000 --max-time=30 --max-requests=0" \
      " --num-threads=20 --report-interval=3 --forced-shutdown=1 run" % path


def run(remote_server, local_host):
    stdout, stderr = remote_server.exec_command_sync(cmd)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    try:
        return float(stdout.split('queries:')[1].split('(')[1].split('per')[0].strip())
    except Exception as e:
        raise ExecutionError('Failed to parse sysbench result, because %s.' % e)
