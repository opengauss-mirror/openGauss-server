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
import time

from tuner.exceptions import ExecutionError

# WARN: You need to import data into the database and SQL statements in the following path will be executed.
# The program automatically collects the total execution duration of these SQL statements.
path = '/path/to/tpch/queries'  # modify this path
cmd = "gsql -U {user} -W {password} -d {db} -p {port} -f {file}"


def run(remote_server, local_host):
    find_file_cmd = "find . -type f -name '*.sql'"
    stdout, stderr = remote_server.exec_command_sync(['cd %s' % path, find_file_cmd])
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    files = stdout.strip().split('\n')
    time_start = time.time()
    for file in files:
        perform_cmd = cmd.format(file=file)
        stdout, stderr = remote_server.exec_command_sync(['cd %s' % path, perform_cmd])
        if len(stderr) > 0:
            print(stderr)
    cost = time.time() - time_start
    return - cost
