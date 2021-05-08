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
import sys
import time

# WARN: You need to import data into the database and SQL statements in the following path will be executed.
# The program automatically collects the total execution duration of these SQL statements.
path = '/path/to/tpcds/queries'  # modify this path which contains benchmark SQL files.
cmd = "find %s -type f -name '*.sql' -exec gsql -U {user} -W {password} -d {db} -p {port} -f {} > /dev/null \\;"


def run(remote_server, local_host):
    time_start = time.time()
    # Check whether the path is valid.
    stdout, stderr = remote_server.exec_command_sync('ls %s' % path)
    if len(stderr) > 0:
        print('You should correct the parameter `benchmark_path` that the path contains several executable SQL files '
              'in the configuration file.')
        exit(-1)

    stdout, stderr = remote_server.exec_command_sync(cmd % path)
    if len(stderr) > 0:
        print(stderr, file=sys.stderr)
    cost = time.time() - time_start
    return - cost
