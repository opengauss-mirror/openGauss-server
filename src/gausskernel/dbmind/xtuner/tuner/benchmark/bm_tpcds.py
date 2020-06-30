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


def run(remote_server, local_host):
    path = '/home/opengauss/project/tpcds/queries'  # modify this path
    cmd = "find . -type f -name '*.sql'"
    stdout, stderr = remote_server.exec_command_sync("cd {path};{cmd}".format(path=path, cmd=cmd))
    if len(stderr) > 0:
        raise Exception(stderr)
    files = stdout.strip().split('\n')
    time_start = time.time()
    for file in files:
        cmd = "gsql -U opengauss -d tpcds -p 5000 -f {file}".format(file=file)
        stdout, stderr = remote_server.exec_command_sync("cd {path};{cmd}".format(path=path, cmd=cmd))
        if len(stderr) > 0:
            print(stderr)
    cost = time.time() - time_start
    return - cost
