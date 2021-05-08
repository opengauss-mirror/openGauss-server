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
import shlex

from tuner.exceptions import ExecutionError

# WARN: You need to download the benchmark-sql test tool to the system,
# replace the PostgreSQL JDBC driver with the openGauss driver,
# and configure the benchmark-sql configuration file.
# The program starts the test by running the following command:
path = '/path/to/benchmarksql/run'  # replace
cmd = "./runBenchmark.sh opengauss.properties"


def run(remote_server, local_host):
    """
    Because TPC-C would insert many tuples into database, we suggest that
    backup the raw data directory and restore it when run TPC-C benchmark some times.
    e.g.
    ```
        remote_server.exec_command_sync('mv ~/backup ~/gsdata')
    ```

    The passed two parameters are both Executor instance.
    :param remote_server: SSH object for remote database server.
    :param local_host: LocalExec object for local client host where run our tuning tool.
    :return: benchmark score, higher one must be better, be sure to keep in mind.
    """
    # Benchmark can be deployed on a remote server or a local server.
    # Here we set the terminal as a remote server.
    terminal = remote_server
    err_logfile = os.path.join(path, 'benchmarksql-error.log')
    cmd_files = shlex.split(cmd)
    if len(cmd_files) != 2:
        print('Invalid configuration parameter `benchmark_cmd`. '
              'You should check the item in the configuration file.', file=sys.stderr)
        exit(-1)
    # Check whether these files exist.
    shell_file, conf_file = cmd_files
    shell_file = os.path.join(path, shell_file)
    conf_file = os.path.join(path, conf_file)
    _, stderr1 = terminal.exec_command_sync('ls %s' % shell_file)
    _, stderr2 = terminal.exec_command_sync('ls %s' % conf_file)
    if len(stderr1) > 0 or len(stderr2) > 0:
        print('You should correct the parameter `benchmark_path` that the path contains several executable SQL files '
              'in the configuration file.')
        exit(-1)
    # Clean log file
    terminal.exec_command_sync('rm -rf %s' % err_logfile)
    # Run benchmark
    stdout, stderr = terminal.exec_command_sync('cd %s; %s %s' % (path, shell_file, conf_file))
    if len(stderr) > 0:
        raise ExecutionError(stderr)

    # Find the tpmC result.
    tpmC = None
    split_string = stdout.split()
    for i, st in enumerate(split_string):
        if "(NewOrders)" in st:
            tpmC = split_string[i + 2]
            break
    stdout, stderr = terminal.exec_command_sync(
        "cat %s/benchmarksql-error.log" % path)
    nb_err = stdout.count("ERROR:")  # Penalty term.
    return float(tpmC) - 10 * nb_err  # You can modify the penalty factor.
