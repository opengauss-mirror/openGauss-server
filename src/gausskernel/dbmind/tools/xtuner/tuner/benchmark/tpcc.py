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
    # The process of generating the final report of the Benchmarksql-5.0 is separate from the test process.
    # Therefore, the `sleep` command needs to be added to wait to prevent the process from exiting prematurely.
    stdout, stderr = remote_server.exec_command_sync(['cd %s' % path, 'rm -rf benchmarksql-error.log', cmd, 'sleep 3'])
    if len(stderr) > 0:
        raise ExecutionError(stderr)

    # Find the tpmC result.
    tpmC = None
    split_string = stdout.split()
    for i, st in enumerate(split_string):
        if "(NewOrders)" in st:
            tpmC = split_string[i + 2]
            break
    stdout, stderr = remote_server.exec_command_sync(
        "cat %s/benchmarksql-error.log" % path)
    nb_err = stdout.count("ERROR:")  # Penalty term.
    return float(tpmC) - 10 * nb_err  # You can modify the penalty factor.
