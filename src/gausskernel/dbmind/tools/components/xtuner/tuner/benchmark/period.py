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
import sys
import time
import logging

from tuner import utils
from tuner.exceptions import ExecutionError

path = ''
# Measure current total committed transactions that do not include xact_rollback.
cmd = "gsql -U {user} -W {password} -d postgres -p {port} -c " \
      "\"SELECT sum(xact_commit) FROM pg_stat_database where datname = '{db}';\""


# This script captures the performance indicators in the user's periodic execution task, and measures the quality
# of the tuning results by measuring the range of changes in the indicators.
def run(remote_server, local_host) -> float:
    wait_seconds = utils.config['benchmark_period']
    if not wait_seconds:
        print("Not configured the parameter 'benchmark_period' in the configuration file.",
              file=sys.stderr)
        exit(-1)

    stdout, stderr = remote_server.exec_command_sync(cmd)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    prev_txn = int(utils.to_tuples(stdout)[0][0])

    time.sleep(wait_seconds)
    stdout, stderr = remote_server.exec_command_sync(cmd)
    if len(stderr) > 0:
        raise ExecutionError(stderr)
    current_txn = int(utils.to_tuples(stdout)[0][0])

    # Return TPS in this period.
    return (current_txn - prev_txn) / wait_seconds
