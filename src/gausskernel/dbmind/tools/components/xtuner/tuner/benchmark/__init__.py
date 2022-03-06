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
import os
import types
import logging

from ..exceptions import ConfigureError
from ..executor import ExecutorFactory

# Create a local shell with resident memory.
# We must pass a local shell as an API to benchmark instance,
# maybe the shell will be used by benchmark instance sometime.
local_ssh = ExecutorFactory() \
    .set_host('127.0.0.1') \
    .get_executor()


def get_benchmark_instance(script, path, cmd, db_info):
    name = script.rstrip('.py')
    if not os.path.exists(os.path.join(os.path.dirname(__file__), name + '.py')):
        raise ConfigureError('Incorrect configuration option benchmark_script. '
                             'Enter the filename of the script in the benchmark directory '
                             'and ensure that the script file exists.')

    bm = importlib.import_module('tuner.benchmark.{}'.format(name))
    # Verify the validity of the benchmark script.
    # An exception will be thrown if benchmark instance does not have specified attributes.
    if not getattr(bm, 'run', False):
        raise ConfigureError('The benchmark script %s is invalid. '
                             'For details, see the example template and description document.' % script)
    # Check whether function run exists and whether its type matches.
    check_run_assertion = isinstance(bm.run, types.FunctionType) and bm.run.__code__.co_argcount == 2
    if not check_run_assertion:
        raise ConfigureError('The run function in the benchmark instance is not correctly defined. '
                             'Redefine the function by referring to the examples.')

    # Priority:
    # 1st: Argument script.
    # 2nd: Variable defined in the Python file.
    if path.strip() != '':
        bm.path = path
    if cmd.strip() != '':
        bm.cmd = cmd

    # Render the cmd.
    bm.cmd = bm.cmd.replace('{host}', db_info['host']) \
        .replace('{port}', str(db_info['port'])) \
        .replace('{user}', db_info['db_user']) \
        .replace('{password}', db_info['db_user_pwd']) \
        .replace('{db}', db_info['db_name'])

    # Wrap remote server shell as an API and pass it to benchmark instance.
    def wrapper(server_ssh):
        try:
            return bm.run(server_ssh, local_ssh)
        except Exception as e:
            logging.warning("An error occured while running the benchmark, hence the benchmark score is 0. The error is %s.", e, exc_info=True)
            return .0

    return wrapper
