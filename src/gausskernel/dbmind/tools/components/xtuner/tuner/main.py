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
from __future__ import print_function

import argparse
try:
    import configparser
except ImportError:
    import ConfigParser as configparser
import json
import os
import sys
import logging
from getpass import getpass

from .exceptions import OptionError
from .xtuner import procedure_main
from . import utils

__version__ = '3.0.0'
__description__ = 'X-Tuner: a self-tuning tool integrated by openGauss.'


def check_port(port):
    return 0 < port <= 65535


def check_path_valid(path):
    path_check_list = [' ', '|', ';', '&', '$', '<', '>', '`', '\\',
                       '\'', '"', '{', '}', '(', ')', '[', ']', '~',
                       '*', '?', '!', '\n']

    if path.strip() == '':
        return True

    for char in path_check_list:
        if path.find(char) >= 0:
            return False

    return True


def check_version():
    version_info = sys.version_info
    major, minor = version_info.major, version_info.minor
    # At least, the Python version is (3, 6)
    if major < 3 or minor <= 5:
        return False
    return True


def build_db_info(args):
    if args.db_config_file:
        if not check_path_valid(args.db_config_file):
            print('FATAL: Detected illegal json path.', file=sys.stderr, flush=True)
            return

        with open(args.db_config_file, mode='r', errors='ignore') as fd:
            db_info = json.load(fd)
            if not {'db_name', 'db_user', 'host', 'host_user',
                    'port', 'ssh_port'}.issubset(db_info):
                print('ERROR: Lack of information in json file, please refer to the file share/server.json.template.',
                      file=sys.stderr, flush=True)
                return
    else:
        db_info = {
            'db_name': args.db_name,
            'db_user': args.db_user,
            'host': args.host,
            'host_user': args.host_user,
            'port': args.port,
            'ssh_port': args.host_ssh_port
        }

    # Requires users to enter password information.
    db_user_pwd = getpass("Please input the password of database: ")
    host_user_pwd = getpass("Please input the password of host: ")
    db_info['db_user_pwd'] = db_user_pwd
    db_info['host_user_pwd'] = host_user_pwd

    # Check the validation of each field.
    for option, value in db_info.items():
        if not value:
            print('ERROR: Need database information for %s.' % option, file=sys.stderr, flush=True)
            return
        else:
            if option in ('db_name', 'db_user', 'host_user'):
                if not check_path_valid(value):
                    print('FATAL: Detected illegal input for %s.' % option, file=sys.stderr, flush=True)
                    return

            if option in ('port', 'ssh_port'):
                if not check_port(value):
                    print('FATAL: Detect illegal port for %s.' % option, file=sys.stderr, flush=True)
                    return

    return db_info


def get_argv_parser():
    tuner_config_file = os.path.join(os.path.dirname(__file__), 'xtuner.conf')

    parser = argparse.ArgumentParser(description=__description__)
    parser.add_argument('mode', choices=['train', 'tune', 'recommend'],
                        help='Train a reinforcement learning model or tune database by model. And also can recommend'
                             ' best_knobs according to your workload.')

    db_info_group = parser.add_argument_group('Database Connection Information')
    db_info_group.add_argument('--db-name', help='The name of database where your workload running on.')
    db_info_group.add_argument('--db-user', help='Use this user to login your database. Note that the user must have '
                                                 'sufficient permissions.')
    db_info_group.add_argument('--port', type=int, help='Use this port to connect with the database.')
    db_info_group.add_argument('--host', help='The IP address of your database installation host.')
    db_info_group.add_argument('--host-user', help='The login user of your database installation host.')
    db_info_group.add_argument('--host-ssh-port', default=22, type=int,
                               help='The SSH port of your database installation host.')

    parser.add_argument('-f', '--db-config-file',
                        help='You can pass a path of configuration file otherwise you should '
                             'enter database information by command arguments manually. '
                             'Please see the template file share/server.json.template.')
    parser.add_argument('-x', '--tuner-config-file', default=tuner_config_file,
                        help='This is the path of the core configuration file of the X-Tuner. '
                             'You can specify the path of the new configuration file. '
                             'The default path is %s. '
                             'You can modify the configuration file to control the tuning process.' % tuner_config_file)

    parser.add_argument('-v', '--version', action='version')

    parser.version = __version__

    return parser


def get_config(filepath):
    if not os.path.exists(filepath):
        print("FATAL: Not found the configuration file %s." % filepath, file=sys.stderr)
        return

    if not os.access(filepath, os.R_OK):
        print("FATAL: Not have permission to read the configuration file %s." % filepath, file=sys.stderr)
        return

    cp = configparser.ConfigParser(inline_comment_prefixes=('#', ';', "'"))
    cp.read(filepath)

    config = dict()
    for section in cp.sections():
        for option, value in cp.items(section):
            config.setdefault(option, value)

    # Check configs.
    null_error_msg = 'The configuration option %s is null. Please set a specific value.'
    invalid_opt_msg = 'The configuration option %s is invalid. Please set one of %s.'
    positive_integer_msg = 'The configuration option %s must be a positive integer greater than 0.'

    # Section Master:
    for name in ('logfile', 'recorder_file', 'tune_strategy'):
        if cp['Master'].get(name, '').strip() == '':
            raise OptionError(null_error_msg % name)

    tune_strategy_opts = ['rl', 'gop', 'auto']
    tune_strategy = cp['Master'].get('tune_strategy')
    if tune_strategy not in tune_strategy_opts:
        raise OptionError(invalid_opt_msg % ('tune_strategy', tune_strategy_opts))

    config['verbose'] = cp['Master'].getboolean('verbose', fallback=True)
    config['drop_cache'] = cp['Master'].getboolean('drop_cache', fallback=False)
    config['used_mem_penalty_term'] = cp['Master'].getfloat('used_mem_penalty_term')

    # Section Benchmark:
    benchmarks = []
    benchmark_dir = os.path.join(os.path.dirname(__file__), 'benchmark')
    for root_dir, sub_dir, files in os.walk(benchmark_dir):
        if os.path.basename(root_dir) == 'benchmark':
            benchmarks = files
            break

    benchmark_script = cp['Benchmark'].get('benchmark_script', '')
    if benchmark_script.rstrip('.py') + '.py' not in benchmarks:
        raise OptionError(invalid_opt_msg % ('benchmark_script', benchmarks))
    config['benchmark_path'] = cp['Benchmark'].get('benchmark_path', '')
    config['benchmark_cmd'] = cp['Benchmark'].get('benchmark_cmd', '')
    benchmark_period = cp['Benchmark'].get('benchmark_period', '0')
    if not benchmark_period:
        benchmark_period = '0'
    config['benchmark_period'] = int(benchmark_period)

    # Section Knobs
    scenario_opts = ['auto', 'ap', 'tp', 'htap']
    if cp['Knobs'].get('scenario', '') not in scenario_opts:
        raise OptionError(invalid_opt_msg % ('scenario', scenario_opts))

    # Section RL and GOP
    def check_positive_integer(*opts):
        for opt in opts:
            if config[opt] <= 0:
                raise OptionError(positive_integer_msg % opt)

    if tune_strategy in ('auto', 'rl'):
        for name in cp['Reinforcement Learning']:
            if name.strip() == '':
                raise OptionError(null_error_msg % name)
        if cp['Reinforcement Learning'].get('rl_algorithm', '') != 'ddpg':
            raise OptionError(invalid_opt_msg % ('rl_algorithm', 'ddpg'))

        config['rl_steps'] = cp['Reinforcement Learning'].getint('rl_steps')
        config['max_episode_steps'] = cp['Reinforcement Learning'].getint('max_episode_steps')
        config['test_episode'] = cp['Reinforcement Learning'].getint('test_episode')
        check_positive_integer('rl_steps', 'max_episode_steps', 'test_episode')

    if tune_strategy in ('auto', 'gop'):
        for name in cp['Gloabal Optimization Algorithm']:
            if name.strip() == '':
                raise OptionError(null_error_msg % name)

        gop_algorithm_opts = ['bayes', 'pso']
        if cp['Gloabal Optimization Algorithm'].get('gop_algorithm', '') not in gop_algorithm_opts:
            raise OptionError(invalid_opt_msg % ('gop_algorithm', gop_algorithm_opts))

        config['max_iterations'] = cp['Gloabal Optimization Algorithm'].getint('max_iterations')
        config['particle_nums'] = cp['Gloabal Optimization Algorithm'].getint('particle_nums')
        check_positive_integer('max_iterations', 'particle_nums')

    return config


def main(argv):
    if not check_version():
        print("FATAL: You should use at least Python 3.6 or above version.")
        return -1

    parser = get_argv_parser()
    args = parser.parse_args(argv)
    mode = args.mode
    db_info = build_db_info(args)
    if not db_info:
        parser.print_usage()
        return -1

    utils.config = config = get_config(args.tuner_config_file)
    if not config:
        return -1

    try:
        return procedure_main(mode, db_info, config)
    except Exception as e:
        logging.exception(e)
        print('FATAL: An exception occurs during program running. '
              'The exception information is "%s". '
              'For details about the error cause, please see %s.' % (e, config['logfile']),
              file=sys.stderr, flush=True)
        return -1
