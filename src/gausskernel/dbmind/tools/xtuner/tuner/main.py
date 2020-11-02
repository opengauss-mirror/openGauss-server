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

import argparse
from getpass import getpass

import benchmark
import knobs
from algorithms.pso import Pso
from db_env import DB_Env
from utils import SysLogger, Recorder

__version__ = '1.0.1'

"""The following are preset parameters, 
and it is not recommended to modify for green hands unless you have any special purpose."""
STEPS = 100
TEST_EPISODES = 1
NB_MAX_EPISODE_STEPS = 10
LOGFILE = 'log/opengauss_tuner.log'
RECORDER_FILE = 'log/recorder.log'


def main():
    # fetch arguments
    args = parse_args()
    # initialize logger
    logger = SysLogger(LOGFILE)
    recorder = Recorder(RECORDER_FILE)
    logger.info('starting...')
    rl_knobs = knobs.get_rl_knobs(args.scenario)
    pso_knobs = knobs.get_pso_knobs(args.scenario)
    bm = benchmark.get_benchmark_instance(args.benchmark)
    env = DB_Env(db_info=args.db_info, benchmark=bm, recorder=recorder)

    if len(rl_knobs) == 0 and args.is_train:
        print(SysLogger)
        logger.print('current mode is training, so you must set reinforcement learning knobs.',
                     fd=SysLogger.stderr)
        return -1

    # reinforcement learning
    if len(rl_knobs) > 0:
        env.set_tuning_knobs(rl_knobs)
        # lazy loading. Because loading tensorflow has to cost too much time.
        from algorithms.rl_agent import RLAgent
        rl = RLAgent(env, agent='ddpg')

        if args.is_train:
            rl.fit(STEPS, nb_max_episode_steps=NB_MAX_EPISODE_STEPS)
            rl.save(args.model_path)
            logger.print('saved model at %s' % args.model_path)
            return 0  # training mode stop here.
        if not args.model_path:
             from sys import stderr
             print('have no model path, you can use --model-path argument.', file=stderr, flush=True)
             exit(-1)
        rl.load(args.model_path)
        rl.test(TEST_EPISODES, nb_max_episode_steps=NB_MAX_EPISODE_STEPS)

        recorder.write_best_val('reward')
    # heuristic algorithm
    if len(pso_knobs) > 0:
        env.set_tuning_knobs(pso_knobs)

        def heuristic_callback(v):
            s, r, d, _ = env.step(v, False)
            return -r  # - reward

        pso = Pso(
            func=heuristic_callback,
            dim=len(pso_knobs),
            particle_nums=3, max_iteration=100, x_min=0, x_max=1, max_vel=0.5
        )
        pso.update()

    # if you have other approaches, you can code here.

        recorder.write_best_val('reward')
    logger.print('please see result at logfile: %s.' % RECORDER_FILE)


def check_port(port):
    return 0 < port <= 65535


def check_path_valid(obtainpath):
    """
    function: check path valid
    input : envValue
    output: NA
    """
    PATH_CHECK_LIST = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"", "{", "}", "(", ")", "[", "]", "~", "*",
                       "?", "!", "\n"]
    if obtainpath.strip() == "":
        return
    for rac in PATH_CHECK_LIST:
        flag = obtainpath.find(rac)
        if flag >= 0:
            return False
    return True


def parse_args():
    parser = argparse.ArgumentParser(description='X-Tuner: a self-tuning toolkit for OpenGauss.')
    parser.add_argument('-m', '--mode', required=True, choices=['train', 'tune'], help='train a reinforcement learning model or '
                                                                        'tune by your trained model.')
    parser.add_argument('-f', '--config-file', help='you can pass a config file path or you should manually '
                                                    'set database information.')
    parser.add_argument('--db-name', help='database name.')
    parser.add_argument('--db-user', help='database user name.')
    parser.add_argument('--port', type=int, help='database connection port.')
    parser.add_argument('--host', help='where did your database install on?')
    parser.add_argument('--host-user', help='user name of the host where your database installed on.')
    parser.add_argument('--host-ssh-port', default=22, help='host ssh port.')
    parser.add_argument('--scenario', required=True, choices=['ap', 'tp', 'htap'], default='htap')
    parser.add_argument('--benchmark', required=True, default='tpcc')
    parser.add_argument('--model-path', help='the place where you want to save model weights to or load model weights '
                                             'from.')
    parser.add_argument('-v', '--version', action='version', help='show version.')

    parser.version = __version__
    
    class ParsedArgs:
        def __init__(self, args):
            self.print_warn()
            self.db_info = None
            self.is_train = False
            self.scenario = None
            self.model_path = None
            self.benchmark = None

            if args.config_file:
                if not check_path_valid(args.config_file):
                    from sys import stderr
                    print('detect illegal json_path.', file=stderr, flush=True)
                    exit(-1)
                from json import load
                with open(args.config_file, mode='r', errors='ignore') as fd:
                    self.db_info = load(fd)
                    if not set(self.db_info.keys()) == set(['db_name', 'db_user', 'db_user_pwd', 'host', \
                                                            'host_user', 'host_user_pwd', 'port']):
                        from sys import stderr
                        print('lack of information in json file.', file=stderr, flush=True)
                        exit(-1)
            else:
                db_user_pwd = getpass("please input the password of database: ")
                host_user_pwd = getpass("please input the password of host: ")
                self.db_info = {
                    'db_name': args.db_name,
                    'db_user': args.db_user,
                    'db_user_pwd': db_user_pwd,
                    'host': args.host,
                    'host_user': args.host_user,
                    'host_user_pwd': host_user_pwd,
                    'port': args.port,
                    'ssh_port': args.host_ssh_port
                }
            self.is_train = args.mode == 'train'
            self.scenario = args.scenario
            self.model_path = args.model_path
            self.benchmark = args.benchmark
            
            for k, v in self.db_info.items():
                if not v:
                    from sys import stderr
                    print('need database authorization information.', file=stderr, flush=True)
                    parser.print_usage()
                    exit(-1)
                else:
                    if k in ['db_name', 'db_user', 'host_user']:
                        if not check_path_valid(v):
                            from sys import stderr
                            print('detect illegal input.', file=stderr, flush=True)
                            exit(-1)
                    if k in ['port', 'ssh_port']:
                        if not check_port(v):
                            from sys import stderr
                            print('detect illegal port.', file=stderr, flush=True)
                            exit(-1)
                            
            if self.is_train and not self.model_path:
                from sys import stderr
                print('have no model filepath input, you can use --model-path argument.', file=stderr, flush=True)
                exit(-1) 
 
        @staticmethod
        def print_warn():
            hint = '\033[32;1mwarning: Database may reboot several times during Tuning, continue or not [yes|no]:\033[0m '
            run_tuner = input(hint)
            while True:
                if run_tuner.lower() == 'no':
                    exit(0)
                elif run_tuner.lower() == 'yes':
                    return
                else:
                    run_tuner = input('\033[32;1mplease input yes or no:\033[0m ')

    return ParsedArgs(parser.parse_args())


if __name__ == '__main__':
    main()

