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

import logging
import os
import signal
from logging import handlers

from tuner import benchmark
from tuner.character import WORKLOAD_TYPE
from tuner.db_agent import new_db_agent
from tuner.db_env import DB_Env
from tuner.knob import load_knobs_from_json_file
from tuner.recommend import recommend_knobs
from tuner.recorder import Recorder
from tuner.exceptions import ConfigureError
from tuner.utils import YELLOW_FMT


def prompt_restart_risks():
    hint = "WARN: The database may restart several times during tuning, continue or not [yes|no]:"
    answer = input(YELLOW_FMT.format(hint))
    while True:
        if answer.lower() == 'no':
            print(
                YELLOW_FMT.format(
                    "FATAL: Tuning program will exit because you are not currently allowed to interrupt business."
                )
            )
            exit(0)
        elif answer.lower() == 'yes':
            return
        else:
            answer = input(YELLOW_FMT.format("Please input yes or no:"))


def set_logger(filename):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    dirname = os.path.dirname(filename)
    if not os.path.exists(dirname):
        os.makedirs(dirname, mode=0o700)

    stream_hdlr = logging.StreamHandler()
    stream_hdlr.setLevel(logging.WARNING)

    rota_hdlr = handlers.RotatingFileHandler(filename=filename,
                                             maxBytes=1024 * 1024 * 100,
                                             backupCount=5)
    rota_hdlr.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
                                  '%m-%d %H:%M:%S')
    rota_hdlr.setFormatter(formatter)

    logger.addHandler(stream_hdlr)
    logger.addHandler(rota_hdlr)


def procedure_main(mode, db_info, config):
    """
    This is the real entry for tuning programs.

    :param mode: Three modes: tune, train and recommend.
    :param db_info: Dict data structure. db_info is used to store information about
                    the database to be connected and is transferred through the command line
                    or configuration file.
    :param config: Information read from xtuner.conf.
    :return: Exit status code.
    """
    # Set the minimum permission on the output files.
    os.umask(0o0077)
    # Initialize logger.
    set_logger(config['logfile'])
    logging.info('Starting... (mode: %s)', mode)
    db_agent = new_db_agent(db_info)

    # Clarify the scenario:
    if config['scenario'] in WORKLOAD_TYPE.TYPES:
        db_agent.metric.set_scenario(config['scenario'])
    else:
        config['scenario'] = db_agent.metric.workload_type
    # Clarify tune strategy:
    if config['tune_strategy'] == 'auto':
        # If more iterations are allowed, reinforcement learning is preferred.
        if config['rl_steps'] * config['max_episode_steps'] > 1500:
            config['tune_strategy'] = 'rl'
        else:
            config['tune_strategy'] = 'gop'

    logging.info("Configurations: %s.", config)
    if config['tuning_list'].strip() != '' and mode != 'recommend':
        print("You have configured the tuning list, so use this list to tune.")
        knobs = load_knobs_from_json_file(config['tuning_list'])
    else:
        print("Start to recommend knobs. Just a moment, please.")
        knobs = recommend_knobs(mode, db_agent.metric)
    if not knobs:
        logging.fatal('No recommended best_knobs for the database. Stop the execution.')
        return -1

    # If the recommend mode is not used,
    # the benchmark running and best_knobs tuning process need to be iterated.
    if mode != 'recommend':
        prompt_restart_risks()  # Users need to be informed of risks.

        recorder = Recorder(config['recorder_file'])
        bm = benchmark.get_benchmark_instance(config['benchmark_script'],
                                              config['benchmark_path'],
                                              config['benchmark_cmd'],
                                              db_info)
        env = DB_Env(db_agent, benchmark=bm, recorder=recorder,
                     drop_cache=config['drop_cache'],
                     mem_penalty=config['used_mem_penalty_term'])
        env.set_tuning_knobs(knobs)

        print('The benchmark will start to run iteratively. '
              'This process may take a long time. Please wait a moment.')
        if mode == 'train':
            rl_model('train', env, config)
        elif mode == 'tune':
            # Run once the performance under the default knob configuration.
            # Its id is 0, aka the first one.
            original_knobs = db_agent.get_default_normalized_vector()
            env.step(original_knobs)

            try:
                if config['tune_strategy'] == 'rl':
                    rl_model('tune', env, config)
                elif config['tune_strategy'] == 'gop':
                    global_search(env, config)
                else:
                    raise ValueError('Incorrect tune strategy: %s.' % config['tune_strategy'])
    
            except KeyboardInterrupt:
                signal.signal(signal.SIGINT, signal.SIG_IGN)
                print("Trigger an interrupt via the keyboard. "
                      "Continue to generate current tuning results.")

            # Rollback/reset to the original/initial knobs while the tuning process is finished.
            db_agent.set_knob_normalized_vector(original_knobs)
            # Modify the variable `knobs` with tuned result.
            recorder.give_best(knobs)
        else:
            raise ValueError('Incorrect mode value: %s.' % mode)

    # After the above process is executed, the tuned best_knobs are output.
    knobs.output_formatted_knobs()
    if config['output_tuning_result'] != '':
        with open(config['output_tuning_result'], 'w+') as fp:
            # In reinforcement learning training mode,
            # only the training knob list is dumped, but the recommended knob result is not dumped.
            # This is because, in tune mode of reinforcement learning,
            # users can directly load the dumped file as the knob tuning list.
            knobs.dump(fp, dump_report_knobs=mode != 'train')
    logging.info('X-Tuner is executed and ready to exit. '
                 'Please refer to the log for details of the execution process.')
    return 0


def rl_model(mode, env, config):
    # Lazy loading. Because loading Tensorflow takes a long time.
    from tuner.algorithms.rl_agent import RLAgent
    rl = RLAgent(env, alg=config['rl_algorithm'])
    if mode == 'train':
        logging.warning('The list of tuned knobs in the training mode '
                        'based on the reinforcement learning algorithm must be the same as '
                        'that in the tuning mode. ')
        rl.fit(config['rl_steps'], nb_max_episode_steps=config['max_episode_steps'])
        rl.save(config['rl_model_path'])
        logging.info('Saved reinforcement learning model at %s.', config['rl_model_path'])
    elif mode == 'tune':
        if config['tuning_list'] == '':
            raise ConfigureError('In the current mode, you must set the value of tuning_list '
                                 'in the configuration file. '
                                 'Besides, the list of tuned knobs in the training mode '
                                 'based on the reinforcement learning algorithm must be the same as '
                                 'that in the tuning mode.')
        logging.warning('The list of tuned knobs in the training mode '
                        'based on the reinforcement learning algorithm must be the same as '
                        'that in the tuning mode. Please verify that is correct.')

        rl.load(config['rl_model_path'])
        rl.test(config['test_episode'], nb_max_episode_steps=config['max_episode_steps'])
    else:
        raise ValueError('Incorrect mode value: %s.' % mode)


def global_search(env, config):
    method = config['gop_algorithm']
    if method == 'bayes':
        from bayes_opt import BayesianOptimization

        action = [0 for _ in range(env.nb_actions)]
        pbound = {name: (0, 1) for name in env.db.ordered_knob_list}

        def performance_function(**params):
            assert len(params) == env.nb_actions, 'Failed to check the input feature dimension.'

            for name, val in params.items():
                index = env.db.ordered_knob_list.index(name)
                action[index] = val

            s, r, d, _ = env.step(action)
            return r  # Wishes to maximize.

        optimizer = BayesianOptimization(
            f=performance_function,
            pbounds=pbound
        )
        optimizer.maximize(
            n_iter=config['max_iterations']
        )
    elif method == 'pso':
        from tuner.algorithms.pso import Pso

        def performance_function(v):
            s, r, d, _ = env.step(v)
            return -r  # Use -reward because PSO wishes to minimize.

        pso = Pso(
            func=performance_function,
            dim=env.nb_actions,
            particle_nums=config['particle_nums'],
            # max_iterations on the PSO indicates the maximum number of iterations per particle,
            # so it must be divided by the number of particles to be consistent with Bayes.
            max_iteration=config['max_iterations'] // config['particle_nums'],
            x_min=0, x_max=1, max_vel=0.5
        )
        pso.minimize()
    else:
        raise ValueError('Incorrect method value: %s.' % method)
