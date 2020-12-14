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


class Recorder:
    def __init__(self, filepath, verbose=True):
        """
        Record each tuning process and write it to a file.
        """
        logger = logging.getLogger('recorder')
        fmt = logging.Formatter("%(asctime)s: %(message)s")

        file_handler = logging.FileHandler(filepath, mode='w')  # no appending
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

        if verbose:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(fmt)
            logger.addHandler(stream_handler)

        logger.setLevel(logging.INFO)

        self.logger = logger
        self.logger.info('Recorder is starting.')
        self.best_reward = None
        self.count = 0

    def prompt_message(self, msg, *args, **kwargs):
        self.logger.info('[%d] ' % self.count + msg, *args, **kwargs)
        logging.info('[Recorder %d]: ' + msg, self.count, *args, **kwargs)

    def record(self, reward, knobs):
        """
        Record the reward value and knobs of the step,
        and update the maximum reward value and the corresponding knobs.
        """
        record = (reward, knobs)
        if self.best_reward is None:
            self.best_reward = record
        else:
            self.best_reward = max(record, self.best_reward, key=lambda r: r[0])

        self.logger.info('[%d] Current reward is %f, knobs: %s.', self.count, reward, knobs)
        self.logger.info('[%d] Best reward is %f, knobs: %s.', self.count, self.best_reward[0], self.best_reward[1])

        self.count += 1

    def give_best(self, rk):
        """
        Give the knobs with the maximum reward value to the parameter RecommendKnobs (RK) object.
        So that RK can update itself with the passed knobs.
        """
        reward, best_knobs = self.best_reward
        self.logger.info('The tuning process is complete. The best reward is %f, best knobs are:\n%s.',
                         reward, best_knobs)

        for name, setting in best_knobs.items():
            rk[name].default = setting
