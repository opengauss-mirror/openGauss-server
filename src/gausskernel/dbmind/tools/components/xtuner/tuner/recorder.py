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
import csv

DELIMITER = ","


class Recorder:
    def __init__(self, filepath):
        """
        Record each tuning process and write it to a file.
        """
        self._fd = open(filepath, 'w', newline='')
        self.writer = csv.writer(self._fd, delimiter=DELIMITER,
                                 quotechar='\\', quoting=csv.QUOTE_MINIMAL)

        # Record the information of best knobs.
        self.best_id = None
        self.names = None
        self.best_values = None
        self.best_reward = None

        self.current_id = 0

    def prompt_message(self, msg, *args, **kwargs):
        logging.info("[Recorder %d]: " + msg, self.current_id, *args, **kwargs)

    def record(self, score, used_mem, reward, names, values):
        """
        Record the reward value and knobs of the step,
        and update the maximum reward value and the corresponding knobs.

        :values: A list contains each knob value. The knob value is str type, not denormalized numeric.
        """
        _names = tuple(names)
        _values = tuple(values)
        if self.current_id == 0:
            header = ("id",) + _names + ("score", "used_mem", "reward", "best_reward", "best_id")
            self.writer.writerow(header)
            self.best_id = 0
            self.names = names
            self.best_values = values
            self.best_reward = reward

        if reward >= self.best_reward:
            self.best_id = self.current_id
            self.best_values = values
            self.best_reward = reward

        record = (self.current_id,) + _values + (score, used_mem, reward, self.best_reward, self.best_id)
        self.writer.writerow(record)
        self._fd.flush()

        self.current_id += 1

    def give_best(self, rk):
        """
        Give the knobs with the maximum reward value to the parameter RecommendKnobs (RK) object.
        So that RK can update itself with the passed knobs.
        """
        logging.info("The tuning process is finished. The best reward is %f, and best knobs (%s) are %s.",
                     self.best_reward, self.names, self.best_values)

        for name, value in zip(self.names, self.best_values):
            knob = rk[name]
            knob.current = knob.to_numeric(value)  # self.current is always a normalized numeric.

    def __del__(self):
        self._fd.flush()
        self._fd.close()
