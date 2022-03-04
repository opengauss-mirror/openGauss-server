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

import numpy as np

from .env import Env, Box
from .exceptions import ExecutionError


class DB_Env(Env):
    def __init__(self, db, benchmark, recorder, drop_cache, mem_penalty):
        """
        This class inherits the implementation of the `Env` class of the deep
        reinforcement learning experiment environment library `gym` so it is compatible
        with the reinforcement learning ecosystem to facilitate integration with third parties.

        :param db: DB_Agent object.
        :param benchmark: Get benchmark instance from the function `benchmark.get_benchmark_instance()`.
        :param recorder: Recorder object. Record information for each tuning step.
        :param drop_cache: Indicates whether to drop cache. Should have administrator permission to drop cache.
        :param mem_penalty: This parameter is set to prevent excessive memory usage caused by unlimited memory usage.
                            For more information, see the keyword "penalty item".
        """
        self.db = db
        self.bm = benchmark
        self.recorder = recorder
        self.drop_cache = drop_cache
        self.mem_penalty = mem_penalty

        self.nb_actions = None
        self.nb_state = None
        self.action_state = None
        self.action_space = None
        self.observation_space = None

    def set_tuning_knobs(self, knobs):
        self.db.set_tuning_knobs(knobs)
        self.nb_actions = len(self.db.ordered_knob_list)
        self.nb_state = len(self.db.ordered_knob_list + self.db.metric.get_internal_state())
        self.action_space = Box(low=0, high=1, shape=(self.nb_actions,), dtype=np.float32)
        self.observation_space = Box(low=0, high=1, shape=(self.nb_state,), dtype=np.float32)

    def step(self, action, delta=False):
        """
        Each step means a calibration attempt.
        To obtain the final tuning result, we need to perform iterations.
        After multiple attempts, we could find the optimal knob settings.

        :param action: Array-like knob settings.
        :param delta: Delta means relative change for actions.
        :return: Return Quadruplet: (observation, reward, done, info).
        """
        if delta:
            cur_knobs = self.db.get_knob_normalized_vector()
            new_knobs = np.add(cur_knobs, action).clip(0, 1)
            self.db.set_knob_normalized_vector(new_knobs)
        else:
            action = np.array(action).clip(0, 1)
            self.db.set_knob_normalized_vector(action)

        if self.drop_cache:
            self.db.drop_cache()

        try:
            obs = self._get_obs()
            score = self.perf(self.bm)
            used_mem = self.db.metric.used_mem
            reward = score - self.mem_penalty * used_mem  # Use the memory usage as a regular term.
        except ExecutionError as e:
            logging.error('An error errored after changed the settings, '
                          'hence rollback to the default settings. The error is %s.', e)
            self.reset()  # Rollback to default setting
            obs = [.0 for _ in self._get_obs()]  # Pad 0 to the observation vector.
            score = .0
            used_mem = self.db.metric.os_mem_total
            # This value is minimal theoretically, so that regard it as a penalty term.
            reward = score - self.mem_penalty * used_mem

        # Record each tuning process.
        knob_values = [self.db.knobs[name].to_string(value) for name, value in zip(self.db.ordered_knob_list, action)]
        self.recorder.record(score, used_mem, reward, names=self.db.ordered_knob_list, values=knob_values)

        self.recorder.prompt_message('Database metrics: %s.', obs)
        self.recorder.prompt_message('Benchmark score: %f, used mem: %d kB, reward: %f.', score, used_mem, reward)

        return obs, reward, False, {}

    def reset(self):
        self.db.set_knob_normalized_vector(self.db.get_default_normalized_vector())
        self.db.reset_state()
        return self._get_obs()

    def _get_obs(self):
        obs = self.db.get_knob_normalized_vector() + self.db.metric.get_internal_state()
        return obs

    def close(self):
        self.db.ssh.close()

    def perf(self, bm):
        score = bm(self.db.ssh)
        # Try to drop_cache. If drop_cache is available, the benchmark score is much more stable.
        self.db.drop_cache()
        return score
