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

import numpy as np

from db_agent import DB_Agent
from env import Env, Box


class DB_Env(Env):
    def __init__(self, db_info, benchmark, recorder):
        self.db = DB_Agent(db_name=db_info['db_name'],
                           db_port=db_info['port'],
                           db_user=db_info['db_user'],
                           db_user_pwd=db_info['db_user_pwd'],
                           host=db_info['host'],
                           host_user=db_info['host_user'],
                           host_user_pwd=db_info['host_user_pwd'])
        self.bm = benchmark
        self.recorder = recorder

        self.nb_actions = None
        self.nb_state = None
        self.action_state = None
        self.action_space = None
        self.observation_space = None

    def set_tuning_knobs(self, knob_dict):
        self.db.set_tuning_knobs(knob_dict)
        self.nb_actions = len(self.db.orderly_knob_list)
        self.nb_state = len(self.db.orderly_knob_list + self.db.get_internal_state())
        self.action_space = Box(low=0, high=1, shape=(self.nb_actions,), dtype=np.float32)
        self.observation_space = Box(low=0, high=1, shape=(self.nb_state,), dtype=np.float32)

    def step(self, action, is_delta=False):
        """
        :param is_delta: delta means relative change
        :param action: array-like knob delta values
        :return: (observation, reward, done, info)
        """
        if is_delta:
            cur_knobs = self.db.get_knob_normalized_vector()
            new_knobs = np.add(cur_knobs, action).clip(0, 1)
            self.db.set_knob_normalized_vector(new_knobs)
        else:
            action = action.clip(0, 1)
            self.db.set_knob_normalized_vector(action)

        metric = self.perf(self.bm)
        used_mem = self.db.get_used_mem()
        reward = metric - 0.01 * used_mem  # regularizer

        # record
        knob_dict = {k: self.db.get_knob_value(k) for k in self.db.orderly_knob_list}
        self.recorder.write_int(val=reward, name='reward', info=knob_dict)
        self.recorder.write_text('metric: %f, mem: %f.' % (metric, used_mem))
        info = dict()
        return self._get_obs(), reward, False, info  #

    def reset(self):
        self.db.reset_state()
        self.db.set_knob_normalized_vector(np.random.random(self.nb_actions))  # maybe we can have more samples.
        return self._get_obs()

    def _get_obs(self):
        obs = self.db.get_knob_normalized_vector() + self.db.get_internal_state()
        return obs

    def close(self):
        self.db.ssh.close()

    def perf(self, bm):
        return bm(self.db.ssh)
