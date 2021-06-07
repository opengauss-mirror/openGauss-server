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

from tensorflow.keras import Model
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Flatten, Dense, Activation, Input, Concatenate
from tensorflow.keras.optimizers import Adam
from rl.agents import DDPGAgent
from rl.agents import DQNAgent
from rl.memory import SequentialMemory
from rl.policy import BoltzmannQPolicy
from rl.random import OrnsteinUhlenbeckProcess


class RLAgent:
    def __init__(self, env, alg='ddpg'):
        self.env = env
        nb_actions = env.action_space.shape[0]
        nb_states = env.observation_space.shape[0]

        if alg == 'ddpg':
            self.agent = self._build_ddpg(nb_actions, nb_states)
        elif alg == 'dpn':
            self.agent = self._build_dqn(nb_actions, nb_states)
        else:
            raise ValueError('Can not support this reinforcement learning algorithm.')

    @staticmethod
    # not regression test on DQN, suggest to choose DDPG.
    def _build_dqn(nb_actions, nb_states):
        # build network
        model = Sequential()
        model.add(Flatten(input_shape=(1, nb_states)))
        model.add(Dense(16))
        model.add(Activation('relu'))
        model.add(Dense(16))
        model.add(Activation('relu'))
        model.add(Dense(nb_actions, activation='linear'))

        # build alg
        memory = SequentialMemory(limit=10240, window_length=1)
        policy = BoltzmannQPolicy()
        dqn = DQNAgent(model=model, nb_actions=nb_actions, memory=memory,
                       nb_steps_warmup=10, enable_dueling_network=True, dueling_type='avg',
                       target_model_update=1e-2, policy=policy)
        dqn.compile(Adam(), metrics=['mae'])

        return dqn

    @staticmethod
    def _build_ddpg(nb_actions, nb_states):
        # build an actor network
        actor = Sequential()
        actor.add(Flatten(input_shape=(1, nb_states)))
        actor.add(Dense(16))
        actor.add(Activation('relu'))
        actor.add(Dense(16))
        actor.add(Activation('relu'))
        actor.add(Dense(nb_actions))
        actor.add(Activation('sigmoid'))

        # build a critic network
        action_input = Input(shape=(nb_actions,), name='action_input')
        observation_input = Input(shape=(1, nb_states), name='observation_input')
        flattened_observation = Flatten()(observation_input)
        x = Concatenate()([action_input, flattened_observation])
        x = Dense(32)(x)
        x = Activation('relu')(x)
        x = Dense(32)(x)
        x = Activation('relu')(x)
        x = Dense(1)(x)
        x = Activation('linear')(x)
        critic = Model(inputs=[action_input, observation_input], outputs=x)

        # tricks:
        memory = SequentialMemory(limit=10240, window_length=1)
        oup = OrnsteinUhlenbeckProcess(size=nb_actions, theta=.15, mu=0., sigma=.3)

        # build ddpg alg
        ddpg = DDPGAgent(nb_actions=nb_actions, actor=actor, critic=critic, critic_action_input=action_input,
                         memory=memory, nb_steps_warmup_actor=100, nb_steps_warmup_critic=100,
                         random_process=oup, gamma=.99, target_model_update=1e-3)
        ddpg.compile(Adam(), metrics=['mae'])
        return ddpg

    def fit(self, steps, nb_max_episode_steps=100, verbose=0):
        self.agent.fit(self.env, nb_steps=steps, nb_max_episode_steps=nb_max_episode_steps, verbose=verbose)

    def save(self, filepath):
        self.agent.save_weights(filepath, overwrite=True)

    def load(self, filepath):
        self.agent.load_weights(filepath)

    def test(self, episodes, nb_max_episode_steps=10, verbose=0):
        self.agent.test(self.env, nb_episodes=episodes, nb_max_episode_steps=nb_max_episode_steps, verbose=verbose)
