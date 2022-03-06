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

Classes in the file are intended to be compatible with gym.
"""

import numpy as np


class Box:
    def __init__(self, low, high, shape, dtype=np.float32):
        self.dtype = dtype

        if not (np.isscalar(low) and np.isscalar(high)):
            raise ValueError()
        self.low = np.full(shape, low)
        self.high = np.full(shape, high)
        self.shape = shape

        self.low = self.low.astype(self.dtype)
        self.high = self.high.astype(self.dtype)

        self.bounded_below = np.inf > - self.low
        self.bounded_above = np.inf > self.high

        self.np_random = None
        self.seed()

    def seed(self, seed=None):
        if seed is None:
            import os
            b = os.urandom(1)
            seed = int.from_bytes(b, byteorder='big')

        self.np_random = np.random
        self.np_random.seed(seed)

    def is_bounded(self, manner="both"):
        below = np.all(self.bounded_below)
        above = np.all(self.bounded_above)
        if manner == "below":
            return below
        elif manner == "above":
            return above
        else:
            return below and above

    def sample(self):
        if self.dtype.kind == 'i':
            high = self.high.astype('int64') + 1
        else:
            high = self.high
        low = self.low

        sample = np.zeros(self.shape)

        # indexes
        unbounded = ~self.bounded_below & ~self.bounded_above
        upp_bounded = ~self.bounded_below & self.bounded_above
        low_bounded = self.bounded_below & ~self.bounded_above
        bounded = self.bounded_below & self.bounded_above

        sample[unbounded] = self.np_random.normal(
            size=unbounded[unbounded].shape)

        sample[low_bounded] = self.np_random.exponential(
            size=low_bounded[low_bounded].shape) + low[low_bounded]

        sample[upp_bounded] = -self.np_random.exponential(
            size=upp_bounded[upp_bounded].shape) + self.high[upp_bounded]

        sample[bounded] = self.np_random.uniform(low=low[bounded],
                                                 high=high[bounded],
                                                 size=bounded[bounded].shape)
        if self.dtype.kind == 'i':
            sample = np.floor(sample)

        return sample.astype(self.dtype)

    def contains(self, x):
        if isinstance(x, list) or isinstance(x, tuple):
            x = np.array(x)
        return x.shape == self.shape and np.all(x <= self.high) and np.all(x >= self.low)

    def __repr__(self):
        return "Box<%s>" % str(self.shape)

    def __eq__(self, other):
        return isinstance(other, Box) \
               and (self.shape == other.shape) \
               and np.allclose(self.low, other.low, equal_nan=True) \
               and np.allclose(self.high, other.high, equal_nan=True)


class Env:
    reward_range = (-float('inf'), float('inf'))
    spec = None

    action_space = None
    observation_space = None

    def step(self, action):
        pass

    def reset(self):
        pass

    def render(self, mode='human'):
        pass

    def close(self):
        pass

    def seed(self, seed=None):
        pass

    @property
    def unwrapped(self):
        return self

    def __str__(self):
        return '{}: reward range: {}.'.format(type(self).__name__,
                                              Env.reward_range)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
