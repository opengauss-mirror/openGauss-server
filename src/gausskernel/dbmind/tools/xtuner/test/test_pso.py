# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# -------------------------------------------------------------------------
#
# test_pso.py
#
# IDENTIFICATION
#    src/gausskernel/dbmind/xtuner/test/test_pso.py
#
# -------------------------------------------------------------------------


import numpy as np

from algorithms.pso import Pso


def quadratic_function(X):
    return (X ** 2).mean()


def cubic_function(X):
    return (X ** 3).mean()


def diy_function1(X):
    scale = X.max() - X.min()
    return np.dot(np.ones(shape=X.shape), X) + scale * np.sin(2 * np.pi / (X.size / 4) * X).max()


def diy_function2(X):
    y = (X[0] ** 2 - X[1] ** 2) / 2
    return y


def test_function(dim, optimal, func, x_min, x_max):
    pso = Pso(func=func, dim=dim, particle_nums=5, max_iteration=100, max_vel=5, x_min=x_min, x_max=x_max)
    best_val, best_X = pso.update()
    print("function: %s, best val: %d, best X: %s." % (func.__name__, best_val, best_X))
    print("fitness list: %s." % pso.fitness_val_list)
    if np.abs(optimal - best_val) > np.abs(0.1 * optimal):
        raise AssertionError


def main():
    # test one dimension
    test_function(1, 0, quadratic_function, 0, 10)
    test_function(1, 0, cubic_function, 0, 10)
    test_function(1, 0, diy_function1, 0, 10)

    # test high dimension
    test_function(10, 0, quadratic_function, 0, 10)
    test_function(10, 0, cubic_function, 0, 10)
    test_function(10, 0, diy_function1, 0, 10)

    test_function(2, -50, diy_function2, 0, 10)


if __name__ == '__main__':
    main()
