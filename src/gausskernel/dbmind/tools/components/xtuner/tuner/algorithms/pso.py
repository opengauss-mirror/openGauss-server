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


PATTERN = '|\t%s\t|\t%s\t|\t%s\t|'

class Particle:
    def __init__(self, position, velocity, best_position, fitness):
        self.position = position
        self.velocity = velocity
        self.best_position = best_position
        self.fitness = fitness


class Pso:
    def __init__(self, func, dim, particle_nums, max_iteration, x_min, x_max, max_vel,
                 c1=2, c2=2, w_max=1, w_min=0.1):
        # hyper-parameters
        self.c1 = c1
        self.c2 = c2
        self.w_max = w_max
        self.w_min = w_min
        self.w = self.w_max
        self.x_min = x_min
        self.x_max = x_max
        self.max_vel = max_vel  # maximum velocity

        # initialize fields
        self.dim = dim  # particle dimension
        self.best_fitness = float('inf')
        self.best_position = np.zeros((dim,))
        self.func = func
        self.max_iteration = max_iteration  # limited iterations
        self.iteration_count = 0

        # initialize a particle list
        self.fitness_val_list = list()  # best fitness value from each iteration
        self.particles = list()
        for _ in range(particle_nums):
            position = np.random.uniform(x_min, x_max, dim)
            velocity = np.random.uniform(-max_vel, max_vel, dim)
            best_position = np.zeros((dim,))  # best position of particle
            fitness = func(position)  # fitness function value
            particle = Particle(position, velocity, best_position, fitness)
            self.particles.append(particle)

    def _update_velocity(self, particle):
        # w is a factor of inertia, we implement it adapting for iterations.
        self.w = self.w * (1 - 0.1 * self.iteration_count / self.max_iteration)
        self.w = np.clip(self.w, self.w_min, self.w_max)

        # mutation
        F = 1
        CR = 0.1
        X = list(map(lambda x: x.best_position, np.random.choice(self.particles, 2)))
        m = particle.best_position + F * (X[0] - X[1])
        mutated_idx = np.random.uniform(size=self.dim) <= CR
        pbest = particle.best_position.copy()
        pbest[mutated_idx] = m[mutated_idx]

        # updating function:
        new_velocity = self.w * particle.velocity + self.c1 * np.random.random() * (pbest - particle.position) + \
                       self.c2 * np.random.random() * (self.best_position - particle.position)
        particle.velocity = np.clip(new_velocity, -self.max_vel, self.max_vel)

    def _update_position(self, particle):
        """
        update current positions
        :param particle: particle object
        :return: None
        """
        new_position = particle.position + particle.velocity
        particle.position = np.clip(new_position, a_min=self.x_min, a_max=self.x_max)
        new_fitness = self.func(particle.position)

        if new_fitness < particle.fitness:
            particle.fitness = new_fitness
            particle.best_position = particle.position

        if new_fitness < self.best_fitness:
            self.best_fitness = new_fitness
            self.best_position = particle.position

    def update_one_step(self):
        for i, particle in enumerate(self.particles):
            self._update_velocity(particle)  # update velocity
            self._update_position(particle)  # update position
            print(PATTERN % ('%s-%s' % (self.iteration_count, i), self.best_fitness, self.best_position), flush=True)
        self.iteration_count += 1
        self.fitness_val_list.append(self.best_fitness)

    def minimize(self):
        # Print a header
        print(PATTERN % ('iter', 'best_fitness', 'best_position'), flush=True)
        for i in range(self.max_iteration):
            self.update_one_step()
        return self.best_fitness, self.best_position

    def get_fitness_val_list(self):
        return self.fitness_val_list
