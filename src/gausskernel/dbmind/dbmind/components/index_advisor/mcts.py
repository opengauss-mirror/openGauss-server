# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

import sys
import math
import random
import copy
from collections import defaultdict
try:
    from utils import infer_workload_benefit
except ImportError:
    from .utils import infer_workload_benefit

TOTAL_STORAGE = 0
STORAGE_THRESHOLD = 0
AVAILABLE_CHOICES = None
ATOMIC_CHOICES = None
WORKLOAD = None
MAX_INDEX_NUM = 0
CANDIDATE_SUBSET = defaultdict(list)
CANDIDATE_SUBSET_BENEFIT = defaultdict(list)


def find_best_benefit(choice):
    if choice[-1] in CANDIDATE_SUBSET.keys() and set(choice[:-1]) in CANDIDATE_SUBSET[choice[-1]]:
        return CANDIDATE_SUBSET_BENEFIT[choice[-1]][CANDIDATE_SUBSET[choice[-1]].index(set(choice[:-1]))]

    total_benefit = infer_workload_benefit(WORKLOAD, choice, ATOMIC_CHOICES)

    CANDIDATE_SUBSET[choice[-1]].append(set(choice[:-1]))
    CANDIDATE_SUBSET_BENEFIT[choice[-1]].append(total_benefit)
    return total_benefit


def get_diff(available_choices, choices):
    return set(available_choices).difference(set(choices))


class State(object):
    """
    The game state of the Monte Carlo tree search,
    the state data recorded under a specific Node node,
    including the current game score, the current number of game rounds,
    and the execution record from the beginning to the current.

    It is necessary to realize whether the current state has reached the end of the game state,
    and support the operation of randomly fetching from the Action collection.
    """

    def __init__(self):
        self.current_storage = 0.0
        self.current_benefit = 0.0
        # record the sum of choices up to the current state
        self.accumulation_choices = []
        # record available choices of current state
        self.available_choices = []
        self.displayable_choices = []

    def reset_state(self):
        self.set_available_choices(set(AVAILABLE_CHOICES).difference(self.accumulation_choices))

    def get_available_choices(self):
        return self.available_choices

    def set_available_choices(self, choices):
        self.available_choices = choices

    def get_current_storage(self):
        return self.current_storage

    def set_current_storage(self, value):
        self.current_storage = value

    def get_current_benefit(self):
        return self.current_benefit

    def set_current_benefit(self, value):
        self.current_benefit = value

    def get_accumulation_choices(self):
        return self.accumulation_choices

    def set_accumulation_choices(self, choices):
        self.accumulation_choices = choices

    def is_terminal(self):
        # The current node is a leaf node.
        return len(self.accumulation_choices) == MAX_INDEX_NUM

    def get_next_state_with_random_choice(self):
        # Ensure that the choices taken are not repeated.
        if not self.available_choices:
            return None
        random_choice = random.choice([choice for choice in self.available_choices])
        self.available_choices.remove(random_choice)
        choice = copy.copy(self.accumulation_choices)
        choice.append(random_choice)
        benefit = find_best_benefit(choice) + self.current_benefit
        # If the current choice does not satisfy restrictions, then continue to get the next choice.
        if benefit <= self.current_benefit or \
                self.current_storage + random_choice.get_storage() > STORAGE_THRESHOLD:
            return self.get_next_state_with_random_choice()

        next_state = State()
        # Initialize the properties of the new state.
        next_state.set_accumulation_choices(choice)
        next_state.set_current_benefit(benefit)
        next_state.set_current_storage(self.current_storage + random_choice.get_storage())
        next_state.set_available_choices(get_diff(AVAILABLE_CHOICES, choice))
        return next_state

    def __repr__(self):
        self.displayable_choices = ['{}: {}'.format(choice.get_table(), choice.get_columns())
                                    for choice in self.accumulation_choices]
        return "reward: {}, storage :{}, choices: {}".format(
            self.current_benefit, self.current_storage, self.displayable_choices)


class Node(object):
    """
    The Node of the Monte Carlo tree search tree contains the parent node and
     current point information,
    which is used to calculate the traversal times and quality value of the UCB,
    and the State of the Node selected by the game.
    """
    def __init__(self):
        self.visit_number = 0
        self.quality = 0.0

        self.parent = None
        self.children = []
        self.state = None

    def reset_node(self):
        self.visit_number = 0
        self.quality = 0.0
        self.children = []

    def get_parent(self):
        return self.parent

    def set_parent(self, parent):
        self.parent = parent

    def get_children(self):
        return self.children

    def expand_child(self, node):
        node.set_parent(self)
        self.children.append(node)

    def set_state(self, state):
        self.state = state

    def get_state(self):
        return self.state

    def get_visit_number(self):
        return self.visit_number

    def set_visit_number(self, number):
        self.visit_number = number

    def update_visit_number(self):
        self.visit_number += 1

    def get_quality_value(self):
        return self.quality

    def set_quality_value(self, value):
        self.quality = value

    def update_quality_value(self, reward):
        self.quality += reward

    def is_all_expand(self):
        return False if self.state.available_choices else True

    def __repr__(self):
        return "Node: {}, Q/N: {}/{}, State: {}".format(
            hash(self), self.quality, self.visit_number, self.state)


def tree_policy(node):
    """
    In the Selection and Expansion stages of the Monte Carlo tree search,
    the node that needs to be searched (such as the root node) is passed in,
    and the best node that needs to be expanded is returned
    according to the exploration/exploitation algorithm.
    Note that if the node is a leaf node, it will be returned directly.

    The basic strategy is first to find the child nodes that have not been selected
    and pick them randomly if there is more than one. Then, if both are selected,
    find the one with the largest UCB value that has weighed exploration/exploitation,
    and randomly choose if the UCB values are equal.
    """

    # Check if the current node is a leaf node.
    while node and not node.get_state().is_terminal():

        if node.is_all_expand():
            if not node.children:
                return node
            node = best_child(node, True)
        else:
            # Return the new sub-node.
            sub_node = expand(node)
            if sub_node:
                return sub_node
            # When no node satisfies the condition in the remaining nodes, this state is terminal.
            return node

    # Return the leaf node.
    return node


def default_policy(node):
    """
    In the Simulation stage of the Monte Carlo tree search, input a node that needs to be expanded,
    create a new node after a random operation, and return the reward of the new node.
    Note that the input node should not be a child node,
    and there are unexecuted Actions that can be expendable.

    The basic strategy is to choose the Action at random.
    """

    # Get the state of the game.
    current_state = copy.deepcopy(node.get_state())
    current_state.set_accumulation_choices(copy.copy(node.get_state().get_accumulation_choices()))
    current_state.set_available_choices(copy.copy(node.get_state().get_available_choices()))

    # Run until the game is over.
    while not current_state.is_terminal():
        # Pick one random action to play and get the next state.
        next_state = current_state.get_next_state_with_random_choice()
        if not next_state:
            break
        current_state = next_state

    final_state_reward = current_state.get_current_benefit()
    return final_state_reward


def expand(node):
    """
    Enter a node, expand a new node on the node, use the random method to execute the Action,
    and return the new node. Note that it is necessary to ensure that the newly
     added nodes differ from other node Action.
    """

    new_state = node.get_state().get_next_state_with_random_choice()
    if not new_state:
        return None
    sub_node = Node()
    sub_node.set_state(new_state)
    node.expand_child(sub_node)

    return sub_node


def best_child(node, is_exploration):
    """
    Using the UCB algorithm,
    select the child node with the highest score after weighing the exploration and exploitation.
    Note that the current Q-value score with the highest score is directly chosen if it is in the prediction stage.
    """

    best_score = -sys.maxsize
    best_sub_node = None

    # Travel all-sub nodes to find the best one.
    for sub_node in node.get_children():
        # The children nodes of the node contain the children node whose state is empty,
        # this kind of node comes from the node that does not meet the conditions.
        if not sub_node.get_state():
            continue
        # Explore constants.
        if is_exploration:
            C = 1 / math.sqrt(2.0)
        else:
            C = 0.0
        # UCB = quality / times + C * sqrt(2 * ln(total_times) / times)
        left = sub_node.get_quality_value() / sub_node.get_visit_number()
        right = 2.0 * math.log(node.get_visit_number()) / sub_node.get_visit_number()
        score = left + C * math.sqrt(right)
        # Get the maximum score while filtering nodes that do not meet the space constraints and
        # nodes that have no revenue
        if score > best_score \
                and sub_node.get_state().get_current_storage() <= STORAGE_THRESHOLD \
                and sub_node.get_state().get_current_benefit() > 0:
            best_sub_node = sub_node
            best_score = score

    return best_sub_node


def backpropagate(node, reward):
    """
    In the Backpropagation stage of the Monte Carlo tree search,
    input the node that needs to be expended and the reward of the newly executed Action,
    feed it back to the expend node and all upstream nodes,
    and update the corresponding data.
    """

    # Update until the root node.
    while node is not None:
        # Update the visit number.
        node.update_visit_number()

        # Update the quality value.
        node.update_quality_value(reward)

        # Change the node to the parent node.
        node = node.parent


def monte_carlo_tree_search(node):
    """
    Implement the Monte Carlo tree search algorithm, pass in a root node,
    expand new nodes and update data according to the
     tree structure that has been explored before in a limited time,
    and then return as long as the child node with the highest exploitation.

    When making predictions,
    you only need to select the node with the largest exploitation according to the Q value,
    and find the next optimal node.
    """

    computation_budget = len(AVAILABLE_CHOICES) * STORAGE_THRESHOLD/TOTAL_STORAGE * 50

    # Run as much as possible under the computation budget.
    for i in range(int(computation_budget)):
        # 1. find the best node to expand.
        expand_node = tree_policy(node)

        # 2. random get next action and get reward.
        reward = default_policy(expand_node)

        # 3. update all passing nodes with reward.
        backpropagate(expand_node, reward)

    # Get the best next node.
    best_next_node = best_child(node, False)

    return best_next_node


def MCTS(workload_info, atomic_choices, available_choices, storage_threshold, max_index_num):
    global ATOMIC_CHOICES, STORAGE_THRESHOLD, WORKLOAD, \
        AVAILABLE_CHOICES, MAX_INDEX_NUM, TOTAL_STORAGE
    WORKLOAD = workload_info
    AVAILABLE_CHOICES = available_choices
    ATOMIC_CHOICES = atomic_choices
    STORAGE_THRESHOLD = storage_threshold
    MAX_INDEX_NUM = max_index_num if max_index_num and max_index_num < len(available_choices) \
        else len(available_choices)
    for index in available_choices:
        TOTAL_STORAGE += index.get_storage()
    if STORAGE_THRESHOLD >= TOTAL_STORAGE:
        return sorted(available_choices, key=lambda x: x.benefit, reverse=True)[:MAX_INDEX_NUM]
    # Create the initialized state and initialized node.
    init_state = State()
    init_node = Node()
    init_node.set_state(init_state)
    current_node = init_node

    opt_config = []
    # Set the rounds to play.
    for i in range(len(AVAILABLE_CHOICES)):
        if current_node:
            current_node.reset_node()
            current_node.state.reset_state()
            current_node = monte_carlo_tree_search(current_node)
            if current_node:
                opt_config = current_node.state.accumulation_choices
            else:
                break
    return opt_config
