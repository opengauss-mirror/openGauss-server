import sys
import math
import random
import copy

STORAGE_THRESHOLD = 0
AVAILABLE_CHOICES = None
ATOMIC_CHOICES = None
WORKLOAD_INFO = None
MAX_INDEX_NUM = 0


def is_same_index(index, compared_index):
    return index.table == compared_index.table and \
           index.columns == compared_index.columns and \
           index.index_type == compared_index.index_type


def atomic_config_is_valid(atomic_config, config):
    # if candidate indexes contains all atomic index of current config1, then record it
    for atomic_index in atomic_config:
        is_exist = False
        for index in config:
            if is_same_index(index, atomic_index):
                index.storage = atomic_index.storage
                is_exist = True
                break
        if not is_exist:
            return False
    return True


def find_subsets_num(choice):
    atomic_subsets_num = []
    for pos, atomic in enumerate(ATOMIC_CHOICES):
        if not atomic or len(atomic) > len(choice):
            continue
        # find valid atomic index
        if atomic_config_is_valid(atomic, choice):
            atomic_subsets_num.append(pos)
        # find the same atomic index as the candidate index
        if len(atomic) == 1 and (is_same_index(choice[-1], atomic[0])):
            choice[-1].atomic_pos = pos
    return atomic_subsets_num


def find_best_benefit(choice):
    atomic_subsets_num = find_subsets_num(choice)
    total_benefit = 0
    for ind, obj in enumerate(WORKLOAD_INFO):
        # calculate the best benefit for the current sql
        max_benefit = 0
        for pos in atomic_subsets_num:
            if (obj.cost_list[0] - obj.cost_list[pos]) > max_benefit:
                max_benefit = obj.cost_list[0] - obj.cost_list[pos]
        total_benefit += max_benefit
    return total_benefit


def get_diff(available_choices, choices):
    except_choices = copy.copy(available_choices)
    for i in available_choices:
        for j in choices:
            if is_same_index(i, j):
                except_choices.remove(i)
    return except_choices


class State(object):
    """
    The game state of the Monte Carlo tree search,
    the state data recorded under a certain Node node,
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
        # the current node is a leaf node
        return len(self.accumulation_choices) == MAX_INDEX_NUM

    def compute_benefit(self):
        return self.current_benefit

    def get_next_state_with_random_choice(self):
        # ensure that the choices taken are not repeated
        if not self.available_choices:
            return None
        random_choice = random.choice([choice for choice in self.available_choices])
        self.available_choices.remove(random_choice)
        choice = copy.copy(self.accumulation_choices)
        choice.append(random_choice)
        benefit = find_best_benefit(choice)
        # if current choice not satisfy restrictions, then continue get next choice
        if benefit <= self.current_benefit or \
                self.current_storage + random_choice.storage > STORAGE_THRESHOLD:
            return self.get_next_state_with_random_choice()

        next_state = State()
        # initialize the properties of the new state
        next_state.set_accumulation_choices(choice)
        next_state.set_current_benefit(benefit)
        next_state.set_current_storage(self.current_storage + random_choice.storage)
        next_state.set_available_choices(get_diff(AVAILABLE_CHOICES, choice))
        return next_state

    def __repr__(self):
        self.displayable_choices = ['{}: {}'.format(choice.table, choice.columns)
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
        return len(self.children) == \
               len(AVAILABLE_CHOICES) - len(self.get_state().get_accumulation_choices())

    def __repr__(self):
        return "Node: {}, Q/N: {}/{}, State: {}".format(
            hash(self), self.quality, self.visit_number, self.state)


def tree_policy(node):
    """
    In the Selection and Expansion stages of Monte Carlo tree search,
    the node that needs to be searched (such as the root node) is passed in,
    and the best node that needs to be expanded is returned
    according to the exploration/exploitation algorithm.
    Note that if the node is a leaf node, it will be returned directly.

   The basic strategy is to first find the child nodes that have not been selected at present,
   and select them randomly if there are more than one. If both are selected,
   find the one with the largest UCB value that has weighed exploration/exploitation,
   and randomly select if the UCB values are equal.
    """

    # check if the current node is leaf node
    while node and not node.get_state().is_terminal():

        if node.is_all_expand():
            node = best_child(node, True)
        else:
            # return the new sub node
            sub_node = expand(node)
            # when there is no node that satisfies the condition in the remaining nodes,
            # this state is empty
            if sub_node.get_state():
                return sub_node

    # return the leaf node
    return node


def default_policy(node):
    """
    In the Simulation stage of Monte Carlo tree search, input a node that needs to be expanded,
    create a new node after random operation, and return the reward of the new node.
    Note that the input node should not be a child node,
    and there are unexecuted Actions that can be expendable.

    The basic strategy is to choose the Action at random.
    """

    # get the state of the game
    current_state = copy.deepcopy(node.get_state())

    # run until the game over
    while not current_state.is_terminal():
        # pick one random action to play and get next state
        next_state = current_state.get_next_state_with_random_choice()
        if not next_state:
            break
        current_state = next_state

    final_state_reward = current_state.compute_benefit()
    return final_state_reward


def expand(node):
    """
    Enter a node, expand a new node on the node, use the random method to execute the Action,
    and return the new node. Note that it is necessary to ensure that the newly
     added nodes are different from other node Action
    """

    new_state = node.get_state().get_next_state_with_random_choice()
    sub_node = Node()
    sub_node.set_state(new_state)
    node.expand_child(sub_node)

    return sub_node


def best_child(node, is_exploration):
    """
    Using the UCB algorithm,
    select the child node with the highest score after weighing the exploration and exploitation.
    Note that if it is the prediction stage,
    the current Q-value score with the highest score is directly selected.
    """

    best_score = -sys.maxsize
    best_sub_node = None

    # travel all sub nodes to find the best one
    for sub_node in node.get_children():
        # The children nodes of the node contains the children node whose state is empty,
        # this kind of node comes from the node that does not meet the conditions.
        if not sub_node.get_state():
            continue
        # ignore exploration for inference
        if is_exploration:
            C = 1 / math.sqrt(2.0)
        else:
            C = 0.0

        # UCB = quality / times + C * sqrt(2 * ln(total_times) / times)
        left = sub_node.get_quality_value() / sub_node.get_visit_number()
        right = 2.0 * math.log(node.get_visit_number()) / sub_node.get_visit_number()
        score = left + C * math.sqrt(right)
        # get the maximum score, while filtering nodes that do not meet the space constraints and
        # nodes that have no revenue
        if score > best_score \
                and sub_node.get_state().get_current_storage() <= STORAGE_THRESHOLD \
                and sub_node.get_state().get_current_benefit() > 0:
            best_sub_node = sub_node
            best_score = score

    return best_sub_node


def backpropagate(node, reward):
    """
    In the Backpropagation stage of Monte Carlo tree search,
    input the node that needs to be expended and the reward of the newly executed Action,
    feed it back to the expend node and all upstream nodes,
    and update the corresponding data.
    """

    # update util the root node
    while node is not None:
        # update the visit number
        node.update_visit_number()

        # update the quality value
        node.update_quality_value(reward)

        # change the node to the parent node
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

    computation_budget = len(AVAILABLE_CHOICES) * 3

    # run as much as possible under the computation budget
    for i in range(computation_budget):
        # 1. find the best node to expand
        expand_node = tree_policy(node)
        if not expand_node:
            # when it is None, it means that all nodes are added but no nodes meet the space limit
            break
        # 2. random get next action and get reward
        reward = default_policy(expand_node)

        # 3. update all passing nodes with reward
        backpropagate(expand_node, reward)

    # get the best next node
    best_next_node = best_child(node, False)

    return best_next_node


def MCTS(workload_info, atomic_choices, available_choices, storage_threshold, max_index_num):
    global ATOMIC_CHOICES, STORAGE_THRESHOLD, WORKLOAD_INFO, AVAILABLE_CHOICES, MAX_INDEX_NUM
    WORKLOAD_INFO = workload_info
    AVAILABLE_CHOICES = available_choices
    ATOMIC_CHOICES = atomic_choices
    STORAGE_THRESHOLD = storage_threshold
    MAX_INDEX_NUM = max_index_num if max_index_num else len(available_choices)

    # create the initialized state and initialized node
    init_state = State()
    choices = copy.copy(available_choices)
    init_state.set_available_choices(choices)
    init_node = Node()
    init_node.set_state(init_state)
    current_node = init_node

    opt_config = []
    # set the rounds to play
    for i in range(len(AVAILABLE_CHOICES)):
        if current_node:
            current_node = monte_carlo_tree_search(current_node)
            if current_node:
                opt_config = current_node.state.accumulation_choices
            else:
                break
    return opt_config
