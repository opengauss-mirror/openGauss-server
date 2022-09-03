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
import logging
import math
import threading
import time
from collections import defaultdict
from typing import Callable

from .algorithm.basic import how_many_lesser_elements, binary_search, binary_search_rightmost, binary_search_leftmost
from .types.sequence import Sequence
from .utils import dbmind_assert


def _merge_intervals(sorted_, start: Callable, end: Callable, merge_func: Callable):
    """In-place merging for sorted list.

    The following link describes the whole problem:
    https://leetcode.com/problems/merge-intervals
    """
    cursor = 1
    while cursor < len(sorted_):
        if end(sorted_[cursor - 1]) < start(sorted_[cursor]):
            cursor += 1
        else:
            if end(sorted_[cursor - 1]) > end(sorted_[cursor]):
                sorted_.pop(cursor)
            else:
                # merging action
                sorted_[cursor] = merge_func(sorted_[cursor - 1], sorted_[cursor])
                sorted_.pop(cursor - 1)


def _get_overlap(two_dim_list):
    sorted_ = sorted(two_dim_list, key=lambda t: t[0])
    smaller, bigger = sorted_
    if smaller[1] <= bigger[0]:
        return ()
    return bigger[0], min(smaller[1], bigger[1])


class TreeNode:
    def __init__(self, sequence=None):
        self.sequence = sequence
        self.start = sequence.timestamps[0] if self.sequence else 0
        self.end = sequence.timestamps[-1] if self.sequence else 0
        self.parent = None
        self._children = []

    def set(self, sequence):
        self.sequence = sequence
        self.start = sequence.timestamps[0]
        self.end = sequence.timestamps[-1]

    def add_child(self, child_node):
        # Orderly add.
        if len(self._children) > 0:
            dbmind_assert(child_node.start > self._children[-1].end)

        self._children.append(child_node)
        child_node.parent = self
        # Create a new node because we only want a tree node only has two child nodes.
        if len(self._children) > 2:
            new_node = TreeNode()
            first = self._children.pop(0)
            second = self._children.pop(0)
            new_node.start = first.start
            new_node.end = second.end
            new_node.children = [first, second]
            self.children = [new_node, child_node]

    def clear_children(self):
        self._children.clear()

    def pop_first_node(self):
        # Take back the child's sequence and revise this node's start value.
        self.start = self.children[1].start
        self.end = self.children[1].end  # not necessary
        self.sequence = self.children[1].sequence
        self.clear_children()

        # Revise the start value of the parent.
        curr = self
        while curr.parent:
            curr = curr.parent
            curr.start = max(curr.children[0].start, curr.start)

    @property
    def children(self):
        return tuple(self._children)

    @children.setter
    def children(self, value):
        dbmind_assert(isinstance(value, list))
        self._children = value
        for child in self._children:
            child.parent = self

    def copy(self):
        node = TreeNode()
        node.sequence = self.sequence
        node.start = self.start
        node.end = self.end
        node.children = self._children.copy()
        return node

    def __repr__(self):
        return "(%d %d)[%d]" % (self.start, self.end, len(self._children))


class SequenceTree:
    """Similar to a segment tree,
    the tree's nodes need to mark the start
    and end timestamps since the cached sequence may be a fragment.
    If users want to search a sequence that this tree only has some parts,
    then SequenceTree only needs to fetch the missing pieces and merge
    these fragmented sequences into a bigger one.
    Using this mechanism can cost less on network I/O and memory.
    """

    def __init__(self):
        self._root = None
        self._step = None
        self._name = None
        self._labels = None

    @property
    def start(self):
        return self._root.start

    @property
    def end(self):
        return self._root.end

    @property
    def step(self):
        """Cannot merge two sequences with different steps.
        So when the root node is given, the step of the whole tree has been confirmed."""
        return self._step

    @property
    def name(self):
        return self._name

    @property
    def labels(self):
        return self._labels

    @property
    def empty(self):
        return self._root is None

    @staticmethod
    def _extract_sequences(tree_node):
        sequences = []

        # Extract all sequences.
        def traverse(node):
            if node is None:
                return
            if node.sequence:
                sequences.append(node.sequence)
                return

            for child in node.children:
                traverse(child)

        traverse(tree_node)
        return sequences

    @staticmethod
    def _merge(nodes):
        """Wrap pure intervals merging function. The distinction between the pure intervals merging function and this
        method is that this method can handle the TreeNode type. However, the TreeNode type isn't a linear/continuous
        structure, and the entity sequence may store in the child node. Thus, you can see we use recursion function
        to traverse all of the sequences.
        """

        def start(s):
            return s.timestamps[0]

        def end(s):
            return s.timestamps[-1]

        def step(s):
            return s.step

        sequences = []
        for node_ in nodes:
            sequences.extend(SequenceTree._extract_sequences(node_))

        sorted_ = sorted(sequences, key=lambda s: s.timestamps[0])

        _merge_intervals(sorted_, start, end, merge_func=lambda a, b: a + b)

        # Concatenate two sequences that the distance between them is the length of the step.
        cursor = 1
        while cursor < len(sorted_):
            # Because the sequence is discrete, here we need to merge two sequences where
            # s0_end + s0_step == s1_start,
            # e.g.,
            # (1, 2, 3) + (5, 6)    -> ((1, 2, 3), (5, 6))
            # but, (1, 2, 3, 4) + (5, 6) -> (1, 2, 3, 4, 5, 6)
            if end(sorted_[cursor - 1]) + step(sorted_[cursor - 1]) == start(sorted_[cursor]):
                concat = sorted_[cursor - 1] + sorted_[cursor]
                sorted_.pop(cursor - 1)
                sorted_[cursor - 1] = concat
                continue
            cursor += 1
        return sorted_

    def intersection_merge(self, node, sequence):
        """Merge the given sequence into a suitable node."""
        if node is None:
            return

        # Always keep the TreeNode has two child nodes.
        # This implementation constructs a binary search tree,
        # which can reduce search overhead.
        # Right node.
        if sequence.timestamps[0] - sequence.step > node.end:
            old_node = node.copy()
            node.sequence = None
            node.start = node.start
            node.end = sequence.timestamps[-1]
            node.children = [old_node, TreeNode(sequence)]
            return

        # Left Node.
        if node.start > sequence.timestamps[-1] + sequence.step:
            old_node = node.copy()
            node.sequence = None
            node.start = sequence.timestamps[0]
            node.end = node.end
            node.children = [TreeNode(sequence), old_node]
            return

        # Traverse and merge children.
        for child in node.children:
            self.intersection_merge(child, sequence)

        if len(node.children) > 0:
            children = [TreeNode(sequence)]
            children.extend(node.children)
        else:
            children = [node, TreeNode(sequence)]

        merged = self._merge(children)
        # If only has one child, this only one child is the current node's value, don't
        # need to maintain the list of tree nodes.
        if len(merged) == 1:
            node.set(merged[0])
            node.clear_children()
        # Otherwise, need to reorganize children.
        else:
            node.sequence = None
            node.start = merged[0].timestamps[0]
            node.end = merged[-1].timestamps[-1]
            node.clear_children()
            for s in merged:
                node.add_child(TreeNode(s))

        dbmind_assert(len(node.children) == 0 or len(node.children) == 2)

    def add(self, sequence: Sequence):
        """Add a new sequence to the tree.
        In this method, we will create a new tree node to wrap the sequence and
         put the node to a suitable position of the tree."""
        if len(sequence) == 0:
            return

        dbmind_assert(sequence.step > 0)

        # Construct the first node.
        if not self._root:
            self._root = TreeNode(sequence)
            self._step = sequence.step
            self._name = sequence.name
            self._labels = sequence.labels
            return

        self.intersection_merge(self._root, sequence)

    def search_node(self, start, end):
        """Locate the node that contains the given range."""

        def traverse(node):
            if node is None:
                return

            # If it can be found in the range, ...
            if node.start <= start <= end <= node.end:
                if node.sequence:
                    return node
                # If this node still has children, continue to traverse.
                dbmind_assert(len(node.children) == 2)
                s0_end, s1_start = node.children[0].end, node.children[1].start
                # binary search
                if s0_end >= end:
                    return traverse(node.children[0])
                elif s1_start <= start:
                    return traverse(node.children[1])
                else:
                    # Lack sequence, return current node.
                    return node

        return traverse(self._root)

    def find_gaps(self, start, end):
        """Find the gaps in the current tree.
        Return the gaps to caller so that he can
        fetch the missing sequences from the
        time-series database and add them to the tree."""
        if (
                self._root is None
                or end < self._root.start
                or start > self._root.end
        ):
            return (start, end),

        node = self.search_node(start, end)
        # Cannot match any suitable nodes, which means no one sequence can
        # hold given range.
        if not node:
            gaps = []
            if start < self._root.start:
                gaps.append((start, self._root.start))
            if end > self._root.end:
                gaps.append((self._root.end, end))
            # Search for gaps of self._root.
            # This is because self._root may have gaps, not including the whole sequences.
            gaps.extend(self.find_gaps(self._root.start, self._root.end))
            # Sometimes, a point can be merged into an adjacent tuple.
            gaps.sort(key=lambda t: t[0])
            _merge_intervals(
                gaps,
                start=lambda t: t[0],
                end=lambda t: t[1],
                merge_func=lambda a, b: (a[1], b[1])
            )
            # Get rid of needless ranges.
            cursor = 0
            while cursor < len(gaps):
                curr_start, curr_end = gaps[cursor]
                if curr_end <= start or curr_start >= end:
                    gaps.pop(cursor)
                    continue

                cursor += 1
            return gaps

        # Current tree already contains this range, thus returning none of gaps.
        if node.sequence:
            return []

        gaps = []

        def traverse(node_, start_, end_):
            if node_ is None or start_ > end_:
                return

            # If current node has no children, only check in the node and return, not need to traverse.
            if node_.sequence:
                if end_ < node_.start:
                    gaps.append((end_, node_.start))
                if start_ < node_.start <= end_:
                    gaps.append((start_, node_.start))
                if start_ <= node_.end < end_:
                    gaps.append((node_.end, end_))
                if end_ > node_.end:
                    gaps.append((node_.end, start_))
                return

            s0_end, s1_start = node_.children[0].end, node_.children[1].start
            gaps.append((max(s0_end, start), min(end, s1_start)))

            traverse(node_.children[0], start_, s0_end)
            traverse(node_.children[1], end_, s1_start)

        traverse(node, start, end)
        gaps.sort(key=lambda t: t[0])

        # Sometimes, a point can be merged into an adjacent tuple.
        _merge_intervals(
            gaps,
            start=lambda t: t[0],
            end=lambda t: t[1],
            merge_func=lambda a, b: (a[1], b[1])
        )
        return gaps

    def _concat_whole_sequence(self, node):
        if not node:
            raise LookupError('Not found the given range.')

        sequences = sorted(self._extract_sequences(node), key=lambda s: s.timestamps[0])
        concat_timestamps = []
        concat_values = []
        for sequence in sequences:
            concat_timestamps.extend(sequence.timestamps)
            concat_values.extend(sequence.values)

        return concat_timestamps, concat_values

    def get(self, start, end):
        """Return a stored sequence according to the given parameters."""
        dbmind_assert(start <= end)
        node = self.search_node(start, end)
        if not node:
            logging.debug(
                'SequenceTree cannot search the appointed range %d - %d. The returned node is %s. ' % (
                    start, end, node)
            )
            raise LookupError('Not found the given range.')

        # Maybe the data itself is fragmented in the time-series database.
        if not node.sequence:
            concat_timestamps, concat_values = self._concat_whole_sequence(node)
            start_index = binary_search_leftmost(concat_timestamps, start)
            end_index = binary_search_rightmost(concat_timestamps, end) + 1
            return Sequence(
                timestamps=concat_timestamps[start_index: end_index], values=concat_values[start_index: end_index],
                name=self.name, step=self.step, labels=self.labels
            )

        return node.sequence[start, end]

    def prune(self, cut_off):
        """An old sequence will be deleted when it is timeout.
        Hence, this method prunes or cuts short a tree node containing an old sequence.
        """
        if self._root is None:
            return
        # Firstly, find the least splitting point.
        node = self.search_node(cut_off, cut_off)
        # That this function can not locate the node only
        # since this tree does not contain the cut-off point.
        if not node:
            # There are two cases in this scenario:
            # The one is that all sequences are greater than the cut-off point, so do nothing.
            # The other is that all sequences are lesser than the cut-off point, evicting all sequences.
            if cut_off > self._root.end:
                # Evict the whole tree.
                self._root = None
            # No matter which scenarios, it should return directly.
            return

        # If current node already has the sequence entity, create a new truncated sequence to
        # replace with the old one.
        if node.sequence:
            cut_off_idx = how_many_lesser_elements(node.sequence.timestamps, cut_off)
            new_sequence = Sequence(
                timestamps=node.sequence.timestamps[cut_off_idx:],
                values=node.sequence.values[cut_off_idx:],
                name=node.sequence.name,
                step=node.sequence.step,
                labels=node.sequence.labels
            )
            node.set(new_sequence)
            # Backtrace:
            # Set parent node's start and evict lesser children.
            curr = node
            while curr.parent:
                curr.parent.start = cut_off
                # If current node only has only child, drop the child node and
                # take back the child's sequence.
                if curr.parent.children[0].end < cut_off:
                    curr.parent.sequence = curr.sequence
                    curr.parent.clear_children()
                curr = curr.parent

            return

        # Pruning for child nodes.
        # If cannot locate the node that has entity sequence, then the cut-off point
        # certainly falls into the gap of current node.
        # In this scenario, we only need to evict the first child
        # (this is a binary tree, the first child is always the lesser one).
        node.pop_first_node()

    def __repr__(self):
        ret = []

        def traverse(node, level):
            if node is None:
                return

            if level >= len(ret):
                ret.append([])

            ret[level].append(node)
            for child in node.children:
                traverse(child, level + 1)

        traverse(self._root, 0)
        return str(ret)


def frozendict(d: dict):
    keys = sorted(d.keys())
    return tuple((k, d[k]) for k in keys)


def restore2dict(t):
    rv = {}
    for k, v in t:
        rv[k] = v
    return rv


def dict_belongs_to(parent: dict, child: dict):
    for k, v in child.items():
        if k not in parent or parent[k] != v:
            return False
    return True


class SequenceBufferPool:
    def __init__(self, ttl=float('inf'), vacuum_timeout=10, buffer=None):
        """\
        This data structure contains two-level keys:
        the first is metric name, the second is immutable labels.
        And the final value is a list of SequenceTree,
        which the list is sorted by step field.
        e.g.,
                                      os_cpu_usage
                              /                               \
         {'from_instance': 'host1'}                        {'from_instance': 'host2'}
                   /                                                           \
         [SequenceTree([1,2,3,...], step=1), SequenceTree([1,3,...], step=2)]   [SequenceTree([1, 3, 5], step=2)

        :param ttl:
        """
        self.ttl = ttl
        self.timeout = vacuum_timeout
        self._buffer = defaultdict(dict) if buffer is None else buffer
        self._lock = threading.RLock()
        self._evict_thread = threading.Thread(
            target=self._evict_task,
            name='SequenceBufferPoolEvictionThread',
            daemon=True
        )
        self._evict_thread.start()

    @staticmethod
    def time():
        """Can be changed to logical time."""
        return int(time.time() * 1000)

    def evict(self, cutoff):
        with self._lock:
            for metric_name, metric_dict in self._buffer.items():
                for labels, trees in metric_dict.items():
                    # Firstly, prune trees.
                    cursor = 0
                    while cursor < len(trees):
                        trees[cursor].prune(cutoff)
                        if trees[cursor].empty:
                            trees.pop(cursor)
                            continue
                        cursor += 1
                    # Secondly, we want to evict redundant trees.
                    cursor = 1
                    while cursor < len(trees):
                        prev = trees[cursor - 1]
                        curr = trees[cursor]
                        # Evict the tree that has a lesser range and greater step.
                        if prev.start <= curr.start <= curr.end <= prev.end:
                            trees.pop(cursor)
                            continue
                        cursor += 1

    def _evict_task(self):
        while True:
            time.sleep(self.timeout)
            self.evict(self.time() - self.ttl)

    def _get_matched_collection(self, metric_name, labels):
        with self._lock:
            lists = []
            dictionaries = [restore2dict(t) for t in self._buffer[metric_name].keys()]
            for d in dictionaries:
                if dict_belongs_to(d, labels):
                    list_ = self._buffer[metric_name][frozendict(d)]
                    dbmind_assert(isinstance(list_, list))
                    if len(list_) > 0:
                        lists.append(list_)
            return lists

    def _get_or_create_sequence_trees(self, metric_name, labels, step):
        with self._lock:
            # If not found existing sequence, put it into buffer pool.
            # Otherwise, merge new sequence and existing sequence.
            matched_collection = self._get_matched_collection(metric_name, labels)
            frozen_labels = frozendict(labels)

            if len(matched_collection) == 0:
                tree = SequenceTree()
                self._buffer[metric_name][frozen_labels] = [tree]
                return [tree]

            rv = []
            for trees in matched_collection:
                dbmind_assert(isinstance(trees, list))
                # If we find the tree with the given step, we can return it directly.
                # Otherwise, we should create a new tree with the given step and return it.
                # Note that: the following list of trees sorted with its step so that we can employ
                # binary search to improve performance.
                steps = [tree.step for tree in trees]
                index = how_many_lesser_elements(steps, step)
                if 0 <= index < len(trees) and steps[index] == step:
                    tree = trees[index]
                else:
                    tree = SequenceTree()
                    trees.insert(index, tree)
                rv.append(tree)
            return rv

    @staticmethod
    def _align_sequences(tree_start, tree_end, sequence: Sequence):
        """Aligns the timestamps of a sequence with the reference timestamp in steps.

        :param tree_start: start position of the tree.
        :param tree_end: end position of the tree.
        :param sequence: need to be processed sequence.
        :return: aligned sequence.
        """
        logging.debug(
            '[SequenceBuffer] align sequences: (%d, %d) and (%d, %d) with step %d.'
            % (tree_start, tree_end, sequence.timestamps[0], sequence.timestamps[-1], sequence.step)
        )
        sequence_start = sequence.timestamps[0]
        distance = tree_end - sequence_start
        if distance % sequence.step == 0:
            return sequence

        if tree_end < sequence_start:
            delta = (sequence_start - tree_end) % sequence.step
            new_timestamps = map(lambda v: v - delta, sequence.timestamps)
        else:
            delta = (tree_end - sequence_start) % sequence.step
            new_timestamps = map(lambda v: v + delta, sequence.timestamps)

        return Sequence(
            new_timestamps, sequence.values, sequence.name, sequence.step, sequence.labels
        )

    def put(self, sequence: Sequence):
        with self._lock:
            dbmind_assert(sequence.name)
            dbmind_assert(sequence.labels)
            dbmind_assert(sequence.step > 0)

            metric_name = sequence.name
            labels = sequence.labels
            step = sequence.step
            trees = self._get_or_create_sequence_trees(metric_name, labels, step)

            dbmind_assert(len(trees) <= 1)
            for tree in trees:
                if not tree.empty:
                    sequence = self._align_sequences(tree.start, tree.end, sequence)
                    dbmind_assert((tree.end - sequence.timestamps[0]) % sequence.step == 0)
                tree.add(sequence)

    @staticmethod
    def _round(timestamp, tree_boundary, tree_step, shrinking_from):
        if tree_step == 0:
            return timestamp
        if shrinking_from == 'left':
            diff = (tree_boundary - timestamp) % tree_step
            return timestamp + diff
        elif shrinking_from == 'right':
            diff = (timestamp - tree_boundary) % tree_step
            return timestamp - diff
        else:
            raise ValueError(shrinking_from)

    @staticmethod
    def _sample(sequence: Sequence, step):
        def _range(start, end, step_):
            curr = start
            while curr < end:
                yield (math.floor(curr), math.ceil(curr))
                curr += step_

        def _mean(a, b):
            return (a + b) / 2

        # That the following assertion fails generally
        # caused by new labels inserted into the time-series database.
        # This is a system error, raising the exception is ok.
        if len(sequence) <= 1:
            return sequence

        dbmind_assert(step >= sequence.step)
        timestamps = []
        values = []
        for left, right in _range(0, len(sequence), step // sequence.step):
            # linear interpolation
            timestamps.append(
                int(_mean(sequence.timestamps[left], sequence.timestamps[right]))
            )
            values.append(
                _mean(sequence.values[left], sequence.values[right])
            )

        return Sequence(
            timestamps=timestamps,
            values=values,
            name=sequence.name,
            step=step,
            labels=sequence.labels
        )

    @staticmethod
    def locate_tree(trees, step):
        steps = [tree.step for tree in trees]
        index = binary_search(steps, step)
        if index >= 0:
            tree = trees[index]
        else:
            tree = trees[0]
        return tree

    def get(self, metric_name, start_time, end_time, step, labels, fetcher_func: Callable):
        with self._lock:
            matched_collection = self._get_matched_collection(metric_name, labels)
            if len(matched_collection) == 0:
                sequences = fetcher_func(start_time, end_time, step)
                for sequence in sequences:
                    sequence.step = sequence.step or step
                    # If the length of the sequence is 0, we don't need to put it into the buffer pool.
                    # Otherwise, there will be a sequence with zero step in the tree list.
                    if sequence.step:
                        self.put(sequence)
                return sequences

            rv = []
            for trees in matched_collection:
                dbmind_assert(isinstance(trees, list))

                if step is None:
                    # If the given step is None, the method will run this branch.
                    # If the step is not given, return the sequence as the smallest step as possible.
                    tree = trees[0]
                    step = tree.step
                else:
                    tree = self.locate_tree(trees, step)

                # If the step of found tree is lesser than given one, sampling from this sequence.
                # Otherwise, fetch a new one with given step.
                cached_sequences = []  # To prevent losing the sequences that put into buffer.
                if step < tree.step or tree.step == 0:
                    cached_sequences = sequences = fetcher_func(start_time, end_time, step)
                    for sequence in sequences:
                        sequence.step = sequence.step or step
                        self.put(sequence)
                    tree = self.locate_tree(trees, step)
                dbmind_assert(tree.step)

                # If found buffered sequences, find gaps and fill up the gaps.
                old_start_time, old_end_time = start_time, end_time
                start_time = self._round(start_time, tree.start, tree.step, 'left')
                end_time = self._round(end_time, tree.end, tree.step, 'right')
                dbmind_assert(
                    old_start_time > tree.start or old_start_time <= start_time,
                    'old: %d, new: %d, tree: %d' % (old_start_time, start_time, tree.start)
                )
                dbmind_assert(
                    old_end_time < tree.end or old_end_time >= end_time,
                    'old: %d, new: %d, tree: %d' % (old_end_time, end_time, tree.end)
                )
                if start_time > end_time:
                    start_time = end_time

                for s, e in tree.find_gaps(start_time, end_time):
                    cached_sequences = sequences = fetcher_func(s, e, tree.step)
                    for sequence in sequences:
                        sequence.step = sequence.step or tree.step
                        self.put(sequence)

                tree = self.locate_tree(trees, step)
                try:
                    # Give the part of the intersection as much as possible.
                    overlap = _get_overlap(((start_time, end_time), (tree.start, tree.end)))
                    if len(overlap) == 0:
                        if len(cached_sequences) == 0:
                            raise LookupError()

                        # There is an unsolved problem:
                        # we cannot get the same sequence from the
                        # tree after putting it into the buffer.
                        # It seems that the buffer pool hasn't cached
                        # the sequence caused an unknown reason.
                        # The most unexpected scenarios are discontinuous sequences,
                        # such as pg_thread_pool_listener.
                        # But, we can record the fetched sequence to
                        # bypass and reuse it in the above exception scenario.
                        rv.extend(cached_sequences)
                        logging.warning('Use bypass for fetching sequence %s.', metric_name)
                        continue

                    # If involved, extract a sequence of the given step size from the denser sequence.
                    raw_sequence = tree.get(*overlap)
                    sequence = self._sample(raw_sequence, step)
                    rv.append(sequence)
                except LookupError as e:
                    logging.warning(
                        'Cannot fetch the sequence %s from %s to %s. The tree is (%d, %d, %d).' % (
                            metric_name, start_time, end_time,
                            tree.start, tree.end, tree.step
                        ), exc_info=e
                    )
            return rv
