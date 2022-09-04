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
"""The functions in this file are only for debugging."""
from dbmind.common.sequence_buffer import SequenceTree, TreeNode


def visualize_sequence_tree(tree: SequenceTree):
    lines = []

    def dfs(node: TreeNode, level):
        if node is None:
            return

        # Shrink range for representation.
        if node.sequence:
            start = (node.sequence.timestamps[0] - tree.start) // 1000
            end = (node.sequence.timestamps[-1] - tree.start) // 1000
            step = node.sequence.step // 1000
            repr_ = 'SEQUENCE: %s-%s %s %s' % (start, end, len(node.sequence), step)
        else:
            repr_ = 'TREE: %s-%s %s' % (
                (node.start - tree.start) // 1000, (node.end - tree.start) // 1000, tree.step // 1000
            )
        lines.append('\t' * level + repr_)
        for child in node.children:
            dfs(child, level + 1)

    print('TREE: %s-%s %s' % (0, (tree.end - tree.start) // 1000, tree.step // 1000))
    dfs(getattr(tree, '_root'), level=1)
    print('\n'.join(lines))


def visualize_buffer_pool(buffer, metric_name):
    for k, v in buffer[metric_name].items():
        print(k)
        for tree in v:
            visualize_sequence_tree(tree)
        print('\n')


def normalize(value_list, dst_range=100):
    max_value = max(value_list)
    min_value = min(value_list)
    value_range = max_value - min_value + 0.001
    return [int(((value - min_value) / value_range) * dst_range)
            for value in value_list]


def shift(value_list):
    min_value = value_list[0]
    return [(value - min_value)
            for value in value_list]


def locate_relative_position_in_sequence_tree(tree: SequenceTree, start, end):
    dst_range = 50
    timestamps = (tree.start, tree.end, start, end)
    tree_start, tree_end, sequence_start, sequence_end = normalize(
        timestamps, dst_range
    )
    print('Ts\tTe\tSs\tSe\n'
          '%s\t%s\t%s\t%s\n'
          '%s\t%s\t%s\t%s\n'
          % (*timestamps, tree_start, tree_end, sequence_start, sequence_end))

    elements = [(tree_start, 'Ts'), (tree_end, 'Te'),
                (sequence_start, 'Ss'), (sequence_end, 'Se')]
    elements.sort(key=lambda t: t[0])

    # drawing
    point_shape = '..'
    axis = [point_shape for _ in range(dst_range)]
    for e in elements:
        position, title = e
        if axis[position] != point_shape:
            axis[position] = axis[position][0] + title[0]
        else:
            axis[position] = title
    print(''.join(axis))
