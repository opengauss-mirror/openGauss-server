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

from dbmind.common.sequence_buffer import SequenceBufferPool
from dbmind.common.sequence_buffer import SequenceTree
from dbmind.common.types import Sequence


def test_add_and_search():
    st = SequenceTree()
    st.add(Sequence((1, 2, 3), (1, 1, 1)))
    assert str(st) == '[[(1 3)[0]]]'
    st.add(Sequence((1, 2), (1, 1)))
    assert str(st) == '[[(1 3)[0]]]'
    st.add(Sequence((2, 3, 4, 5), (1, 1, 1, 1)))
    assert str(st) == '[[(1 5)[0]]]'
    st.add(Sequence((7, 8), (1, 1)))
    assert str(st) == '[[(1 8)[2]], [(1 5)[0], (7 8)[0]]]'
    st.add(Sequence((10, 11, 12), (1, 1, 1)))
    assert str(st) == '[[(1 12)[2]], [(1 8)[2], (10 12)[0]], [(1 5)[0], (7 8)[0]]]'
    st.add(Sequence((6, 7), (1, 1)))
    assert str(st) == '[[(1 12)[2]], [(1 8)[0], (10 12)[0]]]'
    st.add(Sequence((-1, 0), (1, 1)))
    assert str(st) == '[[(-1 12)[2]], [(-1 8)[0], (10 12)[0]]]'
    st.add(Sequence((18, 19, 20), (1, 1, 1)))
    st.add(Sequence((22, 23), (1, 1)))
    assert str(st) == '[[(-1 23)[2]], [(-1 20)[2], (22 23)[0]], [(-1 12)[2], (18 20)[0]], [(-1 8)[0], (10 12)[0]]]'

    ret = st.search_node(0, 1)
    assert str(ret) == '(-1 8)[0]'
    ret = st.search_node(24, 25)
    assert str(ret) == 'None'
    ret = st.search_node(23, 23)
    assert str(ret) == '(22 23)[0]'
    ret = st.search_node(8, 9)
    assert str(ret) == '(-1 12)[2]'
    ret = st.search_node(9, 11)
    assert str(ret) == '(-1 12)[2]'
    ret = st.search_node(9, 9)
    assert str(ret) == '(-1 12)[2]'
    ret = st.search_node(22, 25)
    assert str(ret) == 'None'
    ret = st.search_node(9, 11)
    assert str(ret) == '(-1 12)[2]'


def test_prune_and_gaps():
    st = SequenceTree()
    st.add(Sequence((1, 2), (1, 1)))
    st.add(Sequence((4, 5), (1, 1)))
    st.add(Sequence((7, 8), (1, 1)))
    ret = st.search_node(3, 8)
    assert str(ret) == '(1 8)[2]'
    ret = st.find_gaps(3, 8)
    assert str(ret) == '[(3, 4), (5, 7)]'
    ret = st.find_gaps(3, 3)
    assert str(ret) == '[(3, 4)]'
    ret = st.find_gaps(6, 6)
    assert str(ret) == '[(6, 7)]'
    ret = st.find_gaps(0, 9)
    assert str(ret) == '[(0, 1), (2, 4), (5, 7), (8, 9)]'
    ret = st.find_gaps(2, 7)
    assert str(ret) == '[(2, 4), (5, 7)]'
    ret = st.find_gaps(1, 11)
    assert str(ret) == '[(2, 4), (5, 7), (8, 11)]'

    assert st.get(1, 2).timestamps == (1, 2)
    assert st.get(1, 3).timestamps == (1, 2)
    assert st.get(1, 4).timestamps == (1, 2, 4)
    assert st.get(2, 4).timestamps == (2, 4)
    assert st.get(2, 3).timestamps == (2,)
    assert st.get(3, 3).timestamps == ()  # missing value doesn't raise exception.

    try:
        st.get(0, 3)
        assert False
    except LookupError:
        pass

    st.prune(0)
    assert str(st) == '[[(1 8)[2]], [(1 5)[2], (7 8)[0]], [(1 2)[0], (4 5)[0]]]'
    st.prune(1)
    assert str(st) == '[[(1 8)[2]], [(1 5)[2], (7 8)[0]], [(1 2)[0], (4 5)[0]]]'
    st.prune(2)
    assert str(st) == '[[(2 8)[2]], [(2 5)[2], (7 8)[0]], [(2 2)[0], (4 5)[0]]]'
    st.prune(3)
    assert str(st) == '[[(4 8)[2]], [(4 5)[0], (7 8)[0]]]'
    st.prune(7)
    assert str(st) == '[[(7 8)[0]]]'
    st.prune(8)
    assert str(st) == '[[(8 8)[0]]]'
    st.prune(9)
    assert str(st) == '[]'

    st = SequenceTree()
    st.add(Sequence((1, 2), (1, 1)))
    st.add(Sequence((4, 5), (1, 1)))
    st.add(Sequence((7, 8), (1, 1)))

    ret = st.search_node(2, 2)
    assert str(ret) == '(1 2)[0]'
    assert st.get(2, 2).timestamps == (2,)

    gaps = st.find_gaps(-1, 20)
    for gap in gaps:
        faked_timestamps = list(range(gap[0], gap[1] + 1))
        faked_values = [1] * len(faked_timestamps)
        st.add(Sequence(faked_timestamps, faked_values))
        assert '[3]' not in str(st)
    assert str(st) == '[[(-1 20)[0]]]'

    ret = st.get(1, 3)
    assert ret.timestamps == (1, 2, 3)


def test_sequence_tree_for_step():
    st = SequenceTree()
    st.add(Sequence((1, 3, 5), (1, 1, 1)))
    st.add(Sequence((5, 7), (1, 1)))
    try:
        st.add(Sequence((0, 2), (1, 1)))
        assert False
    except TypeError:
        pass
    st.add(Sequence((-1, 1, 3), (1, 1, 1)))
    st.add(Sequence((-1, 1), (1, 1)))
    assert st.get(-1, 7).timestamps == (-1, 1, 3, 5, 7)


latest_fetch_range = None


def generate_sequence(start, end, step=1, name='os_usage_rate', labels=None):
    global latest_fetch_range

    latest_fetch_range = (start, end, step)
    print("Generate sequence: ", start, end, step, labels)
    if not labels:
        return [Sequence(
            range(start, end + 1, step), range(start, end + 1, step), name, labels={'ip': '127.0.0.1'}
        ),
            Sequence(
                range(start, end + 1, step), range(start, end + 1, step), name, labels={'ip': 'xx.xx.xx.100'}
            )
        ]

    return [Sequence(
        range(start, end + 1, step), range(start, end + 1, step), name, labels=labels
    )]


def test_buffer_pool():
    pool = SequenceBufferPool(5, vacuum_timeout=1000)
    pool._evict_task = lambda a: a  # dummy

    # test the round function.
    for t in range(100):
        for b in range(100):
            for s in range(1, 10):
                r = pool._round(t, b, s, 'left')
                assert (r - b) % s == 0 and r >= t, (t, b, s, r)
                r = pool._round(t, b, s, 'right')
                assert (r - b) % s == 0 and r <= t, (t, b, s, r)

    sequences = generate_sequence(1, 20, 2)
    for seq in sequences:
        pool.put(seq)

    def only_test_get(start, end, step=1, name='os_usage_rate', labels=None):
        labels = labels or {}
        results = pool.get(name, start, end, step,
                           labels, lambda s_, e, step_: generate_sequence(s_, e, step_, name, labels))
        for result in results:
            if result.timestamps != tuple(range(start, end + 1, step)):
                drift = result.timestamps[0] - start
                expected = tuple(range(start + drift, end + drift + 1, step))
                assert result.timestamps == expected
            for k, v in labels.items():
                assert result.labels[k] == v

    only_test_get(15, 25, 1, labels={'ip': '127.0.0.1'})
    only_test_get(1, 10, 2, labels={'ip': '127.0.0.1'})
    only_test_get(0, 11, 2)
    only_test_get(0, 11, 4)
    only_test_get(0, 25, 4)
    only_test_get(1, 15, 5, labels={'ip': 'xx.xx.xx.100'})
    only_test_get(0, 25, 4, labels={'ip': 'xx.xx.xx.100'})
    only_test_get(0, 25, 8)

    # Test for eviction task.
    pool.evict(20)
    only_test_get(25, 35, 2)
    assert latest_fetch_range == (25, 35, 1)

    pool.evict(20)
    only_test_get(15, 25, 2)
    assert latest_fetch_range == (15, 20, 1)
    only_test_get(15, 25, 4)

    pool.evict(20)
    only_test_get(15, 25, 2)
    assert latest_fetch_range == (15, 20, 1)

    only_test_get(10, 20, 1)

    only_test_get(6, 6, 1)


def test_align_sequence():
    def compare(
            tree_start, tree_end,
            sequence_start, sequence_end, step=15000
    ):
        s = Sequence(
            range(sequence_start, sequence_end + 1, step),
            range(sequence_start, sequence_end + 1, step),
            step=step
        )
        new_s = SequenceBufferPool._align_sequences(tree_start, tree_end, s)
        assert (new_s.timestamps[0] - tree_end) % 15000 == 0
        assert (new_s.timestamps[-1] - tree_start) % 15000 == 0

    compare(1651062718708, 1651063333708, 1651062704000, 1651062719000)
    compare(1650976378702, 1651063348702, 1650976319000, 1650976379000)
    compare(1650976318702, 1651063333702, 1651063184000, 1651063214000)
    compare(1651062718708, 1651063243708, 1651063244000, 1651063304000)
    compare(1651062718708, 1651063333708, 1651062704000, 1651062719000)
    compare(1651063288702, 1651063498702, 1651063274000, 1651063289000)
