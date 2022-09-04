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
from dbmind.common.types import Sequence


def test_sequence():
    no_one_seq = Sequence(range(0, 0), range(0, 0))
    assert str(no_one_seq) == 'Sequence[None](0){}'
    assert len(no_one_seq) == 0

    s1_tms = (10, 20, 30, 40, 50)
    s1_vls = (1, 2, 3, 4, 5)
    s1 = Sequence(s1_tms, s1_vls)
    assert s1.timestamps == (10, 20, 30, 40, 50)
    assert s1.values == (1, 2, 3, 4, 5)
    assert len(s1) == 5
    assert s1[30] == 3
    assert s1[1] is None

    sub1 = s1[20, 40]  # (20, 30, 40), (2, 3, 4)
    assert sub1.timestamps == (20, 30, 40)
    assert sub1.values == (2, 3, 4)

    assert len(sub1) == 3

    sub_non = sub1[100, 111]
    assert len(sub_non) == 0
    assert sub_non.timestamps == tuple()
    assert sub_non.values == tuple()
    sub2 = s1[40, 80]
    assert len(sub2) == 2
    sub3 = sub2[40, 40]
    assert len(sub3) == 1
    sub4 = sub2[80, 80]
    assert len(sub4) == 0
    assert sub2.values == (4, 5)

    assert sub2[40] == 4, sub2[50] == 5

    e = None
    try:
        Sequence((1, 2, 3, 4, 4, 5), (10, 20, 30, 40, 30, 20))
    except ValueError as _:
        e = _
    assert isinstance(e, ValueError)

    # test iterator for sequence
    for i, (t, v) in enumerate(s1):
        assert t == s1_tms[i] and v == s1_vls[i]

    assert Sequence([1, 2, 3], [1, 2, 3]) != Sequence([1, 2, 3], [1, 1, 3])
    assert Sequence([1, 2, 3], [1, 2, 3])[2, 3] == Sequence([2, 3], [2, 3])

    s1 = Sequence((1, 2, 3, 4), (1, 2, 3, 4))
    s2 = Sequence((4, 5, 6), (4, 2, 3))
    s3 = Sequence((5, 6, 7), (2, 3, 4))
    s4 = Sequence((5, 7), (3, 4))
    s5 = Sequence((6, 8), (3, 4))
    s6 = Sequence((1, 3, 5, 7), (1, 1, 1, 1))
    s7 = Sequence((9, 11, 13), (1, 1, 1))
    assert Sequence((1, 2, 3, 4, 5, 6), (1, 2, 3, 4, 2, 3)) == s1 + s2
    assert Sequence((1, 2, 3, 4, 5, 6, 7), (1, 2, 3, 4, 2, 3, 4)) == s1 + s3
    assert Sequence((4, 5, 6, 7), (4, 2, 3, 4)) == s2 + s3
    assert Sequence((1, 3, 5, 7), (1, 1, 3, 4)) == s4 + s6  # Wrong case
    assert Sequence((5, 7, 9, 11, 13), (3, 4, 1, 1, 1)) == s4 + s7

    s8 = Sequence((1, 3, 5, 7, 8, 9), (1, 1, 1, 1, 1, 1), align_timestamp=True)
    assert s8.timestamps == (1, 3, 5, 7, 9, 11)

    # Cannot insert.
    try:
        s5 + s4
    except TypeError as e:
        assert 'unaligned' in str(e)
    try:
        s3 + s5
    except TypeError as e:
        assert 'different' in str(e)

    s9 = Sequence(range(0, 30, 3), range(0, 30, 3))
    assert s9.timestamps == (0, 3, 6, 9, 12, 15, 18, 21, 24, 27)
    s10 = s9[4, 13]
    assert s10.timestamps == (6, 9, 12)
