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
from typing import Optional

from dbmind.common.algorithm.basic import binary_search
from dbmind.common.algorithm.basic import how_many_lesser_elements, how_many_larger_elements
from ..either import OptionalContainer, OptionalValue
from ..utils import cached_property

EMPTY_TUPLE = tuple()


class Sequence:
    def __init__(self, timestamps=None, values=None, name=None, step=None, labels=None):
        """Sequence is an **immutable** data structure, which wraps time series and
        its information.

        .. attention::

            All properties are cached property due to immutability.
            Forced to modify a Sequence object could cause error.

        It is one and only representation for time series in DBMind."""
        timestamps = OptionalValue(timestamps).get(EMPTY_TUPLE)
        values = OptionalValue(values).get(EMPTY_TUPLE)
        self._timestamps = tuple(timestamps)
        self._values = tuple(values)
        success, message = Sequence._check_validity(self._timestamps, self._values)
        if not success:
            raise ValueError(message)

        # ``self.name`` is an optional variable.
        self.name = name
        self._step = step
        self._labels = labels or {}
        # Attach sub-sequence to parent-sequence with logical pointer.
        # By this means, sub-sequence can avoid redundant records.
        self._parent: Sequence = None
        self._parent_start = None
        self._parent_end = None

    @staticmethod
    def _check_validity(timestamps, values):
        # invalid scenarios
        if None in (timestamps, values):
            return False, 'NoneType object is not iterable.'
        if len(timestamps) != len(values):
            return False, 'The length between Timestamps (%d) and values (%d) must be equal.' % (
                len(timestamps), len(values)
            )
        if len(timestamps) != len(set(timestamps)):
            return False, 'The sequence prohibits duplicate timestamp.'
        if not all(x < y for x, y in zip(timestamps, timestamps[1:])):
            return False, 'Timestamps must be strictly increasing.'
        for t in timestamps:
            if type(t) is not int:
                return False, 'The type of timestamp must be integer.'

        # valid
        return True, None

    @staticmethod
    def _create_sub_sequence(parent, ts_start, ts_end):
        """This Sequence slicing method is not the same as list slicing. List in Python
        slices from start index till (end - 1), specified as list elements.
        But the sub-sequence includes the last element, i.e., ts_end.
        """
        if parent is None:
            raise ValueError('Parent should not be NoneType.')
        if OptionalContainer(parent._timestamps).get(0) == ts_start and \
                OptionalContainer(parent._timestamps).get(-1) == ts_end:
            return parent

        sub = Sequence()
        if ts_start > ts_end:
            return sub

        sub._parent = parent
        sub._parent_start = ts_start
        sub._parent_end = ts_end
        return sub

    def _get_entity(self):
        """Sub-sequence does not store data entity. Hence, the method backtracks to the
        start node (aka, head node, ancestor node) and return a quadruple to caller to traverse.

        Return a quadruple:
        ..

            (timestamps, values, starting timestamp, ending timestamp).

        """
        this_ts_start = OptionalContainer(self._timestamps).get(0)
        this_ts_end = OptionalContainer(self._timestamps).get(-1)
        # If current sequence has no parent node, the sequence must be at the start node.
        if not self._parent:
            return self._timestamps, self._values, this_ts_start, this_ts_end

        # Since current sequence has a parent node, we should backtrack until
        # we find the start node and then return the data entity based on
        # the data recorded in the start node.
        start_node = self
        while start_node._parent is not None:
            start_node = start_node._parent

        ts_start = OptionalValue(self._parent_start).get(this_ts_start)
        ts_end = OptionalValue(self._parent_end).get(this_ts_end)
        return start_node._timestamps, start_node._values, ts_start, ts_end

    def get(self, timestamp) -> Optional[int]:
        """Get a target by timestamp.

        :return If not found, return None."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        if None in (ts_start, ts_end):
            return

        if ts_start <= timestamp <= ts_end:
            idx = binary_search(timestamps, timestamp)
            if idx < 0:
                return
            return values[idx]

    @cached_property
    def length(self):
        timestamps, _, ts_start, ts_end = self._get_entity()
        if timestamps == EMPTY_TUPLE:
            return 0
        # Notice: this is a TRICK for binary search:
        # ``how_many_larger_elements()`` can ensure that
        # the position of the searching element always stays
        # at the position of the last element not greater than it in the array.
        start_position = how_many_lesser_elements(timestamps, ts_start)
        end_position = how_many_larger_elements(timestamps, ts_end)
        return end_position - start_position + 1

    def to_2d_array(self):
        return self.timestamps, self.values

    @cached_property
    def values(self):
        """The property will generate a copy."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        return values[how_many_lesser_elements(timestamps, ts_start):
                      how_many_larger_elements(timestamps, ts_end) + 1]

    @cached_property
    def timestamps(self):
        """The property will generate a copy."""
        timestamps, values, ts_start, ts_end = self._get_entity()
        return timestamps[how_many_lesser_elements(timestamps, ts_start):
                          how_many_larger_elements(timestamps, ts_end) + 1]

    @cached_property
    def step(self):
        if self._step is None:
            return measure_sequence_interval(self)
        return self._step

    @property
    def labels(self):
        return self._labels

    def copy(self):
        return Sequence(
            self.timestamps, self.values, self.name, self.step, self.labels
        )

    def to_dict(self):
        return {
            'name': self.name,
            'timestamps': self.timestamps,
            'values': self.values,
            'labels': self.labels,
        }

    def __getitem__(self, item):
        """If parameter ``item`` is a two-tuple, create a sub-sequence and return it.
        If ``item`` is an integer, which represents an index (timestamp) of target, search and return
        the target.

        :exception raise ValueError while item does not belong to any valid types.
        """
        if isinstance(item, int):
            return self.get(item)
        elif isinstance(item, slice):
            raise NotImplementedError
        elif isinstance(item, tuple) and len(item) == 2:
            # To distinguish with slicing, override the tuple form.
            start = OptionalContainer(item).get(0, default=OptionalContainer(self._timestamps).get(0))
            end = OptionalContainer(item).get(1, default=OptionalContainer(self._timestamps).get(-1))
            return Sequence._create_sub_sequence(self, start, end)
        else:
            raise ValueError('Not support %s type.' % type(item))

    def __len__(self):
        return self.length

    def __repr__(self):
        return 'Sequence[%s](%d)%s' % (self.name, self.length, self._labels)

    def __iter__(self):
        """Return a pairwise point (timestamp, value)."""
        return zip(*self.to_2d_array())

    def __add__(self, other):
        if not isinstance(other, Sequence):
            raise TypeError('The data type must be Sequence.')
        return Sequence(
            timestamps=(self.timestamps + other.timestamps),
            values=(self.values + other.values)
        )


def measure_sequence_interval(sequence):
    histogram = dict()
    timestamps = sequence.timestamps
    for i in range(0, len(timestamps) - 1):
        interval = timestamps[i + 1] - timestamps[i]
        histogram[interval] = histogram.get(interval, 0) + 1
    # Calculate the mode of the interval array.
    most_interval = most_count = 0
    for interval, count in histogram.items():
        if count > most_count:
            most_interval = interval
            most_count = count
    return int(most_interval)
