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

import os
import re

BLANK = " "
RED_FMT = "\033[31;1m{}\033[0m"
GREEN_FMT = "\033[32;1m{}\033[0m"
YELLOW_FMT = "\033[33;1m{}\033[0m"
WHITE_FMT = "\033[37;1m{}\033[0m"

config = None


class cached_property:
    """
    A decorator for caching properties in classes.
    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self

        val = self.func(instance)
        setattr(owner, self.func.__name__, val)
        return val


def clip(val, lower, upper):
    """
    Given an interval, the value outside the interval is clipped to the interval edges.
    :param val: The value to clip.
    :param lower: Minimum value.
    :param upper: Maximum value.
    :return: Clipped value.
    """
    val = max(lower, val, key=lambda x: float(x))
    val = min(upper, val, key=lambda x: float(x))
    return val


def construct_dividing_line(title='', padding='-'):
    """
    Return a dividing line.
    """
    try:
        term_width = os.get_terminal_size().columns
    except OSError:
        term_width = 120

    side_width = max(0, (term_width - len(title)) // 2 - 1)

    if title == '':
        return padding * term_width
    else:
        return padding * side_width + ' ' + title + ' ' + padding * side_width


def to_tuples(text):
    lines = text.splitlines()
    separator_location = -1
    for i, line in enumerate(lines):
        # Find separator line such as '-----+-----+------'.
        if re.match(r'^\s*?[-|+]+\s*$', line):
            separator_location = i
            break

    if separator_location < 0:
        return []

    separator = lines[separator_location]
    left = 0
    right = len(separator)
    locations = list()
    while left < right:
        try:
            location = separator.index('+', left, right)
        except ValueError:
            break
        locations.append(location)
        left = location + 1
    # Record each value start location and end location.
    pairs = list(zip([0] + locations, locations + [right]))
    tuples = []
    row = []
    wrap_flag = False
    # Continue to parse each line.
    for line in lines[separator_location + 1:]:
        # Prevent from parsing bottom lines.
        if len(line.strip()) == 0 or re.match(r'\(\d+ rows?\)', line):
            continue
        # Parse a record to tuple.
        if wrap_flag:
            row[-1] += line[pairs[-1][0] + 1: pairs[-1][1]].strip()
        else:
            for start, end in pairs:
                # Increase 1 to start index to go over vertical bar (|).
                row.append(line[start + 1: end].strip())

        if len(line) == right and re.match(r'.*\s*\+$', line):
            wrap_flag = True
            row[-1] = row[-1].strip('+').strip(BLANK) + BLANK
        else:
            tuples.append(tuple(row))
            row = []
            wrap_flag = False
    return tuples
