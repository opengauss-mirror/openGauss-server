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
import re

BLANK = " "


def to_tuples(text):
    """Parse execution result by using gsql
     and convert to tuples."""
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
