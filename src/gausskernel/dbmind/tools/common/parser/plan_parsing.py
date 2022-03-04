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

import re

EMPTY = ''
BLANK = ' '
DEFAULT_INDENT_LEN = 2
HIERARCHY_IDENT_LEN = 6
INDENT_BLANK = BLANK * DEFAULT_INDENT_LEN
KV_DELIMITER = ':'
CHILD_NODE_FLAG = '->'

HEADER_PATTERN = re.compile(
    r'(.*?)%s\(cost=(.*?)\.\.(.*?) rows=(.*?) width=(.*?)\)' % INDENT_BLANK)
PROPERTY_PATTERN = re.compile(
    r'(.*?): (.*)')
IDX_SCAN_PATTERN = re.compile(
    r'Index Scan using (.*?) on (.*?)'
)
IDX_ONLY_SCAN_PATTERN = re.compile(
    # r'Index (?:[a-zA-Z_0-9]*?) Scan (?:[a-zA-Z_0-9]*?) using (.*?) on (.*?)'
    r'Index(?:[0-9a-zA-Z ]*?)Scan(?:[0-9a-zA-Z ]*?)using (.*?) on (.*?)$'
)
SEQ_SCAN_PATTERN = re.compile(
    r'Seq Scan on (.*?)'
)


def count_indent(line):
    count = 0
    length = len(line)
    while count < length and line[count] == BLANK:
        count += 1
    return count


def strip_line(line: str):
    return line.replace(CHILD_NODE_FLAG, EMPTY).strip()


class Operator:
    def __init__(self, level=0, parent=None):
        self.name = None
        self.start_cost = self.total_cost = self.exec_cost = 0
        self.rows = self.width = 0
        self.properties = {}

        # Records tree node info:
        self.level = level
        self.parent = parent
        self.children = []  # not only binary tree.

        # Other specific operator info:
        self.table_list = []
        self.index_list = []
        self.type = None

    def update(self, line: str):
        """
        Update information for Operator node.
        :param line: Should be stripped beforehand.
        :return: Return true on success, false on failure.
        """
        if re.match(HEADER_PATTERN, line):
            self._parse_header(line)
        elif re.match(PROPERTY_PATTERN, line):
            self._parse_property(line)
        else:
            return False
        return True  # Not thrown exception means successful.

    def _parse_header(self, line):
        name, start_cost, total_cost, rows, width = re.findall(HEADER_PATTERN, line)[0]
        self.name = name.strip()
        self.start_cost = float(start_cost)
        self.total_cost = float(total_cost)
        self.exec_cost = self.total_cost - self.start_cost
        self.rows = int(rows)
        self.width = int(width)
        self._parse_name()

    def _parse_name(self):
        if self.name.find('Aggregate') >= 0:
            self.type = 'Aggregate'
        elif self.name.find('Sort') >= 0:
            self.type = 'Sort'
        elif self.name.find('Scan') >= 0:
            self.type = 'Scan'
            if self.name.startswith('Index'):
                try:
                    index, tbl = re.findall(IDX_SCAN_PATTERN, self.name)[0]
                except IndexError:
                    index, tbl = re.findall(IDX_ONLY_SCAN_PATTERN, self.name)[0]
                self.index_list.append(index)
                self.table_list.append(tbl)
            elif self.name.startswith('Seq'):
                tbl = re.findall(SEQ_SCAN_PATTERN, self.name)[0]
                self.table_list.append(tbl)
        elif self.name.find('Join') >= 0:
            self.type = 'Join'
        else:
            self.type = 'Other'

    def _parse_property(self, line):
        k, v = re.findall(PROPERTY_PATTERN, line)[0]
        self.properties[k.strip()] = v.strip()

    def __getitem__(self, item):
        return self.properties.get(item)

    def __repr__(self):
        return '%s  (cost=%.2f..%.2f rows=%d width=%d)' % (self.name, self.start_cost,
                                                           self.total_cost, self.rows, self.width)

    def prints(self):
        print(self)
        print(INDENT_BLANK + '<exec_cost=%d level=%d>' % (self.exec_cost, self.level))
        print(INDENT_BLANK + str(self.properties))


class Plan:
    def __init__(self):
        # Records tree structure info:
        self.root_node = None
        self.height = 0

        # execution plan flags:
        self.has_join = self.has_idx = self.has_xxx = self.bypass = False

        # other fields:
        self.primal_indent_len = -1

    def recognize_level(self, line_with_indent):
        """
        The indentation amount of each line in the plan text explains
         the execution tree node's level.
        The indentation calculation formula is as follows:
            starts with CHILD_NODE_FLAG(->):
                INDENT_LEN = PRIMAL_INDENT_LEN + DEFAULT_INDENT_LEN + (level - 1) * HIERARCHY_IDENT_LEN
                where level is at least 1.
            not starts with CHILD_NODE_FLAG(->):
                INDENT_LEN = PRIMAL_INDENT_LEN + DEFAULT_INDENT_LEN + level * HIERARCHY_IDENT_LEN if level >= 1
                INDENT_LEN = PRIMAL_INDENT_LEN if level = 0

        :param line_with_indent: a line of text in the execution plan.
        :return: level
        """
        indent_len = count_indent(line_with_indent)
        if line_with_indent.strip().startswith(CHILD_NODE_FLAG):
            level = (indent_len - self.primal_indent_len - DEFAULT_INDENT_LEN) // HIERARCHY_IDENT_LEN + 1
            return level
        else:
            return 0 if self.primal_indent_len == indent_len else \
                (indent_len - self.primal_indent_len - DEFAULT_INDENT_LEN) // HIERARCHY_IDENT_LEN

    def _has_special_desc(self, line):
        tidy_line = line.strip().lower()
        if tidy_line == '[bypass]':
            self.bypass = True
        else:
            return False

        return True

    def _reset_states(self):
        self.root_node = None
        self.height = 0
        self.has_join = self.has_idx = self.has_xxx = self.bypass = False
        self.primal_indent_len = -1

    def parse(self, text: str):
        # Remove redundant text to interference with the parsing process.
        tidy_text = text.strip('\n')
        # lines = tidy_text.split(r'\n')
        lines = tidy_text.splitlines()
        if len(lines) == 0:
            return

        self._reset_states()
        self.root_node = current_node = Operator()
        self.height = current_level = 0

        for line in lines:
            stripped_line = strip_line(line)
            if stripped_line == EMPTY or self._has_special_desc(stripped_line):
                continue

            if self.primal_indent_len < 0:
                # That means this is the first line, we should count primal indents here.
                self.primal_indent_len = count_indent(line)

            # A line starts with CHILD_NODE_FLAG means that a new operator node needs be created.
            level = self.recognize_level(line)
            if line.strip().startswith(CHILD_NODE_FLAG):
                if level < current_level:
                    # Backtrack until find parent node.
                    # To find the parent node, we must backtrace one more step (level - 1).
                    while current_level > (level - 1):
                        current_node = current_node.parent
                        current_level -= 1

                new_node = Operator(level, parent=current_node)
                # To backtrack, the following sets node relations:
                current_node.children.append(new_node)
                current_node = new_node
                current_level = level
            # Regardless of whether current node is a new node,
            # all possible information should be fetched and updated from the current line.
            current_node.update(stripped_line)
            # Height starts from 1.
            self.height = max(self.height, current_level + 1)

    def traverse(self, callback):
        def recursive_helper(node: Operator):
            if node is None:
                return
            callback(node)
            for child in node.children:
                recursive_helper(child)

        recursive_helper(self.root_node)

    @property
    def sorted_operators(self):
        opts = []

        def append(node):
            opts.append(node)

        self.traverse(append)
        opts.sort(key=lambda n: n.exec_cost, reverse=True)
        for idx, item in enumerate(opts):
            if str.startswith(item.cmdline, 'Sort') or str.startswith(item.cmdline, 'SortAggregate'):
                opts[0], opts[idx] = opts[idx], opts[0]
        return opts

    def __repr__(self):
        lines = []

        def printer(node: Operator):
            indents = INDENT_BLANK * node.level
            lines.append(indents + str(node))
            for k, v in node.properties.items():
                lines.append('%s%s: %s' % (indents, k, v))

        if self.bypass:
            lines.append('[Bypass]')
        self.traverse(printer)
        return '\n'.join(lines)
