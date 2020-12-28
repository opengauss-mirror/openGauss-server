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

import json

from prettytable import PrettyTable

from tuner.utils import construct_dividing_line


class _KnobEncoder(json.JSONEncoder):
    def default(self, o):
        return o.to_dict()


class RecommendedKnobs:
    def __init__(self):
        """
        Package tuned knobs.
        Record recommendation results and final tuning results and outputs formatted results text.
        """
        self._need_tune_knobs = list()
        self._only_report_knobs = list()
        self._tbl = dict()
        self.report = 'There is no report because the current mode specifies the list of tuning knobs ' \
                      'through the configuration file.'

    def output_formatted_knobs(self):
        print(construct_dividing_line('Knob Recommendation Report', padding='*'))
        print(self.report)
        print(construct_dividing_line('Recommended Knob Settings', padding='*'))

        knob_table = PrettyTable()
        knob_table.field_names = ['name', 'recommend', 'min', 'max', 'restart']
        knob_list = list()
        knob_list.extend(self._need_tune_knobs)
        knob_list.extend(self._only_report_knobs)
        for knob in knob_list:
            if knob.type in ('int', 'float'):
                knob_table.add_row([knob.name, knob.default, knob.min, knob.max, knob.restart])
            else:
                knob_table.add_row([knob.name, knob.to_string(knob.default), knob.min, knob.max, knob.restart])
        print(knob_table)

    def dump(self, fp, dump_report_knobs=True):
        """
        Dump the tuning result.
        """
        knob_dict = dict()
        knob_list = list()

        knob_list.extend(self._need_tune_knobs)
        if dump_report_knobs:
            knob_list.extend(self._only_report_knobs)

        for knob in knob_list:
            knob_dict.update(knob.to_dict())
        json.dump(knob_dict, fp, cls=_KnobEncoder, indent=4, sort_keys=True)

    def append_need_tune_knobs(self, *args):
        for knob in args:
            if knob:
                self._need_tune_knobs.append(knob)
                self._tbl[knob.name] = knob

    def append_only_report_knobs(self, *args):
        for knob in args:
            if knob:
                self._only_report_knobs.append(knob)

    def names(self):
        return sorted(self._tbl.keys())

    def __len__(self):
        return len(self._need_tune_knobs)

    def __getitem__(self, item):
        return self._tbl.get(item)

    def __iter__(self):
        return self._need_tune_knobs.__iter__()

    def __bool__(self):
        return len(self._need_tune_knobs) > 0

    def __nonzero__(self):
        return self.__bool__()


class Knob:
    def __init__(self, name, knob):
        """
        Wrap a tuning knob and abstract it as a class.
        The second argument knob is dict type. Its fields include:
            default: Int, float or bool type.
            min: Optional. Int, float type.
            max: Optional. Int, float type.
            type: String type. Constrained by Knob.TYPE.
            restart: Boolean type.

        :param name: The name of a knob.
        :param knob: The dict data-structure of a knob.
        """
        if not isinstance(knob, dict):
            raise TypeError

        self.name = name
        self.type = knob.get('type')
        self.min = knob.get('min')
        self.max = knob.get('max')
        self.default = knob.get('default')
        self.restart = knob.get('restart', False)

        if self.type == 'bool':
            self.min = 0
            self.max = 1

        self._scale = self.max - self.min

        if self._scale < 0:
            raise ValueError('Knob %s is incorrectly configured. '
                             'The max value must be greater than or equal to the min value.' % self.name)

    def to_string(self, val):
        rv = val * self._scale + float(self.min) if self.type in ('int', 'float') else val
        if self.type == 'int':
            rv = str(int(round(rv)))
        elif self.type == 'bool':
            rv = 'on' if rv >= .5 else 'off'
        elif self.type == 'float':
            rv = str(round(rv, 2))
        else:
            raise ValueError('Incorrect type: ' + self.type)

        return rv

    def to_numeric(self, val):
        if self.type in ('float', 'int'):
            rv = (float(val) - float(self.min)) / self._scale
        elif self.type == 'bool':
            rv = 0. if val == 'off' else 1.
        else:
            raise ValueError('Incorrect type: ' + self.type)

        return rv

    @staticmethod
    def new_instance(name, value_default, knob_type, value_min=0, value_max=1, restart=False):
        if knob_type not in Knob.TYPE.ITEMS:
            raise TypeError("The type of parameter 'knob_type' is incorrect.")

        if knob_type == Knob.TYPE.INT:
            value_default = int(value_default)
            value_max = int(value_max)
            value_min = int(value_min)
        elif knob_type == Knob.TYPE.FLOAT:
            value_default = float(value_default)
            value_max = float(value_max)
            value_min = float(value_min)
        else:
            if type(value_default) is not bool:
                raise ValueError

            value_default = 1 if value_default else 0
            value_max = 1
            value_min = 0

        return Knob(name,
                    {'type': knob_type, 'restart': restart,
                     'default': value_default, 'min': value_min, 'max': value_max})

    def to_dict(self):
        return \
            {self.name: {
                'type': self.type, 'restart': self.restart,
                'default': self.default,
                'min': self.min, 'max': self.max
            }}

    def __str__(self):
        return str(self.to_dict())

    class TYPE:
        ITEMS = ['int', 'float', 'bool']

        INT = ITEMS[0]
        FLOAT = ITEMS[1]
        BOOL = ITEMS[2]


def load_knobs_from_json_file(filename):
    knobs = RecommendedKnobs()
    with open(filename) as fp:
        for name, val in json.load(fp).items():
            val['name'] = name
            knobs.append_need_tune_knobs(Knob(name=name, knob=val))

    return knobs
