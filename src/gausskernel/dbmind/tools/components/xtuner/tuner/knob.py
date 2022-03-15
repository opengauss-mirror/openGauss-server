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

from .utils import construct_dividing_line


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
            row = (knob.name, knob.to_string(knob.current), knob.min, knob.max, knob.restart)
            knob_table.add_row(row)
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
            if knob is None:
                continue
            self._need_tune_knobs.append(knob)
            self._tbl[knob.name] = knob

    def append_only_report_knobs(self, *args):
        for knob in args:
            if knob is None:
                continue
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
    def __init__(self, name, **kwargs):
        """
        Wrap a tuning knob and abstract it as a class.
        The second argument knob is dict type. Its fields include:
            recommend: The knob value recommended by knob recommendation.
            current: Normalized current value.
            original: Str type. A setting value from `pg_settings` or user's configuration.
            min: Optional. Int, float type.
            max: Optional. Int, float type.
            type: String type. Constrained by Knob.TYPE.
            restart: Boolean type.

        :param name: The name of the knob.
        :param kwargs: A dict structure contains the knob's all fields.
        """
        self.name = name
        self.recommend = kwargs.get('recommend')
        self.original = self.current = None
        self.user_set = kwargs.get('default')
        self._min = kwargs.get('min')
        self._max = kwargs.get('max')
        self.type = kwargs.get('type')
        self.restart = kwargs.get('restart', False)

        if '' in (self.name, self.type):
            raise ValueError("'name', and 'type' fields of knob are essential.")

        if self.type == 'bool':
            self._min = 0
            self._max = 1

        if str in (type(self._min), type(self._max)):
            raise ValueError("'min', and 'max' fields of knob should not be str type.")

        # Refresh scale.
        self._scale = None
        self.fresh_scale()

    def to_string(self, val):
        rv = self.denormalize(val) if self.type in ('int', 'float') else val
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
            rv = self.normalize(val)
        elif self.type == 'bool':
            rv = 0. if val == 'off' else 1.
        else:
            raise ValueError('Incorrect type: ' + self.type)

        return rv

    def fresh_scale(self):
        if None in (self._min, self._max):
            return

        self._scale = self._max - self._min
        if self._scale <= 0:
            raise ValueError('Knob %s is incorrectly configured. '
                             'The max value must be greater than '
                             'the min value.' % self.name)
        if type(self.user_set) is str:
            self.current = self.to_numeric(self.user_set)
        elif type(self.user_set) in (int, float):
            self.user_set = str(self.user_set)
            self.current = self.normalize(self.user_set)
        elif self.recommend is not None:
            self.current = self.normalize(self.recommend)

    @property
    def min(self):
        return self._min

    @min.setter
    def min(self, val):
        if val is None:
            return
        self._min = val
        self.fresh_scale()

    @property
    def max(self):
        return self._max

    @max.setter
    def max(self, val):
        if val is None:
            return
        self._max = val
        self.fresh_scale()

    def normalize(self, val):
        return (float(val) - float(self._min)) / self._scale

    def denormalize(self, val):
        return val * self._scale + float(self._min)

    def to_dict(self):
        return \
            {self.name:
                {
                    'type': self.type,
                    'restart': self.restart,
                    'default': self.user_set if self.user_set is not None else self.original,
                    'recommend': self.to_string(self.current),
                    'min': self._min,
                    'max': self._max
                }
            }

    def __str__(self):
        return str(self.to_dict())

    class TYPE:
        ITEMS = ['int', 'float', 'bool']

        INT = ITEMS[0]
        FLOAT = ITEMS[1]
        BOOL = ITEMS[2]


def load_knobs_from_json_file(filename):
    knobs = RecommendedKnobs()
    with open(filename) as f:
        for name, _dict in json.load(f).items():
            knobs.append_need_tune_knobs(
                Knob(name=name, **_dict)
            )

    return knobs
