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
"""This implementation is similar to Java-like Optional."""
from abc import abstractmethod


class Maybe:
    @abstractmethod
    def get(self, *args, **kwargs):
        pass


class OptionalValue(Maybe):
    def __init__(self, value):
        self.value = value

    def get(self, default=None):
        return default if self.value is None else self.value


class OptionalContainer(Maybe):
    def __init__(self, container):
        self.container = container

    def __bool__(self):
        return bool(self.container)

    def get(self, item, default=None):
        try:
            return self.container.__getitem__(item)
        except (IndexError, KeyError):
            return default

    def __getitem__(self, item):
        return self.get(item)
