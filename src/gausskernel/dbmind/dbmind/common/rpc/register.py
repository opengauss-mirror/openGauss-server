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


class RPCFunctionRegister(dict):
    def __init__(self):
        """A dict that can not be modified after setting."""
        super().__init__()

    def _undeleted(self, *args, **kws):
        raise TypeError('RPCFunctionRegister is undeleted.')

    def __setitem__(self, key, value):
        if not (isinstance(key, str) and callable(value)):
            raise TypeError('Key must be str and value must be callable.')
        if key in self.keys():
            raise TypeError('Forbidden to modify.')
        self.setdefault(key, value)

    def __getitem__(self, item):
        def dummy(*args, **kwargs):
            raise NotImplementedError('Not found this function %s.' % item)

        return self.get(item, dummy)

    def register(self):
        def decorator(f):
            self.setdefault(f.__name__, f)
            return f

        return decorator

    __delitem__ = _undeleted
    clear = _undeleted
    update = _undeleted
    pop = _undeleted
    popitem = _undeleted
