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
import importlib
import json
from abc import abstractmethod

from .errors import SerializationFormatError

JSONABLE_KEYWORD = '_jsonable'
DTYPE_KEYWORD = '_dtype'


def recognize_data_type(v):
    if v.__module__ == 'builtin':
        return 'builtin', v.__name__

    dtype = type(v)
    return dtype.__module__, dtype.__name__


def initialize_cls(module, name, data):
    if module == 'builtin' and name == 'function':
        raise SerializationFormatError(
            'Not supported serialization or deserialization for function type.'
        )
    if module == 'builtin':
        return data
    cls = getattr(importlib.import_module(module), name)
    if isinstance(cls, RPCJSONAble):
        return cls.from_json(data)

    if isinstance(data, list):
        return cls(*data)
    elif isinstance(data, dict):
        return cls(**data)
    else:
        # Maybe raise error.
        return cls(data)


class RPCJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, RPCJSONAble):
            return o.json()
        return str(o)


class RPCJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook,
                         *args, **kwargs)

    @staticmethod
    def object_hook(obj):
        if isinstance(obj, dict) and obj.get(JSONABLE_KEYWORD, False):
            obj.pop(JSONABLE_KEYWORD)
            module, name = obj.pop(DTYPE_KEYWORD)
            return initialize_cls(module, name, obj)
        return obj


class RPCJSONAble:
    _serialize = staticmethod(json.dumps)
    _deserialize = staticmethod(json.loads)

    @staticmethod
    def serialize(obj):
        return json.dumps(obj, cls=RPCJSONEncoder)

    @staticmethod
    def deserialize(s):
        return json.loads(s, cls=RPCJSONDecoder)

    def json(self):
        """Don't override this, override jsonify() instead."""
        obj = self.jsonify()
        # Add fixed fields.
        obj[JSONABLE_KEYWORD] = True
        obj[DTYPE_KEYWORD] = recognize_data_type(self)
        return obj

    @abstractmethod
    def jsonify(self):
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def get_instance(cls, data):
        raise NotImplementedError()

    @classmethod
    def from_json(cls, data):
        """Don't override this, override get_instance() instead."""
        if not isinstance(data, dict):
            raise ValueError('Given deserialize string should be a dict.')

        if not (data[JSONABLE_KEYWORD]
                and data[DTYPE_KEYWORD] != recognize_data_type(cls)):
            raise TypeError('Not supported given data for performing from_json() method. '
                            'Should get data from json() method.')

        data.pop(JSONABLE_KEYWORD)
        data.pop(DTYPE_KEYWORD)
        return cls.get_instance(data)


class RPCRequest(RPCJSONAble):
    def __init__(self, username, pwd, funcname, fargs=(), fkwargs=None):
        self.username = username
        self.pwd = pwd
        self.funcname = funcname
        self.fargs = fargs
        self.fkwargs = fkwargs or {}

    @property
    def args(self):
        return self.fargs

    @property
    def kwargs(self):
        return self.fkwargs

    def jsonify(self):
        return {
            'username': self.username,
            'pwd': self.pwd,
            'funcname': self.funcname,
            'args': [RPCJSONAble.serialize(arg) for arg in self.fargs],
            'kwargs': {k: RPCJSONAble.serialize(v) for k, v in self.fkwargs.items()},
        }

    @classmethod
    def get_instance(cls, data):
        try:
            username = data['username']
            pwd = data['pwd']
            funcname = data['funcname']
            _args = data['args']
            _kwargs = data['kwargs']
        except KeyError as k:
            raise SerializationFormatError('Not found key field %s.' % k)

        args = [RPCJSONAble.deserialize(arg) for arg in _args]
        kwargs = {k: RPCJSONAble.deserialize(v) for k, v in _kwargs.items()}
        return cls(username, pwd, funcname, args, kwargs)


class RPCResponse(RPCJSONAble):
    def __init__(self, request, success=False, result=None, exception=None):
        if not isinstance(request, RPCRequest):
            raise ValueError()

        self.request = request
        self.success = success
        self.exception = exception
        self.result = result

    def jsonify(self):
        return {
            'request': self.request.json(),
            'success': self.success,
            'exception': self.exception,
            'result': RPCJSONAble.serialize(self.result)
        }

    @classmethod
    def get_instance(cls, data):
        try:
            request = data['request']
            success = data['success']
            exception = data['exception']
            result = data['result']
        except KeyError as k:
            raise SerializationFormatError('Not found key field %s.' % k)

        request = RPCRequest.get_instance(request)
        result = RPCJSONAble.deserialize(result)
        return cls(request, success, result, exception)
