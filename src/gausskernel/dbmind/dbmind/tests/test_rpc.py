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
import pytest

from dbmind.common.rpc import RPCFunctionRegister, start_rpc_service, stop_rpc_service, ping_rpc_url
from dbmind.common.rpc import RPCJSONAble
from dbmind.common.rpc import RPCRequest, RPCResponse, RPCClient
from dbmind.common.rpc.errors import RPCExecutionError, RPCConnectionError
from dbmind.common.rpc.server import RPCExecutionThread
from dbmind.common.types import Sequence


def test_rpc_jsonable():
    # test for Sequence
    seq = Sequence(range(1, 10), range(1, 10), 'test_metric')
    subseq = seq[1, 5]
    seq_json = RPCJSONAble.serialize(seq)
    seq_new = RPCJSONAble.deserialize(seq_json)
    assert seq == seq_new
    assert Sequence.from_json(seq.json()) == seq
    subseq_json = RPCJSONAble.serialize(subseq)
    subseq_new = RPCJSONAble.deserialize(subseq_json)
    assert subseq == subseq_new
    assert Sequence.from_json(subseq.json()) == subseq

    # test for common data structure
    dictionary = {'int': 1, 'float': 2.21, 'class': {'seq': seq},
                  'bool': True,
                  'list': [1, 2, '3']}
    d = RPCJSONAble.serialize(dictionary)
    assert RPCJSONAble.deserialize(d) == dictionary


def test_rpc_request_and_response():
    request = RPCRequest('username', 'password', 'sum', fargs=[1, 2])
    response = RPCResponse(request, True, 3)
    assert request.__dict__ == RPCRequest.from_json(request.json()).__dict__
    res_exp = response.__dict__
    res_rst = RPCResponse.from_json(response.json()).__dict__
    assert res_exp.pop('request').__dict__ == res_rst.pop('request').__dict__
    assert res_exp == res_rst


@pytest.fixture(scope='session')
def rpc_client_testing():
    mapper = RPCFunctionRegister()

    @mapper.register()
    def func(a, b):
        return a + b

    mapper['sum'] = sum

    assert mapper['sum'] == sum
    assert mapper['func'](1, 2) == 3
    assert mapper['sum']([1, 2]) == 3

    try:
        mapper['sum'] = max
    except TypeError as e:
        assert e
    else:
        raise AssertionError()

    test_user = 'username'
    test_pwd = 'password'

    # test for expected process.
    s = start_rpc_service(mapper, host='127.0.0.1', port=5454, uri='/rpc',
                          username=test_user, pwd=test_pwd)

    client = RPCClient('http://127.0.0.1:5454/rpc', test_user, test_pwd)
    assert client.call('sum', [1, 2, 3]) == 6
    assert client.call('func', a=100, b=1.1) == 101.1
    try:
        client.call('sum', 1, 1)
    except RPCExecutionError as e:
        assert 'TypeError' in str(e)
    else:
        raise AssertionError()

    # test for concurrence.
    threads = []

    for i in range(100):
        t = RPCExecutionThread(target=client.call, args=('func', 0, i))
        t.setName(str(i))
        threads.append(t)

        threads.append(
            RPCExecutionThread(
                target=client.call_with_another_credential,
                args=('faked', 'faked', 'func', 0, i)
            )
        )
    for t in threads:
        t.start()

    for i in range(10):
        assert not client.handshake('faked', 'faked')

    results = set()
    exceptions = []
    for t in threads:
        t.join()
        if t.result is not None:
            results.add(t.result)
        if t.exception:
            exceptions.append(t.exception)

    assert len(results) == 100
    assert len(exceptions) == 100

    # test for no functions.
    try:
        client.call('nothisfunc', 1, 2)
    except RPCExecutionError as e:
        assert 'Not found' in str(e)
    else:
        raise AssertionError()

    # test for bad username or pwd.
    try:
        client2 = RPCClient('http://127.0.0.1:5454/rpc', 'badusername', 'wrongpwd')
        assert not client2.handshake()
        client2.call('func', 1, 2)
    except RPCExecutionError as e:
        assert 'authorization' in str(e)
    else:
        raise AssertionError()

    assert ping_rpc_url('http://127.0.0.1:5454/rpc')
    assert ping_rpc_url('http://127.0.0.1:5454')

    stop_rpc_service(s)

    assert not ping_rpc_url('http://127.0.0.1:5454/rpc')

    # test for wrong endpoint.
    client = RPCClient('http://127.0.0.1:1234/rpc', test_user, test_pwd)
    try:
        client.set_timeout(0.1)
        client.call('func', 1, 2)
    except RPCConnectionError:
        pass
    else:
        raise AssertionError()
