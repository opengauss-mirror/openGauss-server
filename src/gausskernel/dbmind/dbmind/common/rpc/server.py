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
import threading
import time
import traceback
from typing import Callable

from dbmind.common.http import HttpService
from .base import RPCResponse, RPCRequest

DEFAULT_URI = '/rpc'
# ? is a special character which doesn't allow in function name.
# Therefore, we attach it into flags.
HEARTBEAT_FLAG = '?HEARTBEAT'
AUTH_FLAG = '?AUTHORIZATION'


class RPCExecutionThread(threading.Thread):
    """A returnable thread."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.result = None
        self.exception = None

    def run(self) -> None:
        try:
            if self._target:
                self.result = self._target(*self._args, **self._kwargs)
        except Exception:
            self.exception = traceback.format_exc()
        finally:
            del self._target, self._args, self._kwargs


class RPCServer:
    def __init__(self, register, credential_checker: Callable, executor=RPCExecutionThread):
        self.register = register
        self.checker = credential_checker
        self.rpc_executor = executor

        if not (hasattr(self.rpc_executor, 'start')
                and hasattr(self.rpc_executor, 'join')):
            raise ValueError('Unsupported executor.')

    def invoke_handler(self, _json: dict):
        """This is a main entry for RPCServer.
        This function can be put at a router or handler.

        :param _json: dict format, must be able to cast to RPCRequest.
        :return: dict format, converted from RPCResponse.
        """
        try:
            req = RPCRequest.from_json(_json)
        except Exception as e:
            return RPCResponse(
                RPCRequest(None, None, 'unknown'), success=False,
                exception='Cannot parse given RPCRequest JSON: %s.' % e
            ).json()

        try:
            funcname = req.funcname

            # Just for heartbeat.
            if funcname == HEARTBEAT_FLAG:
                return RPCResponse(
                    req, success=True, result='ok'
                ).json()

            # Validate credential.
            if not self.checker(req.username, req.pwd):
                return RPCResponse(req, success=False,
                                   exception='Failed to validate authorization.').json()

            # If request is only for authorization test, we can return here.
            if funcname == AUTH_FLAG:
                return RPCResponse(
                    req, success=True, result='ok'
                ).json()

            if funcname not in self.register:
                return RPCResponse(
                    req, success=False,
                    exception='Not found the function %s.' % funcname
                ).json()

            func = self.register[funcname]
            thr = self.rpc_executor(
                target=func, name='Executing %s' % funcname,
                args=req.args, kwargs=req.kwargs
            )
            thr.start()
            thr.join()
            if thr.exception:
                return RPCResponse(req, success=False,
                                   exception=thr.exception).json()
            return RPCResponse(
                req, success=True, result=thr.result
            ).json()
        except Exception as e:
            # unexpected or unusual errors.
            return RPCResponse(
                req, success=False,
                exception='Unexpected error occurred: %s.' % e
            ).json()


class RPCListenService:
    def __init__(self, thread, http_service):
        """A utility data structure for start_rpc_service() and stop_rpc_service()."""
        self.thread = thread
        self.http_service = http_service


def start_rpc_service(
        register,
        host, port,
        uri=DEFAULT_URI,
        username=None, pwd=None,
        ssl_keyfile=None, ssl_certfile=None, ssl_keyfile_password=None
):
    def checker(u, p):
        return u == username and p == pwd

    rpc = RPCServer(register, credential_checker=checker)
    service = HttpService()

    def adaptor(_json: dict):
        return rpc.invoke_handler(_json)

    service.attach(adaptor, uri, methods=['POST'], api=True)
    # Don't want to block at service listening. Therefore, create a new thread to
    # start listening.
    t = threading.Thread(
        target=service.start_listen,
        args=(host, port, ssl_keyfile, ssl_certfile, ssl_keyfile_password)
    )
    t.start()
    # Wait for the server start complete.
    while not service.started:
        time.sleep(0.1)
    return RPCListenService(t, service)


def stop_rpc_service(service: RPCListenService):
    service.http_service.shutdown()
    # Wait for the server stop complete.
    service.http_service.wait_for_shutting_down()
    service.thread.join()  # Block here until Http service closes.
