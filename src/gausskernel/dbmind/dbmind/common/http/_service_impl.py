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
import contextlib
import inspect
import json
import logging
import os
import ssl
import sys
import threading
import time
from functools import partial, wraps

from dbmind.common.security import safe_random_string
from dbmind.common.utils import TTLOrderedDict, dbmind_assert

"""FastAPI is more modern, so we regard it as the first choice and abandon flask."""
from fastapi import FastAPI, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.exceptions import StarletteHTTPException
from fastapi.responses import JSONResponse, Response
from fastapi.openapi.models import OAuth2 as OAuth2Model
from starlette.staticfiles import StaticFiles
import uvicorn

_RequestMappingTable = dict()
dbmind_assert(Response)


class HttpService:
    """A Http service implementation.
    ~~~~~~~~~~~~~~~~~~

    To decouple web service framework and web service interface, DBMind implements the class.
    In this way, DBMind can change to another web framework easily.

    DBMind easily migrated from the flask to the fastapi through this class.
    """

    def __init__(self, name=__name__):
        self.app = FastAPI(title=name)
        self._server = None
        self.static_directory = None
        self.need_to_exit = False

        @self.app.exception_handler(StarletteHTTPException)
        async def exception_handler(_, exc):
            return JSONResponse(
                content={'success': False, 'msg': str(exc.detail)},
                status_code=exc.status_code
            )

        self.rule_num = 0

    def attach(self, func, rule, **options):
        """Attach a rule to the backend app."""
        is_api = options.pop('api', False)
        rule = rule.replace('<', '{').replace('>', '}')
        if is_api:
            self.app.add_api_route(rule, func, **options)
        else:
            self.app.add_route(rule, func, **options)
        self.rule_num += 1

    def route(self, rule, **options):
        def decorator(f):
            self.attach(f, rule, **options)
            return f

        return decorator

    @staticmethod
    def register_controller_module(module_name):
        __import__(module_name)

    def mount_static_files(self, directory):
        if not os.path.exists(directory) and os.path.isdir(directory):
            raise NotADirectoryError(directory)

        self.static_directory = directory

    def start_listen(self, host, port,
                     ssl_keyfile=None, ssl_certfile=None, ssl_keyfile_password=None,
                     ssl_ca_file=None):
        this_service = self

        class Server(uvicorn.Server):
            def install_signal_handlers(self) -> None:
                pass

            @contextlib.contextmanager
            def run_in_thread(self):
                thread = threading.Thread(target=self.run, name='WebServiceThread')
                thread.daemon = True
                thread.start()

                try:
                    # Will block here.
                    while not this_service.need_to_exit:
                        time.sleep(0.001)
                    yield
                finally:
                    thread.join(5)
                    _RequestMappingTable.clear()

        # attach global request mapping table.
        for rule, items in _RequestMappingTable.items():
            f, options = items
            self.attach(f, rule, **options)

        if self.static_directory:
            self.app.mount(
                '/', StaticFiles(directory=self.static_directory), 'ui'
            )

        config = uvicorn.Config(self.app, host=host, port=port,
                                ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
                                ssl_keyfile_password=ssl_keyfile_password,
                                ssl_ca_certs=ssl_ca_file,
                                ssl_cert_reqs=ssl.CERT_REQUIRED,
                                log_config=None)
        config.load()
        if config.is_ssl:
            config.ssl.options |= (
                    ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
            )  # RFC 7540 Section 9.2: MUST be TLS >=1.2
            config.ssl.set_ciphers('DHE+AESGCM:ECDHE+AESGCM')
        self._server = Server(config)
        if sys.version_info >= (3, 7):
            with self._server.run_in_thread():
                pass
        else:
            try:
                self._server.run()
            except RuntimeError:
                # Ignore it due to force exit.
                pass

    @property
    def started(self):
        if self._server:
            return self._server.started
        return False

    def wait_for_shutting_down(self):
        lifespan = self._server.lifespan
        if hasattr(lifespan, 'shutdown_event'):
            lifespan.shutdown_event.wait()

    def shutdown(self):
        self.need_to_exit = True
        self._server.handle_exit(None, None)
        self._server.shutdown()
        if sys.version_info < (3, 7):
            import asyncio
            asyncio.get_event_loop().stop()


def request_mapping(rule, **kwargs):
    """To record to a static mapping dict.

    Notice: This mapper will be used for all HttpServices if you have many.
    """

    def decorator(f):
        _RequestMappingTable[rule] = (f, kwargs)
        return f

    return decorator


class OAuth2:
    __token_url__ = 'token'
    __session_ttl__ = 600  # The limit time for no operations to clean token.

    # Notice: the following variables are static.
    # When you new one more objects, they may be conflict,
    # particularly in multiple-threading or multiple-process.
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl=__token_url__)
    timed_session = TTLOrderedDict(__session_ttl__)

    # thread-unsafe
    __instance__ = None

    @staticmethod
    def get_instance(token_url, pwd_checker, ttl=__session_ttl__):
        """This class has to be implemented with the singleton pattern
        since `oauth2_scheme` and `timed_session` are static, which means
        if we new multiple instances with different startup parameters,
        these instances will be conflict.
        """

        if OAuth2.__instance__:
            return OAuth2.__instance__

        OAuth2.__token_url__ = token_url
        OAuth2.__session_ttl__ = ttl
        OAuth2.oauth2_scheme.model = OAuth2Model(
            flows=dict(password={"tokenUrl": token_url, "scopes": {}})
        )
        OAuth2.timed_session.ttl = ttl
        OAuth2.__instance__ = OAuth2(pwd_checker)
        return OAuth2.__instance__

    def __init__(self, pwd_checker):
        self._session = threading.local()
        dbmind_assert(callable(pwd_checker))
        self._pwd_checker = pwd_checker

    @property
    def token_url(self):
        return OAuth2.__token_url__

    @property
    def credential(self):
        token = self._session.token
        return OAuth2.timed_session.get(token, default=(None, None))

    @property
    def token(self):
        return self._session.token

    @staticmethod
    def set_token_ttl(seconds):
        OAuth2.timed_session.ttl = seconds

    def login_handler(self, form: OAuth2PasswordRequestForm = Depends()):
        try:
            if not self._pwd_checker(form.username, form.password):
                raise HTTPException(400, detail='Incorrect username or password.')
        except ConnectionError:
            raise HTTPException(500, detail='Cannot connect to the agent. Please check whether the agent '
                                            'has been deployed correctly.')

        # Generate a unique token.
        token = safe_random_string(16)
        while token in OAuth2.timed_session:
            token = safe_random_string(16)
        OAuth2.timed_session[token] = (form.username, form.password)

        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": OAuth2.timed_session.ttl
        }

    def add_token_rule(self, service):
        dbmind_assert(isinstance(service, HttpService))

        service.attach(
            partial(self.login_handler, self=self), self.token_url, methods=['POST'], api=True
        )

    @staticmethod
    async def _authenticate(token: str = Depends(oauth2_scheme)):
        if token not in OAuth2.timed_session:
            raise HTTPException(401, 'Not authenticated or expired.')

        OAuth2.timed_session.refresh_ttl(token)
        return token

    def token_authentication(self):
        def decorator(f):
            @wraps(f)
            def wrapped(*args, token: str = Depends(self._authenticate), **kwargs):
                self._session.token = token
                rv = f(*args, **kwargs)
                self._session.token = None
                return rv

            # Append Oauth field into the parameter list of this function. Then, fastapi can
            # observe that this controller has an authorization dependent.
            sig = inspect.signature(f)
            token_param = inspect.Parameter(
                name='token', kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
                default=Depends(self._authenticate), annotation=str
            )

            sig = sig.replace(parameters=tuple(sig.parameters.values()) + (token_param,))
            wrapped.__signature__ = sig
            return wrapped

        return decorator


def standardized_api_output(f):
    class ToleratedEncoder(json.JSONEncoder):
        """Allow to encode more types and abnormal values."""

        def default(self, o):
            return str(o)

        def iterencode(self, o, _one_shot=False):
            try:
                return super().iterencode(o, _one_shot)
            except ValueError:
                if self.check_circular:
                    markers = {}
                else:
                    markers = None
                if self.ensure_ascii:
                    _encoder = json.encoder.encode_basestring_ascii
                else:
                    _encoder = json.encoder.encode_basestring

                def floatstr(o_):
                    if o_ != o_:
                        text = 'null'  # cast NaN to null.
                    elif o_ == float('inf'):
                        text = 'Infinity'
                    elif o_ == -float('inf'):
                        text = '-Infinity'
                    else:
                        return str(o_)

                    return text

                make_iterencode = getattr(json.encoder, '_make_iterencode')
                _iterencode = make_iterencode(
                    markers, self.default, _encoder, self.indent, floatstr,
                    self.key_separator, self.item_separator, self.sort_keys,
                    self.skipkeys, _one_shot)
                return _iterencode(o, 0)

    class ToleratedJSONResponse(JSONResponse):
        def render(self, content) -> bytes:
            return json.dumps(
                content,
                ensure_ascii=False,
                allow_nan=False,
                indent=None,
                sort_keys=True,
                separators=(",", ":"),
                cls=ToleratedEncoder
            ).encode("utf-8")

    @wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return ToleratedJSONResponse(content={
                'success': True,
                'data': f(*args, **kwargs)
            }
            )
        except HTTPException as e:
            raise e
        except Exception as e:
            logging.getLogger('uvicorn.error').exception(e)
            return {
                'success': False,
                'msg': str(e)
            }

    return wrapper
