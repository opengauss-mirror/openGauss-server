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
"""This HttpService supports multiple backends, including:
 fastAPI, flask, and waitress."""
import logging
import ssl
from urllib.parse import quote


class BACKEND_TYPES:
    FAST_API = 0
    PURE_FLASK = 1
    FLASK_WITH_WAITRESS = 2


try:
    """FastAPI is more modern, so we regard it as the first choice."""
    from fastapi import FastAPI
    from fastapi import Response as _Response

    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    _BACKEND = BACKEND_TYPES.FAST_API
except ImportError:
    from flask import Flask
    from flask import Response as _Response
    from flask import request as _request

    _BACKEND = BACKEND_TYPES.PURE_FLASK
    try:
        from waitress import serve

        _BACKEND = BACKEND_TYPES.FLASK_WITH_WAITRESS
    except ImportError:
        pass

_RequestMappingTable = dict()


class Request:
    """A wrapper for Request."""

    def __init__(self, req):
        self.req = req

    @property
    def json(self):
        if _BACKEND >= BACKEND_TYPES.PURE_FLASK:
            return self.req.json
        elif _BACKEND == BACKEND_TYPES.FAST_API:
            return self.req.json()

    @property
    def body(self):
        if _BACKEND >= BACKEND_TYPES.PURE_FLASK:
            return self.req.body
        elif _BACKEND == BACKEND_TYPES.FAST_API:
            return self.req.body()

    @property
    def url(self):
        return str(self.req.url)

    @property
    def cookies(self):
        return self.req.cookies

    @staticmethod
    def parse(args):
        if _BACKEND >= BACKEND_TYPES.PURE_FLASK:
            return _request
        elif _BACKEND == BACKEND_TYPES.FAST_API:
            if len(args) == 0 or len(args[0]) == 0:
                return
            request = args.pop(0)[0]
            return Request(request)


class Response(_Response):
    def __init__(
            self, content=None, status_code=200, headers=None, mimetype=None, **kwargs
    ):
        super().__init__(content, status_code, headers, mimetype, **kwargs)


class RequestLogger:
    """Wrap a WSGI application to log requests.
    The format of logger refers to ``paste.TransLogger``."""
    format = ('%(REMOTE_ADDR)s - %(REMOTE_USER)s '
              '"%(REQUEST_METHOD)s %(REQUEST_URI)s %(HTTP_VERSION)s" '
              '%(status)s %(bytes)s "%(HTTP_REFERER)s" "%(HTTP_USER_AGENT)s"')

    def __init__(self, application):
        self.application = application

    def __call__(self, environ, start_response):
        req_uri = quote(environ.get('SCRIPT_NAME', '')
                        + environ.get('PATH_INFO', ''))
        if environ.get('QUERY_STRING'):
            req_uri += '?' + environ['QUERY_STRING']
        method = environ['REQUEST_METHOD']

        def start_response_wrapper(status, headers, exc_info=None):
            # headers is a list of two-tuple, we should traverse it here.
            content_length = '?'
            for name, value in headers:
                if name.lower() == 'content-length':
                    content_length = value
            RequestLogger.log(environ, method, req_uri, status, content_length)
            return start_response(status, headers)

        return self.application(environ, start_response_wrapper)

    @staticmethod
    def log(environ, method, req_uri, status, content_length):
        remote_addr = '-'
        if environ.get('HTTP_X_FORWARDED_FOR'):
            remote_addr = environ['HTTP_X_FORWARDED_FOR']
        elif environ.get('REMOTE_ADDR'):
            remote_addr = environ['REMOTE_ADDR']
        message = RequestLogger.format % {
            'REMOTE_ADDR': remote_addr,
            'REMOTE_USER': environ.get('REMOTE_USER') or '-',
            'REQUEST_METHOD': method,
            'REQUEST_URI': req_uri,
            'HTTP_VERSION': environ.get('SERVER_PROTOCOL'),
            'status': status.split(None, 1)[0],
            'bytes': content_length,
            'HTTP_REFERER': environ.get('HTTP_REFERER', '-'),
            'HTTP_USER_AGENT': environ.get('HTTP_USER_AGENT', '-'),
        }
        logging.log(logging.INFO, message)


class HttpService:
    """A Http service implementation.
    ~~~~~~~~~~~~~~~~~~

    To decouple web service framework and web service interface, DBMind implements the class.
    In this way, DBMind can change to another web framework (e.g., web.py, ASGI) easily."""

    def __init__(self, name=__name__):
        if _BACKEND >= BACKEND_TYPES.PURE_FLASK:
            self.app = Flask(name)
        elif _BACKEND == BACKEND_TYPES.FAST_API:
            self.app = FastAPI(title=name)
        else:
            raise AssertionError('Should not run to here.')

        self.rule_num = 0

    def attach(self, func, rule, **options):
        """Attach a rule to the backend app."""
        is_api = options.pop('api', False)
        if _BACKEND >= BACKEND_TYPES.PURE_FLASK:
            endpoint = options.pop("endpoint", None)
            rule = rule.replace('{', '<').replace('}', '>')
            self.app.add_url_rule(rule, endpoint, func, **options)
        elif _BACKEND == BACKEND_TYPES.FAST_API:
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

    def register_controller_module(self, module_name):
        __import__(module_name)
        for rule, items in _RequestMappingTable.items():
            f, options = items
            self.attach(f, rule, **options)

    def start_listen(self, host, port,
                     ssl_keyfile=None, ssl_certfile=None, ssl_keyfile_password=None):
        if _BACKEND != BACKEND_TYPES.FAST_API and (ssl_keyfile or ssl_certfile):
            raise NotImplementedError(
                'Not supported Https for flask. You should install fastapi and uvicorn.'
            )

        if _BACKEND == BACKEND_TYPES.FLASK_WITH_WAITRESS:
            serve(RequestLogger(self.app), _quiet=True,
                  listen="{host}:{port}".format(host=host, port=port))
        elif _BACKEND == BACKEND_TYPES.PURE_FLASK:
            self.app.run(host=host, port=port)
        elif _BACKEND == BACKEND_TYPES.FAST_API:
            config = uvicorn.Config(self.app, host=host, port=port,
                                    ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile,
                                    ssl_keyfile_password=ssl_keyfile_password,
                                    log_config=None)
            config.load()
            if config.is_ssl:
                config.ssl.options |= (
                    ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
                )  # RFC 7540 Section 9.2: MUST be TLS >=1.2
                config.ssl.set_ciphers('DHE+AESGCM:ECDHE+AESGCM')
            server = uvicorn.Server(config)
            server.run()


def request_mapping(rule, **kwargs):
    """To record to a static mapping dict."""

    def decorator(f):
        _RequestMappingTable[rule] = (f, kwargs)
        return f

    return decorator

