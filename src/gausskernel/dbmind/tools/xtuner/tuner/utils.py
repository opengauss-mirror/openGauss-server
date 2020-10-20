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
import logging
import os
import sys
from io import IOBase
from logging import handlers
import datetime
from functools import wraps


def singleton(cls):
    """
    a decorator for singleton design pattern.
    """
    _instance = {}

    @wraps(cls)
    def decorator(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]

    return decorator


class FakeStream(IOBase):
    """
    Fake file-like stream so that we can change system standard output or error stream
    to a file.
    """

    def __init__(self, logger, log_level=logging.INFO, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logger
        self.log_level = log_level

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())


class Logger(object):
    def __init__(self, filepath=None, redirect_stream=False):
        if not filepath:
            filepath = 'log/opengauss_tuner.log'

        dirname = os.path.dirname(filepath)
        if not os.path.exists(dirname):
            os.makedirs(dirname, mode=0o700)
        handler = handlers.RotatingFileHandler(filename=filepath,
                                               maxBytes=1024 * 1024 * 100,
                                               backupCount=5)
        logging_format = logging.Formatter(
            '%(asctime)s [ %(levelname)s ] - %(message)s')
        handler.setFormatter(logging_format)
        self.logger = logging.getLogger('OpenGauss Tuner')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(handler)
        self.handler = handler

        if redirect_stream:
            sys.stdout = FakeStream(self.logger, logging.INFO)

    def warn(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def exception(self, e, location="unknown"):
        self.logger.error("an exception occurred at %s:", location)
        self.logger.exception(e)


@singleton
class SysLogger(Logger):
    stderr = sys.stderr
    stdout = sys.stdout

    def __init__(self, filepath):
        # redirect standard stream to logfile.
        Logger.__init__(self, filepath,
                        redirect_stream=True)

    @staticmethod
    def print(*args, fd=stdout):
        print(*args, file=fd, flush=True)


class Recorder(object):
    def __init__(self, filepath):
        self.fd = open(filepath, 'a+')

        self.fd.write('Recorder starting at {}.\n'.format(datetime.datetime.now()))
        self.best_val = dict()

    def write_text(self, content):
        if type(content) is list:
            self.fd.writelines(content)
        elif type(content) is str:
            self.fd.write(content)
        else:
            self.fd.write(str(content))

        self.fd.write('\n')
        self.fd.flush()

    def write_int(self, val, name='value', info=None):
        if self.best_val.get(name) is None:
            self.best_val[name] = (val, info)
        elif self.best_val[name][1].keys() == info.keys():
            self.best_val[name] = max((val, info), self.best_val[name], key=lambda x: x[0])
        else:
            self.best_val[name] = (val, info)

        self.fd.write(
            'current {name} is {val}, best value is {best}.\n'.format(
                name=name, val=val, best=self.best_val[name]
            )
        )

        if info is not None:
            self.fd.write('info:\n{}\n\n'.format(info))

        self.fd.flush()

    def write_best_val(self, name='value'):
        self.fd.write('best {name} is {value}, info:\n{info}\n\n'.format(
            name=name, value=self.best_val[name][0],
            info=self.best_val[name][1])
        )
        self.fd.flush()

    def __del__(self):
        self.fd.flush()
        self.fd.close()
