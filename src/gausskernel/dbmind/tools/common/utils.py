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
import argparse
import logging
import os
import re
import sys
import types
import time
import multiprocessing
import subprocess
import traceback
import threading
from functools import wraps
from datetime import datetime
from queue import Empty
from logging.handlers import RotatingFileHandler

RED_FMT = "\033[31;1m{}\033[0m"
GREEN_FMT = "\033[32;1m{}\033[0m"
YELLOW_FMT = "\033[33;1m{}\033[0m"
WHITE_FMT = "\033[37;1m{}\033[0m"


class cached_property:
    """A decorator for caching a property."""

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        if instance is None:
            return self

        value = self.func(instance)
        setattr(instance, self.func.__name__, value)
        return value


def memoize(func):
    """The function is a generic cache,
     which won't cache unhashable types (e.g., dict, list) but only the immutable types."""
    memtbl = {}

    @wraps(func)
    def wrapper(*args):
        if args in memtbl:
            return memtbl[args]
        else:
            rv = func(*args)
            memtbl[args] = rv
            return rv

    return wrapper


def where_am_i(fvars):
    """Return the module which current function runs on.

    :param fvars: the return value of function ``globals()``.
    """
    file, name = fvars.get('__file__'), fvars.get('__name__')
    if None in (file, name):
        return None
    return name


def read_simple_config_file(filepath):
    """Read the content of ``key=value`` format configuration file.
    The default prefix of comment is sharp (#). e.g.,
    ::

        # The following is a demonstration.
        key1 = value1
        key2 = value2 # some comments


    """
    conf = dict()
    with open(filepath, encoding='UTF-8') as fp:
        lines = fp.readlines()
        configs = map(
            lambda tup: (tup[0].strip(), tup[1].strip()),
            filter(
                lambda tup: len(tup) == 2,
                map(
                    lambda line: line.split('='),
                    filter(
                        lambda line: not (line.startswith('#') or line == ''),
                        map(
                            lambda line: line.strip(),
                            lines
                        )
                    )
                )
            )
        )

        for name, value in configs:
            conf[name] = value

    return conf


def write_to_terminal(
        message,
        level='info',
        color=None
):
    levels = ('info', 'error')
    colors = ('white', 'red', 'green', 'yellow', None)
    dbmind_assert(color in colors and level in levels)

    if not isinstance(message, str):
        message = str(message)

    # coloring.
    if color == 'white':
        out_message = WHITE_FMT.format(message)
    elif color == 'red':
        out_message = RED_FMT.format(message)
    elif color == 'green':
        out_message = GREEN_FMT.format(message)
    elif color == 'yellow':
        out_message = YELLOW_FMT.format(message)
    else:
        out_message = message

    # choosing a streaming.
    if level == 'error':
        sys.stderr.write(out_message)
        sys.stderr.write(os.linesep)
        sys.stderr.flush()
    else:
        sys.stdout.write(out_message)
        sys.stdout.write(os.linesep)
        sys.stdout.flush()


class MultiProcessingRFHandler(RotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._queue = multiprocessing.Queue(-1)
        self._should_exit = False
        self._receiv_thr = threading.Thread(target=self._receive, name='LoggingReceiverThread')
        self._receiv_thr.daemon = True
        self._receiv_thr.start()

    def _receive(self):
        while True:
            try:
                if self._should_exit and self._queue.empty():
                    break
                record = self._queue.get(timeout=.2)
                super().emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except (OSError, EOFError):
                break
            except Empty:
                pass
            except:
                traceback.print_exc(file=sys.stderr)
        self._queue.close()
        self._queue.join_thread()
 
    def _send(self, s):
        self._queue.put_nowait(s)

    def emit(self, record):
        try:
            if record.args:
                record.msg = record.msg % record.args
                record.args = None
            if record.exc_info:
                self.format(record)
                record.exc_info = None
            self._send(record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def close(self):
        if not self._should_exit:
            self._should_exit = True
            self._receiv_thr.join(5)
            super().close()


class ExceptionCatch:
    """Class for catching object exception"""
    def __init__(self, strategy='warn', name='UNKNOWN'):
        """
        :param strategy: Exception handling strategy
        :param name: The object from which the exception came
        """
        self.strategy = strategy
        self.name = name

    def __get__(self, instance, cls):
        if instance is None:
            return self
        return types.MethodType(self, instance)

    def __call__(self, func):
        wraps(func)(self)

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if self.strategy == 'warn':
                    logging.warning(f"[{self.name}] {func.__name__} occurred exception: {str(e)}", exc_info=True)
                elif self.strategy == 'exit':
                    raise e
                else:
                    raise ValueError('Not support strategy %s' % self.strategy)

        return wrapper


def set_proc_title(name: str):
    new_name = name.encode('ascii', 'replace')

    try:
        import ctypes
        libc = ctypes.CDLL('libc.so.6')
        progname = ctypes.c_char_p.in_dll(libc, '__progname_full')
        with open('/proc/self/cmdline') as fp:
            old_progname_len = len(fp.readline())
        if old_progname_len > len(new_name):
            # padding blank chars
            new_name += b' ' * (old_progname_len - len(new_name))

        # for `ps` command:
        # Environment variables are already copied to Python app zone.
        # We can get environment variables by `os.environ` module,
        # so we can ignore the destroying from the following action.
        libc.strcpy(progname, ctypes.c_char_p(new_name))
        # for `top` command and `/proc/self/comm`:
        buff = ctypes.create_string_buffer(len(new_name) + 1)
        buff.value = new_name
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)
    except Exception as e:
        logging.debug('An error (%s) occured while setting the process name.', e)


def retry(times_limit=2):
    """A decorator which helps to retry while an exception occurs."""
    def decorator(func):
        def wrap(*args, **kwargs):
            try_times = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    try_times += 1
                    if try_times < times_limit:
                        logging.warning(
                            'Caught an exception while %s running, and try to run again.', func.__name__
                        )
                        continue
                    else:
                        raise e
        return wrap
    return decorator


def keep_inputting_until_correct(prompt, options):
    input_char = ''
    while input_char not in options:
        input_char = input(prompt).upper()
    return input_char


def check_positive_integer(value):
    if re.match(r'^\d+$', value):
        return int(value)
    else:
        raise argparse.ArgumentTypeError('%s is not a valid number.' % value)


def check_positive_float(value):
    if re.match(r'^\d+(.\d*)?$', value):
        return float(value)
    else:
        raise argparse.ArgumentTypeError('%s is not a valid number.' % value)


def check_ssl_file_permission(keyfile, certfile):
    if not keyfile or not certfile:
        return
    ssl_keyfile_permission_invalid = (os.stat(keyfile).st_mode & 0o777) > 0o600
    ssl_certfile_permission_invalid = (os.stat(certfile).st_mode & 0o777) > 0o600
    if ssl_keyfile_permission_invalid:
        result_msg = "WARNING:the ssl keyfile permission greater then 600"
        write_to_terminal(result_msg, color="yellow")
    if ssl_certfile_permission_invalid:
        result_msg = "WARNING:the ssl certfile permission greater then 600"
        write_to_terminal(result_msg, color="yellow")


def check_ssl_certificate_remaining_days(certificate_path, certificate_warn_threshold=90):
    """
    Check whether the certificate is expired or invalid.
    :param certificate_path: path of certificate.
    :certificate_warn_threshold: the warning days for certificate_remaining_days
    output: dict, check result which include 'check status' and 'check information'.
    """
    if not certificate_path:
        return
    gmt_format = '%b %d %H:%M:%S %Y GMT'
    child = subprocess.Popen(['openssl', 'x509', '-in', certificate_path, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[0]:
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = datetime.strptime(not_after, gmt_format)
        certificate_remaining_days = (end_time - datetime.now()).days
        if 0 < certificate_remaining_days < certificate_warn_threshold:
            result_msg = "WARNING: the '{certificate}' has remaining " \
                         "{certificate_remaining_days} days before out of date." \
                .format(certificate=certificate_path,
                        certificate_remaining_days=certificate_remaining_days)
            write_to_terminal(result_msg, color="yellow")
        elif certificate_remaining_days <= 0:
            result_msg = "WARNING: the '{certificate}' is out of date."\
                .format(certificate=certificate_path)
            write_to_terminal(result_msg, color="yellow")


def dbmind_assert(condition, comment=None):
    if not condition:
        if comment is None:
            raise AssertionError("Please check the value of this variable.")
        else:
            raise ValueError(comment)

