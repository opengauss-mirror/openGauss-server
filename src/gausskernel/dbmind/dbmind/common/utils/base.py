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
import logging
import multiprocessing
import sys
import os
import threading
import time
import traceback
import types
from collections import OrderedDict
from functools import wraps, lru_cache
from logging.handlers import RotatingFileHandler
from queue import Empty

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


def ttl_cache(seconds, maxsize=32):
    """Time aware caching to LRU cache."""

    def decorator(func):
        @lru_cache(maxsize)
        def _new(*args, __time_salt, **kwargs):
            return func(*args, **kwargs)

        @wraps(func)
        def _wrapped(*args, **kwargs):
            return _new(*args, __time_salt=round(time.monotonic() / seconds), **kwargs)

        return _wrapped

    return decorator


class TTLOrderedDict(OrderedDict):
    def __init__(self, ttl_seconds=600, *args, **kwargs):
        self.ttl = ttl_seconds
        self._lock = threading.RLock()
        super().__init__()
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        with self._lock:
            expired_time = time.monotonic() + self.ttl
            super().__setitem__(key, (expired_time, value))

    def __getitem__(self, key):
        with self._lock:
            expired_time, value = super().__getitem__(key)
            if expired_time < time.monotonic():
                super().__delitem__(key)
                raise KeyError(key)
            return value

    def refresh_ttl(self, key):
        with self._lock:
            self.__setitem__(key, self.__getitem__(key))

    def __delitem__(self, key):
        with self._lock:
            super().__delitem__(key)

    def _purge(self):
        to_delete = []
        for key, item in super().items():
            expired_time, value = item
            if expired_time < time.monotonic():
                to_delete.append(key)
        for key in to_delete:
            super().__delitem__(key)

    def __len__(self):
        with self._lock:
            self._purge()
            return super().__len__()

    def __iter__(self):
        with self._lock:
            self._purge()
            return self.__iter__()

    def __contains__(self, key):
        with self._lock:
            self._purge()
            return super().__contains__(key)

    def __repr__(self):
        with self._lock:
            self._purge()
            return super().__repr__()

    def keys(self):
        with self._lock:
            self._purge()
            return super().keys()

    def values(self):
        with self._lock:
            self._purge()
            return super().values()

    def get(self, k, default=None):
        with self._lock:
            try:
                return self.__getitem__(k)
            except KeyError:
                return default


class NaiveQueue:
    def __init__(self, maxsize=10):
        """Unlike the Queue that Python builds in,
        if the length exceeds the max size, the first inserted element
        will be popped and will not be blocked. """
        self.q = list()
        dbmind_assert(maxsize > 0)
        self.maxsize = maxsize
        self.mutex = threading.Lock()

    def put(self, e):
        with self.mutex:
            self.q.append(e)
            while len(self.q) > self.maxsize:
                self.q.pop(0)

    def get(self, default=None):
        with self.mutex:
            if len(self.q) > 0:
                return self.q.pop(0)
            return default

    def __iter__(self):
        with self.mutex:
            return self.q.__iter__()

    def __len__(self):
        with self.mutex:
            return len(self.q)


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


class MultiProcessingRFHandler(RotatingFileHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._queue = multiprocessing.Queue(-1)
        self._sensitive_words = []
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
            except Exception as e:
                dbmind_assert(e)  # ignore
                traceback.print_exc(file=sys.stderr)
        self._queue.close()
        self._queue.join_thread()

    def _send(self, s):
        self._queue.put_nowait(s)

    def add_sensitive_word(self, word):
        """Prevent sensitive information from leaking
        in plaintext through logs, such as passwords."""
        self._sensitive_words.append(word)

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

    def format(self, record):
        s = super().format(record)
        for word in self._sensitive_words:
            s = s.replace(word, '******')
        return s

    def close(self):
        if not self._should_exit:
            self._should_exit = True
            self._receiv_thr.join(5)
            super().close()


class ExceptionCatcher:
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
                elif self.strategy == 'raise':
                    raise e
                elif self.strategy == 'ignore':
                    pass
                elif self.strategy == 'sensitive':
                    SENSITIVE_WORD = (
                        'PASSWORD', 'PWD', 'AUTH', 'USERNAME', 'USER', 'CERTIFICATE', 'SSL'
                    )
                    error = str(e).upper()
                    for word in SENSITIVE_WORD:
                        if word in error:
                            raise AssertionError(
                                "Involves sensitive information, details are ignored, "
                                "please check the call stack."
                            ).with_traceback(sys.exc_info()[2]) from None
                    raise e
                elif self.strategy == 'exit':
                    from .cli import raise_fatal_and_exit
                    raise_fatal_and_exit('An exception raised: %s' % e)
                else:
                    raise ValueError('Not support strategy %s' % self.strategy)

        return wrapper


ignore_exc = ExceptionCatcher(strategy='ignore', name='common utils')


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


def is_integer_string(s):
    if s.isdigit():
        return True
    try:
        int(s)
        return True
    except ValueError:
        pass
    return False


def cast_to_int_or_float(value):
    if isinstance(value, (int, float)):
        return value
    try:
        if isinstance(value, str) and is_integer_string(value):
            return int(value)
        return float(value)
    except (ValueError, TypeError):
        logging.warning('The value: %s cannot be converted to int or float.', value, exc_info=True)
        return float('nan')


def dbmind_assert(condition, comment=None):
    if not condition:
        if comment is None:
            raise AssertionError("Please check the value of this variable. "
                                 "The value of condition is %s." % condition)
        else:
            raise ValueError(comment)


def chmod_r(path, directory_mode=0o700, file_mode=0o600):
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path):
        for d in dirs:
            os.chmod(os.path.join(root, d), directory_mode)
        for f in files:
            os.chmod(os.path.join(root, f), file_mode)
    os.chmod(path, directory_mode)

