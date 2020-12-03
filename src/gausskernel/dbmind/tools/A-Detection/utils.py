import inspect
import logging
import pydoc
import re
import subprocess
from datetime import datetime, timedelta
from logging import handlers
from threading import Thread, Event

import dateutil.parser


class RepeatTimer(Thread):
    def __init__(self, interval, function, args=None, kwargs=None):
        Thread.__init__(self)
        self.interval = interval
        self.function = function
        self.args = args if args is not None else []
        self.kwargs = kwargs if kwargs is not None else {}
        self.finished = Event()

    def cancel(self):
        self.finished.set()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self.interval)
            self.function(*self.args, **self.kwargs)
        self.finished.set()


def transform_time_string(time_str, mode='timedelta'):
    """
    only support 'weeks, days, hours, minutes, seconds
    W: week, D: days, H: hours, M: minutes, S: seconds
    """
    if mode not in ('timedelta', 'to_second'):
        raise ValueError('wrong mode {mode} in time_transfer.'.format(mode=mode))

    time_num, time_flag = re.match(r'(\d+)?([WDHMS])', time_str).groups()

    if time_flag is None:
        raise ValueError('wrong format {time_str} for time_str in time_transfer.'.format(time_str=time_str))

    if time_num is None:
        time_num = 1
    else:
        time_num = int(time_num)

    timedelta_mapper = {'W': timedelta(weeks=1),
                        'D': timedelta(days=1),
                        'H': timedelta(hours=1),
                        'M': timedelta(minutes=1),
                        'S': timedelta(seconds=1)}

    second_mapper = {'W': 7 * 24 * 3600, 'D': 24 * 3600, 'H': 3600, 'M': 60, 'S': 1}

    if mode == 'timedelta':
        return timedelta_mapper.get(time_flag) * time_num
    if mode == 'to_second':
        return second_mapper.get(time_flag) * time_num


def detection_logger(log_name, log_path, level):
    logger = logging.getLogger(log_name)
    agent_handler = handlers.RotatingFileHandler(filename=log_path,
                                                 maxBytes=1024 * 1024 * 100,
                                                 backupCount=5)
    agent_handler.setFormatter(logging.Formatter("[%(asctime)s %(levelname)s]-[%(name)s]: %(message)s"))
    logger.addHandler(agent_handler)
    logger.setLevel(getattr(logging, level.upper()) if hasattr(logging, level.upper()) else logging.INFO)
    return logger


def unify_byte_unit(byte_info):
    """
    func: transfer unit of K、M、G、T、P to M
    """
    byte_info = byte_info.upper()
    bytes_num, bytes_unit = re.match(r'^(\d+|\d+\.\d+)([KMGTP])', byte_info).groups()
    if bytes_num is None or bytes_unit is None or bytes_unit not in 'KMGTP':
        raise ValueError('can not parse format of {bytes}'.format(bytes=byte_info))
    byte_unit_mapper = {'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024, 'P': 1024 * 1024 * 1024}
    return byte_unit_mapper[bytes_unit] * int(float(bytes_num))


def get_funcs(thing):
    """
    return functions in python file
    """
    funcs = []
    _object, _ = pydoc.resolve(thing)
    _all = getattr(_object, '__all__', None)
    for key, value in inspect.getmembers(_object, inspect.isroutine):
        if _all is not None or inspect.isbuiltin(value) or inspect.getmodule(value) is _object:
            if pydoc.visiblename(key, _all, _object):
                funcs.append((key, value))
    return funcs


def check_certificate(certificate_path):
    """
    check certificate validity
    """
    check_result = {}
    certificate_waring_threshold = 365
    child = subprocess.Popen(['openssl', 'x509', '-in', certificate_path, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[1] or not sub_chan[0]:
        check_result['status'] = 'fail'
    else:
        check_result['status'] = 'success'
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = dateutil.parser.parse(not_after).replace(tzinfo=None)
        certificate_remaining_days = (end_time - datetime.now()).days
        if 0 < certificate_remaining_days < certificate_waring_threshold:
            check_result['level'] = 'warn'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                                   .format(certificate=certificate_path,
                                           certificate_remaining_days=certificate_remaining_days)
        elif certificate_remaining_days >= certificate_waring_threshold:
            check_result['level'] = 'info'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                                   .format(certificate=certificate_path,
                                           certificate_remaining_days=certificate_remaining_days)
        else:
            check_result['level'] = 'error'
            check_result['info'] = "the '{certificate}' is out of date." \
                                   .format(certificate=certificate_path)
    return check_result

