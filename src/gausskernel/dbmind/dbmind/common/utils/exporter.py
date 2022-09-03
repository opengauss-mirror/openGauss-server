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
import argparse
import getpass
import os
import socket
from logging.handlers import TimedRotatingFileHandler

import requests

from .cli import write_to_terminal


def is_exporter_alive(host, port):
    return is_port_used(host, port)


def is_port_used(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if host.strip() == '0.0.0.0':
        try:
            s.bind(('0.0.0.0', port))
            return False
        except socket.error as e:
            if 'Address already in use' in str(e):
                return True
            else:
                return False

        finally:
            s.close()
    try:
        resp = s.connect_ex((host, port))
        if resp == 0:
            return True
        else:
            return False
    except socket.error:
        return False
    finally:
        s.close()


def can_access_the_url(url):
    try:
        response = requests.get(
            url,
            headers={"Content-Type": "application/json"},
        )
        if response.status_code == 200:
            return True
        return False

    except requests.exceptions.ConnectionError:
        return False

    except Exception as e:
        logging.exception(e)
        return False


def is_prometheus_alive(host, port):
    if not is_port_used(host, port):
        return False, None

    for scheme in ('http', 'https'):
        url = "{}://{}:{}/api/v1/query?query=up".format(scheme, host, port)
        if can_access_the_url(url):
            return True, scheme
    return False, None


def exporter_ssl_logic(parser, args):
    ssl_keyfile_pwd = None
    if args.disable_https:
        # Clear up redundant arguments.
        args.ssl_keyfile = None
        args.ssl_certfile = None
    else:
        if not (args.ssl_keyfile and args.ssl_certfile):
            parser.error('If you use the Https protocol (default), you need to give the argument values '
                         'of --ssl-keyfile and --ssl-certfile. '
                         'Otherwise, use the --disable-https argument to disable the Https protocol.')
        else:
            # Need to check whether the key file has been encrypted.
            with open(args.ssl_keyfile) as fp:
                for line in fp.readlines():
                    if line.startswith('Proc-Type') and 'ENCRYPTED' in line.upper():
                        ssl_keyfile_pwd = ''
                        while not ssl_keyfile_pwd:
                            ssl_keyfile_pwd = getpass.getpass('Enter PEM pass phrase:')
    setattr(args, 'keyfile_password', ssl_keyfile_pwd)
    return args


class KVPairAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        d = dict()
        try:
            for pair in values.split(','):
                name, value = pair.split('=')
                d[name.strip()] = value.strip()
            setattr(args, self.dest, d)
        except ValueError:
            parser.error('Illegal constant labels: %s.' % values)


class ListPairAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        items = list()
        try:
            for item in values.split(','):
                if item.count('=') == 1:
                    name, value = item.split('=')
                    items.append(value.strip())
                else:
                    items.append(item.strip())
            setattr(args, self.dest, items)
        except ValueError:
            parser.error('Illegal value: %s.' % values)


def warn_logging_and_terminal(message):
    logging.warning(message)
    write_to_terminal(message, level='error')


def set_logger(filepath, level):
    level = level.upper()
    log_path = os.path.dirname(filepath)
    if not os.path.exists(log_path):
        os.makedirs(log_path, 0o700)

    formatter = logging.Formatter(
        '[%(asctime)s]'
        '[%(filename)s:%(lineno)d]'
        '[%(funcName)s][%(levelname)s][%(thread)d] '
        '- %(message)s'
    )
    handler = TimedRotatingFileHandler(
        filename=filepath,
        when='D',
        interval=1,
        backupCount=15,
        encoding='UTF-8',
        delay=False,
        utc=True
    )
    handler.setFormatter(formatter)
    handler.setLevel(level)
    default_logger = logging.getLogger()
    default_logger.setLevel(level)
    default_logger.addHandler(handler)


def parse_and_adjust_args(parser, argv):
    args = parser.parse_args(argv)

    ssl_keyfile_pwd = None
    if args.disable_https:
        # Clear up redundant arguments.
        args.ssl_keyfile = None
        args.ssl_certfile = None
    else:
        if not (args.ssl_keyfile and args.ssl_certfile):
            parser.error('If you use the Https protocol (default), you need to give the argument values '
                         'of --ssl-keyfile and --ssl-certfile. '
                         'Otherwise, use the --disable-https argument to disable the Https protocol.')
        else:
            # Need to check whether the key file has been encrypted.
            with open(args.ssl_keyfile) as fp:
                for line in fp.readlines():
                    if line.startswith('Proc-Type') and 'ENCRYPTED' in line.upper():
                        ssl_keyfile_pwd = ''
                        while not ssl_keyfile_pwd:
                            ssl_keyfile_pwd = getpass.getpass('Enter PEM pass phrase:')
    setattr(args, 'keyfile_password', ssl_keyfile_pwd)
    return args

