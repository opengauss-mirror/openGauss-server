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
import argparse
import datetime
import os
import re
import subprocess
import time

import paramiko

from dbmind.common.utils import write_to_terminal
from .base import ignore_exc


def check_path_valid(path):
    char_black_list = (' ', '|', ';', '&', '$', '<', '>', '`', '\\',
                       '\'', '"', '{', '}', '(', ')', '[', ']', '~',
                       '*', '?', '!', '\n')

    if path.strip() == '':
        return True

    for char in char_black_list:
        if path.find(char) >= 0:
            return False

    return True


def check_ip_valid(value):
    ip_pattern = re.compile(
        r'^(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.'
        '(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.'
        '(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.'
        '(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)$'
    )
    if ip_pattern.match(value):
        return True
    return value == '0.0.0.0'


def check_port_valid(value):
    if isinstance(value, str):
        return str.isdigit(value) and 1023 < int(value) <= 65535
    elif isinstance(value, int):
        return 1023 < value <= 65535
    else:
        return False


def is_more_permissive(filepath, max_permissions=0o600):
    return (os.stat(filepath).st_mode & 0o777) > max_permissions


def check_ssl_file_permission(certfile, keyfile):
    if keyfile and is_more_permissive(keyfile, 0o600):
        result_msg = "WARNING: the permission of ssl key file %s is greater then 600." % keyfile
        write_to_terminal(result_msg, color="yellow")
    if certfile and is_more_permissive(certfile, 0o600):
        result_msg = "WARNING: the permission of ssl certificate file %s is greater then 600." % certfile
        write_to_terminal(result_msg, color="yellow")


@ignore_exc
def check_ssl_certificate_remaining_days(certfile, expired_threshold=90):
    """
    Check whether the certificate is expired or invalid.
    :param expired_threshold: how many days to warn.
    :param certfile: path of certificate.
    :certificate_warn_threshold: the warning days for certificate_remaining_days
    output: dict, check result which include 'check status' and 'check information'.
    """
    if not certfile:
        return
    gmt_format = '%b %d %H:%M:%S %Y GMT'
    child = subprocess.Popen(['openssl', 'x509', '-in', certfile, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[0]:
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = datetime.datetime.strptime(not_after, gmt_format)
        certificate_remaining_days = (end_time - datetime.datetime.now()).days
        if 0 < certificate_remaining_days < expired_threshold:
            result_msg = "WARNING: the certificate '{certificate}' has the remaining " \
                         "{certificate_remaining_days} days before out of date." \
                .format(certificate=certfile,
                        certificate_remaining_days=certificate_remaining_days)
            write_to_terminal(result_msg, color="yellow")
        elif certificate_remaining_days <= 0:
            result_msg = "WARNING: the certificate '{certificate}' is out of date." \
                .format(certificate=certfile)
            write_to_terminal(result_msg, color="yellow")


def warn_ssl_certificate(certfile, keyfile):
    check_ssl_file_permission(certfile, keyfile)
    check_ssl_certificate_remaining_days(certfile)


# The following utils are checking for command line arguments.
class CheckDSN(argparse.Action):
    @staticmethod
    def is_identifier_correct(url: str):
        # A correct DSN url is similar to:
        # postgres://username:pwd@host:port/dbname
        # Hence, we can limit the number of identifiers to prevent bad url.
        identifiers = {
            '@': 1,
            ':': 3,
            '/': 3
        }
        for ident, limit in identifiers.items():
            if url.count(ident) > limit:
                return False, "Incorrect URL because you haven't encoded the identifier %s."
        return True, None

    def __call__(self, parser, args, values, option_string=None):
        if values.startswith('postgres'):
            correct, msg = self.is_identifier_correct(values)
            if not correct:
                parser.error(msg)

        import psycopg2.extensions
        try:
            psycopg2.extensions.parse_dsn(values)
        except psycopg2.ProgrammingError:
            parser.error('This URL is an invalid dsn.')

        setattr(args, self.dest, values)


class CheckPort(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_port_valid(values):
            parser.error('Illegal port value(1024~65535): %s.' % values)
        setattr(args, self.dest, values)


class CheckIP(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        if not check_ip_valid(values):
            parser.error('Illegal IP: %s.' % values)
        setattr(args, self.dest, values)


class CheckWordValid(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        ill_character = [" ", "|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                         "{", "}", "(", ")", "[", "]", "~", "*", "?", "!", "\n"]
        if not values.strip():
            return
        if any(ill_char in values for ill_char in ill_character):
            parser.error('There are illegal characters in your input.')
        setattr(namespace, self.dest, values)


def path_type(path):
    realpath = os.path.realpath(path)
    if os.path.exists(realpath):
        return realpath
    raise argparse.ArgumentTypeError('%s is not a valid path.' % path)


def http_scheme_type(param):
    param = param.lower()
    if param in ('http', 'https'):
        return param
    raise argparse.ArgumentTypeError('%s is not valid.' % param)


def positive_int_type(integer: str):
    if not integer.isdigit():
        raise argparse.ArgumentTypeError('Invalid value %s.' % integer)

    integer = int(integer)
    if integer == 0:
        raise argparse.ArgumentTypeError('Invalid value 0.')

    return integer


def date_type(date_str):
    """Cast date or timestamp string to
    a 13-bit timestamp integer."""
    if date_str.isdigit():
        # We can't know whether the timestamp users give is valid.
        # So, we only cast the string to integer and return it.
        return int(date_str)

    # If the date string users give is not a timestamp string, we
    # will regard it as the date time format.
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return int(d.timestamp() * 1000)  # unit: ms
    except ValueError:
        pass

    raise argparse.ArgumentTypeError('Invalid value %s.' % date_str)


def check_ssh_version(minimum_version=(2, 0)):
    version = getattr(paramiko.Transport, '_PROTO_ID')
    if tuple(map(int, version.split('.'))) < minimum_version:
        raise ValueError('The ssh version is lower than v2.x.')
    else:
        write_to_terminal('The ssh version {} is qualified.'.format(version), color='red')


def check_datetime_legality(time_string):
    try:
        datetime.datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False
