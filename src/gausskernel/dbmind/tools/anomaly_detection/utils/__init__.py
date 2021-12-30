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
import datetime
import logging
import os
import re
import sys
import shlex
import ctypes
import subprocess
import requests
import config
from datetime import timedelta
from threading import Thread, Event
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import hashlib

import psycopg2
import sqlparse
from sqlparse.sql import Identifier, IdentifierList
from sqlparse.tokens import Keyword, DML

from global_vars import DATE_FORMAT

split_flag = ('!=', '<=', '>=', '==', '<', '>', '=', ',', '*', ';', '%', '+', ',', ';', '/')


class AesCbcUtil(object):
    """
    aes  cbc tool
    """
    @classmethod
    def check_content_key(cls, content, key):
        """
        check ase cbc content and key
        """
        if not isinstance(content, bytes):
            raise ValueError('incorrect parameter.')
        if not isinstance(key, (bytes, str)):
            raise ValueError('incorrect parameter.')

        iv_len = 16
        if not len(content) >= (iv_len + 16):
            raise Exception(Errors.GAUSS_61101.build_msg("check content key. "
                            "content's len must >= (iv_len + 16)."))

    @classmethod
    def aes_cbc_decrypt(cls, content, key):
        """
        aes cbc decrypt for content and key
        """
        cls.check_content_key(content, key)
        if isinstance(key, str):
            key = bytes(key)
        iv_len = 16
        # pre shared key iv
        iv = content[16 + 1 + 16 + 1:16 + 1 + 16 + 1 + 16]

        # pre shared key  enctryt
        enc_content = content[:iv_len]

        try:
            backend = default_backend()
        except Exception as imp_clib_err:
            if str(imp_clib_err).find('SSLv3_method') == -1:
                # not find SSLv3_method, and it's not ours
                local_path = os.path.dirname(os.path.realpath(__file__))
                clib_path = os.path.realpath(os.path.join(local_path, "../clib"))
                ssl_path = os.path.join(clib_path, 'libssl.so.1.1')
                crypto_path = os.path.join(clib_path, 'libcrypto.so.1.1')
                if os.path.isfile(crypto_path):
                    ctypes.CDLL(crypto_path, mode=ctypes.RTLD_GLOBAL)
                if os.path.isfile(ssl_path):
                    ctypes.CDLL(ssl_path, mode=ctypes.RTLD_GLOBAL)
            else:
                ssl_path = '/usr/lib64/libssl.so.1.1'
                crypto_path = '/usr/lib64/libcrypto.so.1.1'
                if os.path.isfile(crypto_path):
                    ctypes.CDLL(crypto_path, mode=ctypes.RTLD_GLOBAL)
                if os.path.isfile(ssl_path):
                    ctypes.CDLL(ssl_path, mode=ctypes.RTLD_GLOBAL)
            backend = default_backend()

        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
        decrypter = cipher.decryptor()
        dec_content = decrypter.update(enc_content) + decrypter.finalize()
        server_decipher_key = dec_content.rstrip(b'\x00')[:-1].decode()
        return server_decipher_key

    @classmethod
    def get_old_version_path(cls, path):
        """ Compatible old version path, only 'encrypt'
            old: /home/xxx/key_0
            new: /home/xxx/cipher/key_0
        """
        dirname, basename = os.path.split(path.rstrip("/"))
        dirname, _ = os.path.split(dirname)
        path = os.path.join(dirname, basename)
        return path

    @classmethod
    def aes_cbc_decrypt_with_path(cls, cipher_path, rand_path):
        """
        aes cbc decrypt for one path
        """
        if not os.path.isdir(cipher_path):
            cipher_path = cls.get_old_version_path(cipher_path)
            rand_path = cls.get_old_version_path(rand_path)
        with open(os.path.join(cipher_path, 'server.key.cipher'), 'rb') as cipher_file:
            cipher_txt = cipher_file.read()
        with open(os.path.join(rand_path, 'server.key.rand'), 'rb') as rand_file:
            rand_txt = rand_file.read()

        if cipher_txt is None or cipher_txt == "":
            return None

        server_vector_cipher_vector = cipher_txt[16 + 1:16 + 1 + 16]
        # pre shared key rand
        server_key_rand = rand_txt[:16]

        # worker key
        server_decrypt_key = hashlib.pbkdf2_hmac('sha256', server_key_rand,
                                                 server_vector_cipher_vector, 10000, 16)

        enc = cls.aes_cbc_decrypt(cipher_txt, server_decrypt_key)
        return enc

    @classmethod
    def aes_cbc_decrypt_with_multi(cls, cipher_root, rand_root):
        """
        decrypt message with multi depth
        """
        num = 0
        rt = ""
        if not os.path.isdir(cipher_root):
            cipher_root = os.path.dirname(cipher_root.rstrip("/"))
            rand_root = os.path.dirname(rand_root.rstrip("/"))
        while True:
            cipher_path = os.path.join(cipher_root, "key_%s" % num)
            rand_path = os.path.join(rand_root, "key_%s" % num)
            part = cls.aes_cbc_decrypt_with_path(cipher_path, rand_path)
            if part is None:
                break
            elif len(part) < 15:
                rt += part
                break
            else:
                rt += part
            num = num + 1

        if rt == "":
            return None
        return rt

    @staticmethod
    def format_path(root_path):
        """format decrypt_with_multi or decrypt_with_path"""
        return os.path.join(root_path, "cipher"), os.path.join(root_path, "rand")


class RepeatTimer(Thread):
    """
    This class inherits from threading.Thread, it is used for periodic execution
    function at a specified time interval.
    """

    def __init__(self, interval, function, *args, **kwargs):
        Thread.__init__(self)
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._finished = Event()

    def run(self):
        while not self._finished.is_set():
            # Execute first, wait later.
            self._function(*self._args, **self._kwargs)
            self._finished.wait(self._interval)
        self._finished.set()

    def cancel(self):
        self._finished.set()


class StdStreamSuppressor:
    """
    This class suppress standard stream object 'stdout' and 'stderr' in context.
    """

    def __init__(self):
        self.default_stdout_fd = sys.stdout.fileno()
        self.default_stderr_fd = sys.stderr.fileno()
        self.null_device_fd = [os.open(os.devnull, os.O_WRONLY), os.open(os.devnull, os.O_WRONLY)]
        self.standard_stream_fd = (os.dup(self.default_stdout_fd), os.dup(self.default_stderr_fd))

    def __enter__(self):
        os.dup2(self.null_device_fd[0], self.default_stdout_fd)
        os.dup2(self.null_device_fd[1], self.default_stderr_fd)

    def __exit__(self, *args):
        os.dup2(self.standard_stream_fd[0], self.default_stdout_fd)
        os.dup2(self.standard_stream_fd[1], self.default_stderr_fd)
        os.close(self.null_device_fd[0])
        os.close(self.null_device_fd[1])


class TimeString:
    TIMEDELTA_MAPPER = {'W': timedelta(weeks=1),
                        'D': timedelta(days=1),
                        'H': timedelta(hours=1),
                        'M': timedelta(minutes=1),
                        'S': timedelta(seconds=1)}
    SECOND_MAPPER = {'W': 7 * 24 * 3600, 'D': 24 * 3600, 'H': 3600, 'M': 60, 'S': 1}

    def __init__(self, time_string):
        """
        Transform time string to timedelta or second, only support 'weeks(W), days(D),
        hours(H), minutes(M), seconds(S)
        :param time_string: string,  time string like '10S', '20H', '3W'.
        """
        self._str = time_string
        num, self._unit = re.match(r'(\d+)?([WDHMS])', time_string).groups()

        if self._unit is None:
            raise ValueError('Incorrect format %s.' % time_string)
        if num is None:
            self._val = 1
        else:
            self._val = int(num)

    def to_second(self):
        return TimeString.SECOND_MAPPER.get(self._unit) * self._val

    def to_timedelta(self):
        return TimeString.TIMEDELTA_MAPPER.get(self._unit) * self._val

    @property
    def standard(self):
        return '%dS' % self.to_second()


class DBAgent:
    def __init__(self, port, host=None, user=None, password=None, database=None):
        self.host = host
        self.port = port
        self.user = user
        self.database = database
        self.password = password
        self.conn = None
        self.cursor = None
        self.connect()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        self.conn = psycopg2.connect(host=self.host,
                                     user=self.user,
                                     password=self.password,
                                     database=self.database,
                                     port=self.port)
        self.conn.set_client_encoding('latin9')
        self.cursor = self.conn.cursor()

    def fetch_all_result(self, sql):
        try:
            self.cursor.execute(sql)
            result = list(self.cursor.fetchall())
            return result
        except Exception as e:
            logging.getLogger('agent').warning(str(e))

    def close(self):
        self.cursor.close()
        self.conn.close()


def remove_comment(sql):
    sql = re.sub(r'\n', r' ', sql)
    sql = re.sub(r'/\s*\*[\w\W]*?\*\s*/\s*', r'', sql)
    sql = re.sub(r'^--.*\s?', r'', sql)
    return sql


def unify_sql(sql):
    index = 0
    sql = remove_comment(sql)
    while index < len(sql):
        if sql[index] in split_flag:
            if sql[index:index + 2] in split_flag:
                sql = sql[:index].strip() + ' ' + sql[index:index + 2] + ' ' + sql[index + 2:].strip()
                index = index + 3
            else:
                sql = sql[:index].strip() + ' ' + sql[index] + ' ' + sql[index + 1:].strip()
                index = index + 2
        else:
            index = index + 1
    new_sql = list()
    for word in sql.split():
        new_sql.append(word.upper())
    sql = ' '.join(new_sql)
    return sql.strip()


def input_sql_processing(sql):
    """
    SQL desensitization
    """
    if not sql:
        return ''
    standard_sql = unify_sql(sql)

    if standard_sql.startswith('INSERT'):
        standard_sql = re.sub(r'VALUES (\(.*\))', r'VALUES', standard_sql)
    # remove digital like 12, 12.565
    standard_sql = re.sub(r'[\s]+\d+(\.\d+)?', r' ?', standard_sql)
    # remove '$n' in sql
    standard_sql = re.sub(r'\$\d+', r'?', standard_sql)
    # remove single quotes content
    standard_sql = re.sub(r'\'.*?\'', r'?', standard_sql)
    # remove double quotes content
    standard_sql = re.sub(r'".*?"', r'?', standard_sql)
    # remove '(1' format
    standard_sql = re.sub(r'\(\d+(\.\d+)?', r'(?', standard_sql)
    # remove '`' in sql
    standard_sql = re.sub(r'`', r'', standard_sql)
    # remove ; in sql
    standard_sql = re.sub(r';', r'', standard_sql)

    return standard_sql.strip()


def wdr_sql_processing(sql):
    standard_sql = unify_sql(sql)
    standard_sql = re.sub(r';', r'', standard_sql)
    standard_sql = re.sub(r'VALUES (\(.*\))', r'VALUES', standard_sql)
    standard_sql = re.sub(r'\$\d+?', r'?', standard_sql)
    return standard_sql


def convert_to_mb(volume_str):
    """
    Transfer unit of K、M、G、T、P to M
    :param volume_str: string, byte information like '100M', '2K', '30G'.
    :return: int, bytes size in unit of M, like '400M' -> 400.
    """
    convtbl = {'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024, 'P': 1024 * 1024 * 1024}

    volume_str = volume_str.upper()
    num, unit = re.match(r'^(\d+|\d+\.\d+)([KMGTP])', volume_str).groups()
    if (num is None) or (unit is None) or (unit not in 'KMGTP'):
        raise ValueError('cannot parse format of {bytes}'.format(bytes=volume_str))
    return convtbl[unit] * int(float(num))


def fatal_exit(msg=None):
    if msg:
        print("FATAL: %s." % msg, file=sys.stderr)
    logging.getLogger('service').fatal("A fatal problem has occurred, and the process will exit.")
    raise SystemExit(2)


def abnormal_exit(msg=None):
    if msg:
        print("ERROR: %s." % msg, file=sys.stderr)
    logging.getLogger('service').fatal("An abnormal has occurred, and the process will exit.")
    raise SystemExit(1)


def check_select(parsed_sql):
    if not parsed_sql.is_group:
        return False
    for token in parsed_sql.tokens:
        if token.ttype is DML and token.value.upper() == 'SELECT':
            return True
    return False


def get_table_token_list(parsed_sql, token_list):
    flag = False
    for token in parsed_sql.tokens:
        if not flag:
            if token.ttype is Keyword and token.value.upper() == 'FROM':
                flag = True
        else:
            if check_select(token):
                get_table_token_list(token, token_list)
            elif token.ttype is Keyword:
                return
            else:
                token_list.append(token)


def extract_table_from_select(sql):
    tables = []
    table_token_list = []
    sql_parsed = sqlparse.parse(sql)[0]
    get_table_token_list(sql_parsed, table_token_list)
    for table_token in table_token_list:
        if isinstance(table_token, Identifier):
            tables.append(table_token.get_name())
        elif isinstance(table_token, IdentifierList):
            for identifier in table_token.get_identifiers():
                tables.append(identifier.get_name())
        else:
            if table_token.ttype is Keyword:
                tables.append(table_token.value)
    return tables


def extract_table_from_sql(sql):
    """
    Function: get table name in sql
    has many problems in code, especially in 'delete', 'update', 'insert into' sql
    """
    if not sql.strip():
        return []
    delete_pattern_1 = re.compile(r'FROM\s+([^\s]*)[;\s ]?', re.IGNORECASE)
    delete_pattern_2 = re.compile(r'FROM\s+([^\s]*)\s+WHERE', re.IGNORECASE)
    update_pattern = re.compile(r'UPDATE\s+([^\s]*)\s+SET', re.IGNORECASE)
    insert_pattern = re.compile(r'INSERT\s+INTO\s+([^\s]*)\s+VALUES', re.IGNORECASE)
    if sql.upper().strip().startswith('SELECT'):
        tables = extract_table_from_select(sql)
    elif sql.upper().strip().startswith('DELETE'):
        if 'WHERE' not in sql:
            tables = delete_pattern_1.findall(sql)
        else:
            tables = delete_pattern_2.findall(sql)
    elif sql.upper().strip().startswith('UPDATE'):
        tables = update_pattern.findall(sql)
    elif sql.upper().strip().startswith('INSERT INTO'):
        sql = re.sub(r'\(.*?\)', r' ', sql)
        tables = insert_pattern.findall(sql)
    else:
        tables = []
    return tables


def check_time_legality(time_string):
    try:
        datetime.datetime.strptime(time_string, DATE_FORMAT)
        return True
    except ValueError:
        return False


def check_port_occupancy(port):
    if not port.isdigit():
       raise RuntimeError("The port should be digit: '{port}'".format(port=port))
    child = subprocess.Popen(shlex.split('lsof -i:{port}'.format(port=port)), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    stream = child.communicate()
    if stream[0]:
       raise RuntimeError("The port {port} is occupied.".format(port=port))


def read_pid_file(filepath):
    """
    Return the pid of the running process recorded in the file,
    and return 0 if the acquisition fails.
    """
    if not os.path.exists(filepath):
        return 0

    try:
        with open(filepath, mode='r') as f:
            pid = int(f.read())
        if os.path.exists('/proc/%d' % pid):
            return pid
        else:
            return 0
    except PermissionError:
        return 0
    except ValueError:
        return 0


def check_db_alive(port, database='postgres'):
    try:
        with DBAgent(port=port, database=database) as db:
            sql = "select pg_sleep(0.1)"
            result = db.fetch_all_result(sql)
        return True
    except Exception as e:
        return False


def check_collector():
    agent_logger = logging.getLogger('agent')

    try:
        req_url = 'http://{host}:{port}/sink'.format(host=config.get('server', 'host'), port=config.get('server', 'listen_port'))
        response = requests.get(req_url)
        return True
    except Exception as e:
        agent_logger.error("{error}".format(error=str(e)))
        return False


def check_tls_protocol():
    try:
        context = config.getboolean('security', 'tls')
        if context:
            protocol = 'https'
        else:
            protocol = 'http'
        return protocol

    except Exception as e:
        agent_logger.error("[security] part must exists in configure file.")
        raise 


def getpasswd(key_path):
    if os.path.isdir(key_path):
        output = AesCbcUtil.aes_cbc_decrypt_with_multi(*AesCbcUtil.format_path(key_path))
        if len(str(output).strip().split()) < 1:
            return ''
        else:
            return str(output).strip().split()[-1]
    else:
        return ''

