import csv
import datetime
import os
import sys
import stat
from collections import OrderedDict

import pandas as pd
import psycopg2


class ResultSaver:
    """
    This class is used for saving result, and now support 'list', 'dict', 'tuple'.
    """

    def __init__(self):
        pass

    def save(self, data, path):
        realpath = os.path.realpath(path)
        dirname = os.path.dirname(realpath)
        if not os.path.exists(dirname):
            os.makedirs(dirname, mode=0o700)
        if oct(os.stat(dirname).st_mode)[-3:] != '700':
            os.chmod(dirname, stat.S_IRWXU)
        if isinstance(data, (list, tuple)):
            self.save_list(data, realpath)
        else:
            raise TypeError("mode should be 'list', 'tuple' or 'dict', but input type is '{}'".format(str(type(data))))

    @staticmethod
    def save_list(data, path):
        with open(path, mode='w') as f:
            for item in data:
                content = ",".join([str(sub_item) for sub_item in item])
                f.write(content + '\n') 


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
                                     passwd=self.password,
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
            logging.warning(str(e))

    def close(self):
        self.cursor.close()
        self.conn.close()


class LRUCache:
    """
    LRU algorithm based on ordered dictionary
    """

    def __init__(self, max_size=1000):
        self.cache = OrderedDict()
        self.max_size = max_size

    def set(self, key, val):
        if len(self.cache) == self.max_size:
            self.cache.popitem(last=False)
            self.cache[key] = val
        else:
            self.cache[key] = val

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            key = make_key(args, kwargs)
            if key in self.cache:
                return self.cache.get(key)
            else:
                value = func(*args, **kwargs)
                self.set(key, value)
                return value

        return wrapper


def make_key(args, kwargs):
    key = args
    if kwargs:
        for item in kwargs.items():
            key += item
    return str(key)


def check_illegal_sql(sql):
    if not sql or sql.strip().split()[0].upper() not in (
            'INSERT', 'SELECT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'GRANT', 'DENY', 'REVOKE'):
        return True
    return False


def check_time_legality(time_string):
    try:
        datetime.datetime.strptime(time_string, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        return False


def is_valid_conf(filepath):
    if os.path.exists(filepath):
        file_abs_path = filepath
    elif os.path.exists(os.path.realpath(os.path.join(os.getcwd(), filepath))):
        file_abs_path = os.path.realpath(os.path.join(os.getcwd(), filepath))
    else:
        print("FATAL: Not found the configuration file %s." % filepath, file=sys.stderr)
        return False

    if not os.access(file_abs_path, os.R_OK):
        print("FATAL: Not have permission to read the configuration file %s." % filepath, file=sys.stderr)
        return False

    return True
