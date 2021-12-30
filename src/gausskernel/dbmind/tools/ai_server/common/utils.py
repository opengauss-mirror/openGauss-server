#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : utils.py
# Version      :
# Date         : 2021-4-7
# Description  : Common Methods
#############################################################################

import sys
import os
try:
    import ssl
    import json
    import logging
    import re
    import pwd
    import dateutil.parser
    from subprocess import Popen, PIPE
    from configparser import ConfigParser
    from datetime import datetime, timedelta
    from logging import handlers

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
except ImportError as err:
    sys.exit("utils.py: Failed to import module, \nError: %s" % str(err))

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../dbmind.conf")


class Common:

    @staticmethod
    def transform_time_string(time_string, mode='timedelta'):
        """
        Transform time string to timedelta or second, only support 'weeks(W), days(D),
        hours(H), minutes(M), seconds(S)
        :param time_string: string,  time string like '10S', '20H', '3W'.
        :param mode: string, 'timedelta' or 'to_second', 'timedelta' represent transform
        time_string to timedelta, 'to_second' represent transform time_string to second.
        :return: 'mode' is 'timedelta', return datetime.timedelta; 'mode' is 'to_second',
        return int(second).
        """
        if mode not in ('timedelta', 'to_second'):
            raise ValueError('wrong mode {mode} in time_transfer.'.format(mode=mode))

        time_prefix, time_suffix = re.match(r'(\d+)?([WDHMS])', time_string).groups()

        if time_suffix is None:
            raise ValueError('wrong format {time_string} for time_string in time_transfer.'.format(
                time_string=time_string))

        if time_prefix is None:
            time_prefix = 1
        else:
            time_prefix = int(time_prefix)

        timedelta_mapper = {'W': timedelta(weeks=1),
                            'D': timedelta(days=1),
                            'H': timedelta(hours=1),
                            'M': timedelta(minutes=1),
                            'S': timedelta(seconds=1)}

        second_mapper = {'W': 7 * 24 * 3600, 'D': 24 * 3600, 'H': 3600, 'M': 60, 'S': 1}

        if mode == 'timedelta':
            return timedelta_mapper.get(time_suffix) * time_prefix
        if mode == 'to_second':
            return second_mapper.get(time_suffix) * time_prefix

    @staticmethod
    def create_logger(log_name, log_path, level):
        """
        Create logger.
        :param log_name: string, log name.
        :param log_path: string, log path.
        :param level: string, log level such as 'INFO', 'WARN', 'ERROR'.
        """
        logger = logging.getLogger(log_name)
        agent_handler = handlers.RotatingFileHandler(filename=log_path,
                                                     maxBytes=1024 * 1024 * 100,
                                                     backupCount=5)
        agent_handler.setFormatter(logging.Formatter(
            "[%(asctime)s %(levelname)s]-[%(filename)s][%(lineno)d]: %(message)s"))
        logger.addHandler(agent_handler)
        logger.setLevel(
            getattr(logging, level.upper()) if hasattr(logging, level.upper()) else logging.INFO)
        return logger

    @staticmethod
    def unify_byte_unit(byte_info):
        """
        Transfer unit of K、M、G、T、P to M
        :param byte_info: string, byte information like '100M', '2K', '30G'.
        :return: int, bytes size in unit of M, like '400M' -> 400.
        """
        byte_info = byte_info.upper()
        bytes_prefix, bytes_suffix = re.match(r'^(\d+|\d+\.\d+)([KMGTP])', byte_info).groups()
        if bytes_prefix is None or bytes_suffix is None or bytes_suffix not in 'KMGTP':
            raise ValueError('can not parse format of {bytes}'.format(bytes=byte_info))
        byte_unit_mapper = {'K': 1 / 1024, 'M': 1, 'G': 1024, 'T': 1024 * 1024,
                            'P': 1024 * 1024 * 1024}
        return byte_unit_mapper[bytes_suffix] * int(float(bytes_prefix))

    @staticmethod
    def check_certificate(certificate_path):
        """
        Check whether the certificate is expired or invalid.
        :param certificate_path: path of certificate.
        output: dict, check result which include 'check status' and 'check information'.
        """
        check_result = {}
        certificate_warn_threshold = 365
        child = Popen(['openssl', 'x509', '-in', certificate_path, '-noout', '-dates'],
                      shell=False, stdout=PIPE, stdin=PIPE)
        sub_chan = child.communicate()
        if sub_chan[1] or not sub_chan[0]:
            check_result['status'] = 'fail'
        else:
            check_result['status'] = 'success'
            not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
            end_time = dateutil.parser.parse(not_after).replace(tzinfo=None)
            certificate_remaining_days = (end_time - datetime.now()).days
            if 0 < certificate_remaining_days < certificate_warn_threshold:
                check_result['level'] = 'warn'
                check_result['info'] = "the '{certificate}' has {certificate_remaining_days} " \
                                       "days before out of date." \
                    .format(certificate=certificate_path,
                            certificate_remaining_days=certificate_remaining_days)
            elif certificate_remaining_days >= certificate_warn_threshold:
                check_result['level'] = 'info'
                check_result['info'] = "the '{certificate}' has {certificate_remaining_days} " \
                                       "days before out of date."\
                    .format(certificate=certificate_path,
                            certificate_remaining_days=certificate_remaining_days)
            else:
                check_result['level'] = 'error'
                check_result['info'] = "the '{certificate}' is out of date." \
                    .format(certificate=certificate_path)
        return check_result

    @staticmethod
    def execute_cmd(cmd, shell=True):
        """
        execute cmd
        :param cmd: cmd str
        :param shell: execute shell mode, True or False
        :return: execute result
        """
        if not shell:
            cmd_list = [cmd.strip() for cmd in cmd.split("|")]
            proc = Popen(cmd_list[0].split(), stdout=PIPE, stderr=PIPE, shell=shell)
            if len(cmd_list) > 1:
                for i in range(1, len(cmd_list)):
                    proc = Popen(cmd_list[i].split(), stdin=proc.stdout, stdout=PIPE, stderr=PIPE,
                                 shell=shell)
        else:
            proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=shell)
        std, err_msg = proc.communicate()
        if proc.returncode != 0:
            raise Exception("Failed to execute command: %s, \nError: %s." % (cmd, str(err_msg)))
        return std, err_msg

    @staticmethod
    def check_ip_and_port(ip, port):
        """
        check the ip and port valid or not
        """
        if not str(port).isdigit():
            raise Exception("The value type of port is invalid, it must be an integer.")
        check_ip_pattern = r"^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\." \
                           r"(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\." \
                           r"(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\." \
                           r"(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])$"
        if not re.match(check_ip_pattern, ip):
            raise Exception("The host ip is invalid.")

    @staticmethod
    def acquire_collection_info():
        """
        Obtain the IP addresses, port and collect data type
        :return: {data_type: xxx, ip: xxx, port:xxx}
        """
        collect_info = {"data_type": "", "ip": "", "port": ""}
        value = Common.parser_config_file("agent", "collection_item")
        # value is: '[data_type, host, port]'
        if len(json.loads(value)) != 3:
            raise ValueError("can not parse format of '{value}'".format(value=value))
        collect_info["data_type"] = json.loads(value)[0]
        collect_info["ip"] = json.loads(value)[1]
        collect_info["port"] = json.loads(value)[2]
        Common.check_ip_and_port(collect_info["ip"], collect_info["port"])
        return collect_info

    @staticmethod
    def parser_config_file(section, option,
                           conf_path=os.path.join(os.path.dirname(__file__), "../dbmind.conf")):
        """
        parser the config file
        :param option: config option
        :param section: config section
        :param conf_path: config file path
        :return: config value
        """
        if not os.path.isfile(conf_path):
            raise Exception("The %s does not exists." % conf_path)
        config = ConfigParser()
        config.read(conf_path)
        try:
            value = config.get(section, option)
            if not value:
                raise Exception("The value of %s is empty." % option)
        except Exception as err_msg:
            raise Exception(str(err_msg))
        return value.lower()

    @staticmethod
    def get_proc_pid(ip, port):
        get_process_pid_cmd = "netstat -nltp | grep '%s:%s' | awk '{print $7}'" % (ip, port)
        std, _ = Common.execute_cmd(get_process_pid_cmd)
        if std.decode("utf-8").strip() == "-":
            raise Exception("The collected process does not exist.")
        # std is: b'PID/Program name\n'
        proc_pid = std.decode("utf-8").strip().split("/")[0]
        return proc_pid

    @staticmethod
    def check_certificate_setting(logger, config_path, mode):
        """
        If use https method, it is used for checking whether CA and Agent certificate is valid,
        if certificate is not valid, then exit process, otherwise return right context;
        if use http method, it skip checking and just return None.
        :param mode: agent|server
        :param logger: logger object.
        :param config_path: string, config path.
        :return: if 'https', return certificate context, else return None.
        """
        logger.info("Checking certificate setting...")
        context = None
        config = ConfigParser()
        config.read(config_path)
        if config.has_option('security', 'tls') and config.getboolean('security', 'tls'):
            try:
                agent_cert = os.path.realpath(config.get('security', '%s_cert' % mode))
                agent_key = os.path.realpath(config.get('security', '%s_key' % mode))
                ca = os.path.realpath(config.get('security', 'ca'))
            except Exception as e:
                logger.error(e)
                sys.exit(1)
            else:
                ssl_certificate_status = Common.check_certificate(agent_cert)
                ca_certificate_status = Common.check_certificate(ca)
                if ssl_certificate_status['status'] == 'fail' \
                        or ca_certificate_status['status'] == 'fail':
                    logger.warn("error occur when check 'certificate'.")
                else:
                    if ssl_certificate_status['level'] == 'error' or \
                            ca_certificate_status['level'] == 'error':
                        logger.error("{ssl_certificate_info}; {ca_certificate_info}".format(
                            ssl_certificate_info=ssl_certificate_status['info'],
                            ca_certificate_info=ca_certificate_status['info']))
                        sys.exit(1)
                    else:
                        logger.warn("{ssl_certificate_info}; {ca_certificate_info}".format(
                            ssl_certificate_info=ssl_certificate_status['info'],
                            ca_certificate_info=ca_certificate_status['info']))

                        pw_file = os.path.join(os.path.dirname(config_path), 'certificate/pwf')
                        from common.encrypt_decrypt_handler import AesCbcUtil
                        pw = AesCbcUtil.aes_cbc_decrypt_with_multi(pw_file)

                        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                        context.check_hostname = False
                        context.load_cert_chain(certfile=agent_cert, keyfile=agent_key, password=pw)
        logger.info("Successfully checked certificate setting.")
        return context

    @staticmethod
    def check_proc_exist(proc_name):
        """
        check proc exist
        :param proc_name: proc name
        :return: proc pid
        """
        check_proc = "ps ux | grep '%s' | grep -v grep | grep -v nohup | awk \'{print $2}\'" % proc_name
        std, _ = Common.execute_cmd(check_proc)
        current_pid = str(os.getpid())
        pid_list = [pid for pid in std.decode("utf-8").split("\n") if pid and pid != current_pid]
        if not pid_list:
            return ""
        return " ".join(pid_list)


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
        import psycopg2
        self.conn = psycopg2.connect(host=self.host,
                                     user=self.user,
                                     passwd=self.password,
                                     database=self.database,
                                     port=self.port)
        self.conn.set_client_encoding('latin9')
        self.cursor = self.conn.cursor()

    def fetch_all_result(self, sql, logger):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            logger.error(str(e))

    def fetch_one_result(self, sql, logger):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            return result
        except Exception as e:
            logger.error(str(e))

    def close(self):
        self.cursor.close()
        self.conn.close()
