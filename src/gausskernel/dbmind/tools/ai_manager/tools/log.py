#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : log.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : log
#############################################################################

import logging
import os
import re
import sys
from logging import StreamHandler
from logging.handlers import RotatingFileHandler
sys.path.append(sys.path[0] + "/../")
from config_cabin.config import LOG_PATH
from config_cabin.config import LOG_LEVEL
from config_cabin.config import LOG_MAX_BYTES
from config_cabin.config import LOG_BACK_UP
from definitions.constants import Constant
from definitions.errors import Errors
from tools.common_tools import CommonTools


class MaskHandler(StreamHandler):
    def __init__(self, *arg, **kwargs):
        super().__init__(*arg, **kwargs)

    @staticmethod
    def __mask_dict_data(msg):
        """
        mask sensitive data in dict style
        """
        regs_mapping = {
            r'password[^:]*:([^,}]+)': 'password*: ******'
        }
        for reg, rep in regs_mapping.items():
            msg = re.sub(reg, rep, str(msg))
        return msg

    @staticmethod
    def __mask_sensitive_para(msg):
        """ mask gs tools Sensitive param """
        mask_items = {
            "gsql": ["--with-key", "-k"],
            "gs_encrypt": ["--key-base64", "--key", "-B", "-k"],
            "gs_guc encrypt": ["-K"],
            "gs_guc generate": ["-S"],
            "gs_dump": ["--rolepassword", "--with-key"],
            "gs_dumpall": ["--rolepassword", "--with-key"],
            "gs_restore": ["--with-key", "--rolepassword"],
            "gs_ctl": ["-P"],
            "gs_redis": ["-A"],
            "gs_initdb": ["--pwprompt", "--pwpasswd"],
            "gs_roach": ["--obs-sk"],
            "InitInstance": ["--pwpasswd"]
        }
        for t_key, t_value in mask_items.items():
            if t_key in msg:
                pattern = re.compile("|".join([r"(?<=%s)[ =]+[^ ]*[ ]*" % i for i in t_value]))
                msg = pattern.sub(lambda m: " *** ", msg)
        return msg

    def mask_pwd(self, msg):
        """mask pwd in msg"""
        replace_reg = re.compile(r'-W[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('-W *** ', str(msg))
        replace_reg = re.compile(r'-w[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('-w *** ', str(msg))
        replace_reg = re.compile(r'--password[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('--password *** ', str(msg))
        replace_reg = re.compile(r'--pwd[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('--pwd *** ', str(msg))
        replace_reg = re.compile(r'--root-passwd[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('--root-passwd *** ', str(msg))
        if msg.find("gs_guc") >= 0:
            msgd = msg.split()
            for idx in range(len(msgd)):
                if "gs_guc" in msgd[idx] and len(msgd) > (idx + 5) and \
                        msgd[idx + 1] == "encrypt" and msgd[idx + 3] in (
                        "server", "client", "source"):
                    regula = re.compile(r"-K[ ]*[^ ]*[ ]*")
                    msg = regula.sub("-K *** ", str(msg))

        msg = self.__mask_sensitive_para(msg)
        msg = self.__mask_dict_data(msg)
        replace_reg = re.compile(r'echo[ ]*[^ ]*[ ]*')
        msg = replace_reg.sub('echo *** ', str(msg))
        return msg

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        try:
            record.msg = self.mask_pwd(record.msg)
            msg = self.format(record)
            stream = self.stream
            # issue 35046: merged two stream.writes into one.
            stream.write(msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)


class MainLog(object):
    def __init__(self, log_name):
        self.log_file = None
        self.expect_level = None
        self.logger = None
        self.log_name = log_name
        self.log_formatter = None
        self.__init_globals()

    def __init_globals(self):
        self.log_file = LOG_PATH
        self.expect_level = LOG_LEVEL
        self.log_formatter = Constant.LOG_FORMATTER
        self.__create_logfile()

    def get_logger(self):
        log_level = self.__get_log_level(self.expect_level)
        logger = logging.getLogger(self.log_name)
        logger.setLevel(log_level)
        if not logger.handlers:
            # stream handler
            handler_mask_stream = MaskHandler()
            stream_formatter = logging.Formatter(self.log_formatter)
            handler_mask_stream.setFormatter(stream_formatter)
            logger.addHandler(handler_mask_stream)

            # file handler
            file_rotating_handler = RotatingFileHandler(
                self.log_file, mode="a", maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACK_UP)
            file_formatter = logging.Formatter(self.log_formatter)
            file_rotating_handler.setFormatter(file_formatter)
            logger.addHandler(file_rotating_handler)

        return logger

    def __get_log_level(self, log_level):
        if self.expect_level not in Constant.VALID_LOG_LEVEL:
            raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'Log level')
        return log_level * 10

    def __create_logfile(self):
        """
        function: create log file
        input : N/A
        output: N/A
        """
        try:
            if not os.path.isdir(os.path.dirname(self.log_file)):
                CommonTools.mkdir_with_mode(
                    os.path.dirname(self.log_file), Constant.AUTH_COMMON_DIR_STR)
            CommonTools.create_file_if_not_exist(self.log_file)
        except Exception as error:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0411'] % error)
