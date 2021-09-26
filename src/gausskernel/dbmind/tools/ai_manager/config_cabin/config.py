#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : config.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : config
#############################################################################

PROJECT_PATH = '/dbs/AI-tools'
# for cron lock file
TMP_DIR = PROJECT_PATH + '/lock_file'
# for temp decompression package
EXTRACT_DIR = PROJECT_PATH + '/tmp_extract_dir'
# for record anomaly detection module installed info
VERSION_RECORD_FILE_ANOMALY_DETECTION = PROJECT_PATH + '/version_record_anomaly_detection'
# for record index advisor module installed info
VERSION_RECORD_FILE_INDEX_ADVISOR = PROJECT_PATH + '/version_record_index_advisor'
# for tep keep ca certs
TMP_CA_FILE = PROJECT_PATH + '/tmp_ca'
# for python path
PYTHON_PATH = PROJECT_PATH + '/ai_lib'
# ai env file
ENV_FILE = PROJECT_PATH + '/ai_env'


# log config
LOG_PATH = '/dbs/AI-tools/ai_manager.log'
# debug:1 info:2 warning:3 error:4 fatal:5
LOG_LEVEL = 1
LOG_MAX_BYTES = 1024 * 1024 * 128
LOG_BACK_UP = 10

