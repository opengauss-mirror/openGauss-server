#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : errors.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : errors.py
#############################################################################

class Errors(object):

    FILE_DIR_PATH = {
        'gauss_0101': '[GAUSS_0101] The directory [%s] is not exist.',
        'gauss_0102': '[GAUSS_0102] The file [%s] is not exist.',
        'gauss_0103': '[GAUSS_0103] Getting install path [%s] failed.',
        'gauss_0104': '[GAUSS_0104] The path [%s] should be abs path.',
        'gauss_0105': '[GAUSS_0105] Failed to get param file.'
    }

    PARAMETER = {
        'gauss_0201': '[GAUSS_0201] The parameter [%s] is wrong.',
        'gauss_0202': '[GAUSS_0202] The config key [%s] not found in the config file or section.',
        'gauss_0203': '[GAUSS_0203] The parameter [%s] not in the valid parameter list.',
        'gauss_0204': '[GAUSS_0204] The config section [%s] not in the valid section list.',
        'gauss_0205': '[GAUSS_0205] The config option [%s] not in the valid option list.',
        'gauss_0206': '[GAUSS_0206] The scene [%s] is not in the valid scene list.',
        'gauss_0207': '[GAUSS_0207] Unsupported operating system %s.',
        'gauss_0208': '[GAUSS_0208] The parameter [%s] should be [%s].',
        'gauss_0209': '[GAUSS_0209] The parameter [%s] is essential but got None.'
    }

    ATTRIBUTE = {
        'gauss_0301': '[GAUSS_0301] Missed logger object.'
    }
    EXECUTE_RESULT = {
        'gauss_0401': '[GAUSS_0401] Failed to execute cmd [%s] when [%s] with error [%s].',
        'gauss_0402': '[GAUSS_0402] Transfer json to dict failed with error[%s].',
        'gauss_0403': '[GAUSS_0403] Failed to unpack files to install path with error:[%s].',
        'gauss_0404': '[GAUSS_0404] Failed to get old cron information with error:[%s].',
        'gauss_0405': '[GAUSS_0405] Failed to copy file to path [%s] with error:[%s].',
        'gauss_0406': '[GAUSS_0406] Failed copy file to node[%s] with error[%s].',
        'gauss_0407': '[GAUSS_0407] Failed to install module [%s] on node[%s] with error[%s].',
        'gauss_0408': '[GAUSS_0408] Transfer dict to json file failed with error[%s].',
        'gauss_0409': '[GAUSS_0409] Remote install failed with error[%s].',
        'gauss_0410': '[GAUSS_0410] Remote uninstall failed with error[%s].',
        'gauss_0411': '[GAUSS_0411] Prepare log file failed with error[%s].',
        'gauss_0412': '[GAUSS_0412] Get rand string error with status:%s.',
        'gauss_0413': '[GAUSS_0413] Failed execute cmd [%s] when [%s].',
        'gauss_0414': '[GAUSS_0414] Failed to generate [%s].',
        'gauss_0415': '[GAUSS_0415] Failed obtain [%s].',
        'gauss_0416': '[GAUSS_0416] Failed start all process.',
        'gauss_0417': '[GAUSS_0417] Failed to encrypt random string.',
        'gauss_0418': '[GAUSS_0418] Failed to install agent on node [%s] with error [%s].',
        'gauss_0419': '[GAUSS_0419] Failed to clean dir [%s] on node [%s], remain doc number [%s].',
        'gauss_0420': '[GAUSS_0420] Failed to [%s].'
    }
    CONTENT_OR_VALUE = {
        'gauss_0501': '[GAUSS_0501] The version conf info [%s] is wrong.',
        'gauss_0502': '[GAUSS_0502] The cmd [%s] for setting cron is wrong.',
        'gauss_0503': '[GAUSS_0503] The value [%s] for [%s] is wrong.',
        'gauss_0504': '[GAUSS_0504] Found same file name of ssl certs in config file.'
    }
    ILLEGAL = {
        'gauss_0601': '[GAUSS_0601] There are illegal character [%s] in the [%s].',
        'gauss_0602': '[GAUSS_0602] The [%s] is illegal.',
        'gauss_0603': '[GAUSS_0603] The [%s] should be [%s].',
        'gauss_0604': '[GAUSS_0604] The param [%s] not in params check list.',
        'gauss_0605': '[GAUSS_0605] The section:[%s] option:[%s] is invalid.',
        'gauss_0606': '[GAUSS_0606] Check config data failed with error:%s'
    }

    PERMISSION = {
        'gauss_0701': '[GAUSS_0701] The path [%s] can not access.',
        'gauss_0702': '[GAUSS_0702] The process of [%s] is already exist.',
        'gauss_0703': '[GAUSS_0703] User root is not permitted.'
    }

