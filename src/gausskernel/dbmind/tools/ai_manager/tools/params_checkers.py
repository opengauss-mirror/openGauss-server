#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : params_checkers.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Check params
#############################################################################

import re
import sys


sys.path.append(sys.path[0] + "/../")
from definitions.errors import Errors
from definitions.constants import Constant


# ===================================ANOMALY DETECTION PARAMS=======================================
# param name: check param func name
PARAMS_CHECK_MAPPING = {
    'module': 'check_module',
    'package_path': 'check_path',
    'action': 'check_action',
    'config_info': 'check_config_info',
    'scene': 'check_scene',
    'agent_nodes': 'check_agent_nodes',
    'install_path': 'check_install_path',
    'version': 'check_version',
    'service_list': 'check_service_list',
    'stopping_list': 'check_stopping_list',
    'install_nodes': 'check_install_nodes',
    'ca_info': 'check_ca_info'
}


def check_string(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % (str(obj), 'string'))


def check_valid_string(obj):
    for rac in Constant.CMD_CHECK_LIST:
        flag = obj.find(rac)
        if flag >= 0:
            raise Exception(Errors.ILLEGAL['gauss_0601'] % (rac, obj))


def check_digit(obj):
    if not str(obj).isdigit():
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % (str(obj), 'digit'))


def check_list(obj):
    if not isinstance(obj, list):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % (str(obj), 'list'))


def check_dict(obj):
    if not isinstance(obj, dict):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % (str(obj), 'dict'))


def check_password(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'password type')
    res = re.search(r'^[A-Za-z0-9~!@#%^*\-_=+?,\.]+$', obj)
    if not res:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'password character')


def check_ip(obj):
    """
    function : check if the ip address is valid
    input : String
    output : NA
    """
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % (str(obj), 'ip string'))
    valid = re.match("^(25[0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9])\.(25["
                     "0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25["
                     "0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[1-9]|0)\.(25["
                     "0-5]|2[0-4][0-9]|[0-1]{1}[0-9]{2}|[1-9]{1}[0-9]{1}|[0-9])$", obj)
    if obj == '0.0.0.0':
        valid = True
    if not valid:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'ip')


def check_port(obj):
    """
    Judge if the port is valid
    """
    if not str(obj).isdigit():
        raise Exception(Errors.ILLEGAL['gauss_0603'] % ('port', 'digit'))
    port = int(obj)
    if port < 0 or port > 65535:
        raise Exception(Errors.ILLEGAL['gauss_0602'] % 'port')
    if 0 <= port <= 1023:
        raise Exception(Errors.ILLEGAL['gauss_0602'] % 'port')


def check_scene(obj):
    check_string(obj)
    if obj not in Constant.VALID_SCENE:
        raise Exception(Errors.PARAMETER['gauss_0206'] % obj)


def check_path(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'path type')
    for rac in Constant.PATH_CHECK_LIST:
        flag = obj.find(rac)
        if flag >= 0:
            raise Exception(Errors.ILLEGAL['gauss_0601'] % (rac, obj))


def check_module(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('module name', 'string'))
    if obj not in Constant.VALID_MODULE_NAME:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'module name')


def check_action(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('action name', 'string'))
    if obj not in Constant.VALID_ACTION_NAME:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'action name')


def check_tls(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('tls type', 'string'))
    if obj not in Constant.VALID_BOOL_TYPE:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'tls type')


def check_pull_kafka(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('pull kafka param', 'string'))
    if obj not in Constant.VALID_BOOL_TYPE:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'pull_kakfa param')


def check_database_name(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('database name', 'string'))
    if obj not in Constant.VALID_DATABASE_NAME:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'database name')


def check_collection_type(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('collection type', 'string'))
    if obj not in Constant.VALID_COLLECTION_TYPE:
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'collection type')


def check_timer_interval(obj):
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('timer_interval', 'string'))
    if not re.match(r'^\d+[WDHMS]$', obj):
        raise Exception(Errors.PARAMETER['gauss_0201'] % 'timer_interval in config file')


def check_config_info(obj):
    check_dict(obj)
    for section_name, section_value in obj.items():
        if section_name not in Constant.VALID_CONFIG_SECTION_OPT_MAPPING:
            raise Exception(Errors.PARAMETER['gauss_0204'] % section_name)
        check_dict(section_value)
        for opt_name, opt_value in obj[section_name].items():
            if opt_name not in Constant.VALID_CONFIG_SECTION_OPT_MAPPING[section_name]:
                raise Exception(Errors.PARAMETER['gauss_0205'] % opt_name)
            if opt_name != 'collection_item':
                check_string(opt_value)
                if opt_name in Constant.CHECK_IP:
                    check_ip(opt_value)
                if opt_name in Constant.CHECK_PORT:
                    check_port(opt_value)
                if opt_name == 'tls':
                    check_tls(opt_value)
            else:
                check_list(opt_value)
                for item in opt_value:
                    check_list(item)
                    if len(item) != 3:
                        raise ValueError(Errors.PARAMETER['gauss_0208'] % (
                            item, 'given 3 items'))
                    if str(item[0]).lower() not in Constant.VALID_COLLECT_DATA_TYPE:
                        raise Exception(Errors.PARAMETER['gauss_0208'] % (str(item[0]), 'cn or dn'))
                    check_ip(item[1])
                    check_port(item[2])


def check_agent_nodes(obj):
    if not isinstance(obj, list):
        raise Exception(Errors.ILLEGAL['gauss_0603'] % ('type of agent nodes', 'list'))
    for node in obj:
        if not isinstance(node, dict):
            raise Exception(Errors.ILLEGAL['gauss_0603'] % ('type of agent node info', 'dict'))
        ip = node.get(Constant.NODE_IP)
        uname = node.get(Constant.NODE_USER)
        pwd = node.get(Constant.NODE_PWD)
        check_ip(ip)
        check_valid_string(uname)
        check_password(pwd)


# REMOTE
def check_install_path(obj):
    """
    Check install path when remote option
    """
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'path type')
    for rac in Constant.PATH_CHECK_LIST:
        flag = obj.find(rac)
        if flag >= 0:
            raise Exception(Errors.ILLEGAL['gauss_0601'] % (rac, obj))


def check_version(obj):
    """
    Check version info when remote option
    """
    if not isinstance(obj, str):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'path type')
    if not obj.isdigit():
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('version info', 'digit'))


def check_service_list(obj):
    """
    Check cron info when remote option
    """
    if not isinstance(obj, list):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'cron service type')
    for cron in obj:
        if cron not in Constant.ANOMALY_INSTALL_CRON_LOCALLY\
                + Constant.OPENGAUSS_ANOMALY_INSTALL_CRON_LOCALLY\
                + Constant.OPENGAUSS_ANOMALY_INSTALL_CRON_REMOTE:
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'service cron params')


def check_stopping_list(obj):
    """
    Check cron info when remote option
    """
    if not isinstance(obj, list):
        raise ValueError(Errors.ILLEGAL['gauss_0602'] % 'cron stopping type')
    for cron in obj:
        if cron not in Constant.ANOMALY_STOP_CMD_LOCALLY\
                + Constant.OPENGAUSS_ANOMALY_STOP_CMD_LOCALLY\
                + Constant.OPENGAUSS_ANOMALY_STOP_CMD_REMOTE:
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'stopping cron params')


def check_ca_info(obj):
    """
    Check ca cert information.
    """
    if not isinstance(obj, dict):
        raise ValueError(Errors.ILLEGAL['gauss_0603'] % ('ca cert info', 'type of dict'))
    for key, value in obj.items():
        if key not in Constant.VALID_CA_INFO:
            raise ValueError(Errors.ILLEGAL['gauss_0604'] % key)
        if 'pass' in key:
            check_password(obj[key])
        else:
            check_path(obj[key])


# index advisor
def check_install_nodes(obj):
    """
    Check install_nodes info
    """
    if not isinstance(obj, list):
        raise Exception(Errors.ILLEGAL['gauss_0603'] % ('type of install nodes', 'list'))
    for node in obj:
        if not isinstance(node, dict):
            raise Exception(Errors.ILLEGAL['gauss_0603'] % ('type of install node info', 'dict'))
        ip = node.get(Constant.NODE_IP)
        uname = node.get(Constant.NODE_USER)
        pwd = node.get(Constant.NODE_PWD)
        check_ip(ip)
        check_valid_string(uname)
        check_password(pwd)


class ConfigChecker(object):
    """
    Check config info value
    """
    check_mapping = {
        "server": {
            "host": check_ip,
            "listen_host": check_ip,
            "listen_port": check_port,
            "pull_kafka": check_pull_kafka
        },
        "database": {
            "name": check_database_name,
            "host": check_ip,
            "port": check_port,
            "user": check_valid_string,
            "size": check_digit,
            "max_rows": check_digit,
            "database_path": check_path
        },
        "agent": {
            "cluster_name": check_valid_string,
            "collection_type": check_collection_type,
            "collection_item": check_valid_string,
            "channel_capacity": check_digit,
            "sink_timer_interval": check_timer_interval,
            "source_timer_interval": check_timer_interval
        },
        "security": {
            "tls": check_tls,
            "ca": check_path,
            "server_cert": check_path,
            "server_key": check_path,
            "agent_cert": check_path,
            "agent_key": check_path
        },
        "log": {
            "log_path": check_path
        }
    }

    @staticmethod
    def check(section, option, value):
        try:
            func = ConfigChecker.check_mapping[section][option]
        except Exception:
            raise Exception(Errors.ILLEGAL['gauss_0605'] % (section, option))
        try:
            func(value)
        except Exception as error:
            raise Exception(Errors.ILLEGAL['gauss_0606'] % str(error))


class LostChecker(object):
    def __init__(self, params):
        self.params = params
        self.scene = None
        self.module = None
        self.action = None
        self.tls = False

    def init_globals(self):
        self.scene = self.params.get('scene')
        self.module = self.params.get('module')
        self.action = self.params.get('action')
        if not self.scene:
            raise Exception(Errors.PARAMETER['gauss_0209'] % 'scene')
        if not self.module:
            raise Exception(Errors.PARAMETER['gauss_0209'] % 'module')
        if not self.action:
            raise Exception(Errors.PARAMETER['gauss_0209'] % 'action')

    def check_agent_nodes(self):
        """
        Check agent node info for opengauss scene.
        :return:
        """
        agent_nodes = self.params.get(Constant.AGENT_NODES)
        if not isinstance(agent_nodes, list):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'agent nodes')
        for each_node in agent_nodes:
            if not isinstance(each_node, dict):
                raise Exception(Errors.PARAMETER['gauss_0201'] % 'each agent node')
            node_ip = each_node.get(Constant.NODE_IP)
            user_name = each_node.get(Constant.NODE_USER)
            password = each_node.get(Constant.NODE_PWD)
            if not all([node_ip, user_name, password]):
                raise Exception(Errors.PARAMETER['gauss_0201'] % 'agent node info')

    def check_ca_info(self):
        """
        Check ca information in https mode
        :return:
        """
        ca_info = self.params.get(Constant.CA_INFO)
        if not isinstance(ca_info, dict):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'ca info')
        cert_path = ca_info.get(Constant.CA_CERT_PATH)
        cert_key = ca_info.get(Constant.CA_KEY_PATH)
        cert_pass = ca_info.get(Constant.CA_PASSWORD)
        if not all([cert_path, cert_pass, cert_key]):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'ca info')

    @staticmethod
    def _check_server(server_info):
        """
        Check server info of config
        :param server_info:
        :return:
        """
        host = server_info.get(Constant.SERVER_HOST)
        listen_host = server_info.get(Constant.SERVER_LISTEN_HOST)
        listen_port = server_info.get(Constant.SERVER_LISTEN_PORT)
        pull_kafka = server_info.get(Constant.SERVER_PULL_KAFKA)
        if pull_kafka and (pull_kafka not in Constant.VALID_BOOL_TYPE):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'pull_kafka')
        if not all([host, listen_host, listen_port]):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'server config info')

    @staticmethod
    def _check_database(database_info):
        """
        Check database info of config
        :param database_info:
        :return:
        """
        name = database_info.get(Constant.DATABASE_NAME)
        host = database_info.get(Constant.DATABASE_HOST)
        user = database_info.get(Constant.DATABASE_USER)
        port = database_info.get(Constant.DATABASE_PORT)
        size = database_info.get(Constant.DATABASE_SIZE)
        max_rows = database_info.get(Constant.DATABASE_MAX_ROWS)
        if name is None or (not isinstance(name, str)):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'database name')
        if name.lower() not in Constant.VALID_DATABASE_NAME:
            raise Exception(Errors.ILLEGAL['gauss_0602'] % 'database name')
        if name.lower() != Constant.VALID_DATABASE_NAME[0]:
            if not all([host, user, port]):
                raise Exception(Errors.PARAMETER['gauss_0201'] % 'database info')
        if (size is not None) and (not re.match(r'^\d+$', size)):
            raise Exception(
                Errors.PARAMETER['gauss_0208'] % ('size of database', 'positive integer'))
        if (max_rows is not None) and (not re.match(r'^\d+$', max_rows)):
            raise Exception(
                Errors.PARAMETER['gauss_0208'] % ('max_rows of database', 'positive integer'))

    @staticmethod
    def _check_agent(agent_info):
        """
        Check agent info when opengauss scene
        :param agent_info:
        :return:
        """
        cluster_name = agent_info.get(Constant.AGENT_CLUSTER_NAME)
        collection_type = agent_info.get(Constant.AGENT_COLLECTION_TYPE)
        collection_item = agent_info.get(Constant.AGENT_COLLECTION_ITEM)
        channel_capacity = agent_info.get(Constant.AGENT_CHANNEL_CAPACITY)
        source_timer_interval = agent_info.get(Constant.AGENT_SOURCE_TIMER_INTERVAL)
        sink_timer_interval = agent_info.get(Constant.AGENT_SINK_TIMER_INTERVAL)
        if not all([cluster_name, collection_type, collection_item]):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'agent config info')
        if collection_type not in Constant.VALID_COLLECTION_TYPE:
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'collection_type of agent info')
        if (channel_capacity is not None) and (not re.match(r'^\d+$', channel_capacity) or int(
                channel_capacity) == 0):
            raise Exception(
                Errors.PARAMETER['gauss_0208'] % (
                    'channel_capacity of agent info', 'positive integer'))
        if (source_timer_interval is not None) and (
                not re.match(r'^\d+[WDHMS]$', source_timer_interval)):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'source_timer_interval of agent info')
        if (sink_timer_interval is not None) and (
                not re.match(r'^\d+[WDHMS]$', sink_timer_interval)):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'sink_timer_interval of agent info')

    def check_config_info(self):
        """
        Check config info
        :return:
        """
        config_info = self.params.get(Constant.CONFIG_INFO)
        if not isinstance(config_info, dict):
            raise Exception(Errors.PARAMETER['gauss_0208'] % ('config info', 'dict'))
        server = config_info.get(Constant.SERVER)
        if not isinstance(server, dict):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'server info')
        self._check_server(server)
        database = config_info.get(Constant.DATABASE)
        if not isinstance(database, dict):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'database info')
        self._check_database(database)
        agent = config_info.get(Constant.AGENT)
        if not isinstance(database, dict):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'agent info')
        if self.scene == Constant.SCENE_OPENGAUSS:
            self._check_agent(agent)
        security = config_info.get(Constant.AD_CONF_SECTION_SECURITY)
        tls = security.get(Constant.AD_CONF_TLS_FLAG) if security else None
        if tls in ['True', True]:
            self.tls = True

    def run(self):
        self.init_globals()
        if self.scene == Constant.SCENE_OPENGAUSS:
            self.check_agent_nodes()
        if self.action == 'uninstall':
            return
        self.check_config_info()
        if self.tls is True:
            self.check_ca_info()

