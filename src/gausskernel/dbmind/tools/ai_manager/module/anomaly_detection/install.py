#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : install.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Local install
#############################################################################

import optparse
import os
import time
import sys
from datetime import datetime

sys.path.append(sys.path[0] + "/../../")
from tools import params_checkers
from tools.common_tools import CommonTools
from definitions.constants import Constant
from definitions.errors import Errors
from config_cabin.config import PROJECT_PATH
from config_cabin.config import VERSION_RECORD_FILE_ANOMALY_DETECTION
from config_cabin.config import EXTRACT_DIR
from config_cabin.config import TMP_CA_FILE
from tools.global_box import g
from config_cabin.config import ENV_FILE
from tools.env_handler import EnvHandler
from tools.cert_generator import CertGenerator


class Installer(object):
    def __init__(self, **param_dict):
        self.param_dict = param_dict
        self.project_path = PROJECT_PATH
        self.module_name = self.param_dict.get(Constant.MODULE)
        self.agent_nodes = self.param_dict.get(Constant.AGENT_NODES)
        self.package_path = self.param_dict.get('package_path')
        self.install_path = None
        self.version = None
        self.service_list = None
        self.stopping_list = None
        self.module_path = None
        self.ca_info = None

    def check_remote_params(self):
        funcs = CommonTools.get_funcs(params_checkers)
        for param_name, param_value in self.param_dict.items():
            if param_name not in params_checkers.PARAMS_CHECK_MAPPING:
                raise Exception(Errors.PARAMETER['gauss_0203'] % param_name)
            funcs[params_checkers.PARAMS_CHECK_MAPPING[param_name]](param_value)

    def init_globals(self):
        self.install_path = self.param_dict.get('install_path')
        self.version = self.param_dict.get('version')
        self.service_list = self.param_dict.get('service_list')
        self.module_name = Constant.AI_SERVER if \
            self.module_name == Constant.MODULE_ANOMALY_DETECTION else self.module_name
        self.module_path = os.path.realpath(os.path.join(self.install_path, self.module_name))

    def check_project_path_access(self):
        """
        Check project path is full authority.
        """
        CommonTools.check_dir_access(self.project_path, 'full')

    def read_ca_cert_path(self, agent_only=False):
        """
        Read ca certs path from config file
        """
        ad_config_path = os.path.join(self.install_path, Constant.ANOMALY_DETECTION_CONFIG_PATH)
        # read agent cert path
        agent_cert_path = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY,
            Constant.AD_CONF_AGENT_CERT, self.module_path)
        # read agent key path
        agent_key_path = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY,
            Constant.AD_CONF_AGENT_KEY, self.module_path)
        # read ca root cert path
        ca_root_file_path = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY,
            Constant.AD_CONF_CA_PATH, self.module_path)
        if agent_only:
            return [agent_key_path, agent_cert_path, ca_root_file_path]
        # get ca root key path
        ca_root_key_path = ca_root_file_path + '.key'
        # read server cert path
        server_cert_path = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY,
            Constant.AD_CONF_SERVER_CERT, self.module_path)
        # read server key path
        server_key_path = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY,
            Constant.AD_CONF_SERVER_KEY, self.module_path)
        # judge the basename of path is duplicate
        file_names = [os.path.basename(file) for file in [
            ca_root_file_path, server_cert_path, server_key_path, agent_cert_path, agent_key_path]]
        if len(file_names) != len(set(file_names)):
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0504'])
        return ca_root_file_path, ca_root_key_path, server_cert_path, \
               server_key_path, agent_cert_path, agent_key_path

    def backup_db_file(self):
        """
        Backup data file.
        """
        ad_config_path = os.path.join(self.install_path, Constant.ANOMALY_DETECTION_CONFIG_PATH)
        if not os.path.isfile(ad_config_path):
            g.logger.info('Config file not exist, can not backup db file')
            return False, None, None
        db_cabin = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_DATABASE,
            Constant.AD_CONF_DATABASE_PATH, self.module_path)
        if os.path.isdir(db_cabin):
            g.logger.info('Start backup db file.')
            back_up_path = os.path.join(EXTRACT_DIR, os.path.basename(db_cabin))
            CommonTools.copy_file_to_dest_path(db_cabin, back_up_path)
            return True, db_cabin, back_up_path
        else:
            g.logger.info('No need backup db file.')
            return False, None, None

    @staticmethod
    def restore_db_file(status, db_cabin, back_up_path):
        """
        Restore db file.
        """
        if status:
            CommonTools.mkdir_with_mode(os.path.dirname(db_cabin), Constant.AUTH_COMMON_DIR_STR)
            CommonTools.copy_file_to_dest_path(back_up_path, db_cabin)
        else:
            g.logger.info('No need Restore db file.')

    def prepare_module_path(self):
        """
        Prepare install path
        """
        self._mk_module_dir()
        self._clean_module_dir()
        self._check_module_path_access()

    def modify_config_file(self, remote=False):
        """
        Modify config file with "config_info" in param file.
        """
        config_path = os.path.realpath(os.path.join(
            self.install_path, Constant.ANOMALY_DETECTION_CONFIG_PATH))
        g.logger.info('Got config file path:[%s]' % config_path)
        if not os.path.isfile(config_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % config_path)
        config_info = self.param_dict.get('config_info')
        if not remote:
            CommonTools.modify_config_file(config_path, config_info)
        else:
            CommonTools.modify_agent_config_file(config_path, config_info)
        g.logger.info('Successfully modify config file.')

    def try_to_kill_process_exist(self):
        """
        Try to kill process, if already exist
        """
        script_path = os.path.realpath(
            os.path.join(self.install_path, Constant.ANORMALY_MAIN_SCRIPT))
        process_list = [(cmd % (Constant.CMD_PREFIX, os.path.dirname(script_path), script_path)
                         ).split(self.version)[-1] for cmd in self.service_list]
        for process in process_list:
            process_num = CommonTools.check_process(process)
            if process_num:
                CommonTools.grep_process_and_kill(process)
                g.logger.info('Killed process of [%s]' % process)

    def start_agent_server_monitor(self):
        """
        Add cron service for agent, server, monitor.
        """
        script_path = os.path.realpath(
            os.path.join(self.install_path, Constant.ANORMALY_MAIN_SCRIPT))
        for cmd in self.service_list:
            cron_cmd = cmd % (Constant.CMD_PREFIX, os.path.dirname(script_path), script_path)
            status, output = CommonTools.add_cron(self.install_path, cron_cmd, '1m')
            if status != 0:
                err = 'add cron CMD[%s]-STATUS[%s]-OUTPUT[%s]' % (cron_cmd, status, output)
                raise Exception(Errors.EXECUTE_RESULT['gauss_0420'] % err)
            else:
                g.logger.info('Successfully add new cron:[%s]' % cron_cmd)

    @staticmethod
    def modify_env_file():
        handler = EnvHandler(ENV_FILE)
        handler.run()

    def waiting_for_start(self, wait_seconds):
        """
        Wait cron start agent, server and monitor in 1m.
        """
        script_path = os.path.realpath(
            os.path.join(self.install_path, Constant.ANORMALY_MAIN_SCRIPT))
        process_list = [(cmd % (Constant.CMD_PREFIX, os.path.dirname(script_path), script_path)
                         ).split(self.version)[-1] for cmd in self.service_list]
        ret_mapping = {}
        for sec in range(wait_seconds):
            for process in process_list:
                process_num = CommonTools.check_process(process)
                ret_mapping[process] = process_num
            if all(ret_mapping.values()):
                g.logger.info('Successfully start all process.')
                return
            time.sleep(1)
        g.logger.error('Failed start all process with result:%s' % str(ret_mapping))
        raise Exception(Errors.EXECUTE_RESULT['gauss_0416'])

    def _mk_module_dir(self):
        """
        Create install path if the path is not exist.
        """
        if not os.path.isdir(self.module_path):
            g.logger.info('Install path:%s is not exist, start creating.' % self.module_path)
            CommonTools.mkdir_with_mode(self.module_path, Constant.AUTH_COMMON_DIR_STR)
        else:
            g.logger.info('Install path:%s is already exist.' % self.module_path)

    def _clean_module_dir(self):
        """
        Clean install path before unpack.
        """
        file_list = os.listdir(self.module_path)
        if file_list:
            g.logger.info('Start clean install path, file list:[%s]' % file_list)
            CommonTools.clean_dir(self.module_path)

    def deploy_module_files(self):
        """
        Copy files to module path.
        """
        from_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))))), self.module_name)
        to_path = os.path.realpath(os.path.join(self.install_path, self.module_name))
        CommonTools.copy_file_to_dest_path(from_path, to_path)
        g.logger.info('Successfully to copy files to package path.')

    def deploy_manager_files(self):
        """
        Copy manager files to manager dir
        """
        from_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.realpath(__file__))))), Constant.AI_MANAGER_PATH)
        to_path = os.path.realpath(os.path.join(
            self.install_path, Constant.AI_MANAGER_PATH))
        CommonTools.copy_file_to_dest_path(from_path, to_path)
        g.logger.info('Successfully to copy files to manager path.')

    def prepare_ca_certificates(self):
        """
        Generate server ca certificates.
        """
        ca_root_file_path, ca_root_key_path, server_cert_path, server_key_path, agent_cert_path, \
            agent_key_path = self.read_ca_cert_path()
        self.ca_info = self.param_dict.pop(Constant.CA_INFO)
        if not self.ca_info:
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'ca root cert information')
        get_ca_root_cert_path = self.ca_info.get(Constant.CA_CERT_PATH)
        get_ca_root_key_path = self.ca_info.get(Constant.CA_KEY_PATH)
        get_ca_root_password = self.ca_info.get(Constant.CA_PASSWORD)
        if not all([get_ca_root_cert_path, get_ca_root_key_path, get_ca_root_password]):
            raise Exception(Errors.PARAMETER['gauss_0201'] % 'items info of ca root cert')

        # copy ca root cert and key files to path of configured
        g.logger.info('Start deploy ca root files.')
        CommonTools.remove_files([ca_root_file_path, ca_root_key_path])
        CommonTools.mkdir_with_mode(os.path.dirname(ca_root_file_path),
                                    Constant.AUTH_COMMON_DIR_STR)
        CommonTools.copy_file_to_dest_path(get_ca_root_cert_path, ca_root_file_path)
        CommonTools.mkdir_with_mode(os.path.dirname(ca_root_key_path),
                                    Constant.AUTH_COMMON_DIR_STR)
        CommonTools.copy_file_to_dest_path(get_ca_root_key_path, ca_root_key_path)

        # get ssl password
        ssl_password = CertGenerator.get_rand_str()
        ca_config_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), Constant.CA_CONFIG)
        server_ip = CommonTools.get_local_ip(ignore=True)
        g.logger.info('Get server ip:[%s].' % server_ip)

        # create server cert
        CertGenerator.create_ca_certificate_with_script(get_ca_root_password, ssl_password,
                                                        ca_root_file_path, ca_root_key_path,
                                                        ca_config_path, server_cert_path,
                                                        server_key_path, server_ip,
                                                        crt_type='server')
        g.logger.info('Successfully generate server ca certificate.')
        return get_ca_root_password, ssl_password, ca_root_file_path, ca_root_key_path, \
            ca_config_path, agent_cert_path, agent_key_path

    def _generate_key_file(self, password):
        """
        Create key file
        """
        key_file_path = os.path.join(self.module_path, Constant.PWF_PATH)
        CommonTools.mkdir_with_mode(key_file_path, Constant.AUTH_COMMON_DIR_STR)
        CommonTools.clean_dir(key_file_path)
        encrypt_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.realpath(__file__)))))), Constant.ENCRYPT_TOOL)
        lib_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))))))), 'lib')
        CommonTools.encrypt_with_path(password, key_file_path, encrypt_path, lib_path)
        g.logger.info('Successfully generate key files with path [%s].' % key_file_path)
        return key_file_path

    def generate_agent_ca_files(self, ca_password, ssl_password, ca_root_file_path,
                                ca_root_key_path, ca_config_path, agent_cert_path, agent_key_path):
        """
        Generate client ca certificates.
        """
        for node in self.agent_nodes:
            ip = node.get(Constant.NODE_IP)
            username = node.get(Constant.NODE_USER)
            pwd = node.get(Constant.NODE_PWD)

            CertGenerator.create_ca_certificate_with_script(ca_password, ssl_password,
                                                            ca_root_file_path, ca_root_key_path,
                                                            ca_config_path, agent_cert_path,
                                                            agent_key_path, ip, crt_type='agent')
            file_list = self.read_ca_cert_path(agent_only=True)
            key_file = self._generate_key_file(ssl_password)
            file_list.append(key_file)
            temp_ca_dir = TMP_CA_FILE
            CertGenerator.deploy_ca_certs(
                file_list, ip, username, pwd, temp_ca_dir, no_delete=[key_file, ca_root_file_path])
            g.logger.info('Successfully deploy certs to node:[%s]' % ip)

    def deploy_agent_certs(self):
        """
        Copy file from temp dir to config path.
        """
        file_list = self.read_ca_cert_path(agent_only=True)
        file_list.append(os.path.join(self.module_path, Constant.PWF_PATH))
        temp_cert_list = os.listdir(TMP_CA_FILE)
        for dest_path in file_list:
            CommonTools.mkdir_with_mode(
                os.path.dirname(dest_path), Constant.AUTH_COMMON_DIR_STR)
        CommonTools.remove_files(file_list)
        g.logger.info('Successfully prepare the path:%s' % str(file_list))
        for file_name in temp_cert_list:
            for dest_file in file_list:
                if file_name in dest_file:
                    from_path = os.path.join(TMP_CA_FILE, file_name)
                    CommonTools.copy_file_to_dest_path(from_path, dest_file)
        CommonTools.clean_dir(TMP_CA_FILE)

    def _check_module_path_access(self):
        """
        Check module path is full authority.
        """
        CommonTools.check_dir_access(self.module_path, 'full')

    def record_version_info(self):
        """
        Record install time, version, install path in record file.
        """
        time_install = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
        content = '|'.join([time_install, self.version, self.install_path]) + '\n'
        CommonTools.add_content_to_file(VERSION_RECORD_FILE_ANOMALY_DETECTION, content)
        CommonTools.delete_early_record(
            VERSION_RECORD_FILE_ANOMALY_DETECTION, Constant.VERSION_FILE_MAX_LINES)
        g.logger.info('Successfully record version information.')

    def unpack_file_to_temp_dir(self):
        """
        Unpack file to temp dir on remote node.
        """
        # mk temp extract dir if not exist
        CommonTools.mkdir_with_mode(EXTRACT_DIR, Constant.AUTH_COMMON_DIR_STR)
        # clean extract dir
        CommonTools.clean_dir(EXTRACT_DIR)
        # extract package file to temp dir
        CommonTools.extract_file_to_dir(self.package_path, EXTRACT_DIR)
        g.logger.info('Success unpack files to temp dir.')

    def run(self, remote=False):
        if remote:
            self.check_remote_params()
            self.check_project_path_access()
        self.init_globals()
        back_status, from_path, to_path = self.backup_db_file()
        if not remote:
            self.prepare_module_path()
            g.logger.info('Start deploy module files.')
            self.deploy_module_files()
        self.restore_db_file(back_status, from_path, to_path)
        g.logger.info('Start modify config file.')
        self.modify_config_file(remote=remote)
        g.logger.info('Start parse ca information.')
        ad_config_path = os.path.join(self.install_path, Constant.ANOMALY_DETECTION_CONFIG_PATH)
        tls = CommonTools.read_info_from_config_file(
            ad_config_path, Constant.AD_CONF_SECTION_SECURITY, Constant.AD_CONF_TLS_FLAG)
        g.logger.info('Get server type is https:[%s].' % tls)
        if (not remote) and tls.lower() == 'true':
            ca_password, ssl_password, ca_root_file_path, ca_root_key_path, ca_config_path, \
                agent_cert_path, agent_key_path = self.prepare_ca_certificates()
            self.generate_agent_ca_files(
                ca_password, ssl_password, ca_root_file_path, ca_root_key_path,
                ca_config_path, agent_cert_path, agent_key_path)
        if remote and tls.lower() == 'true':
            self.deploy_agent_certs()

        g.logger.info('Start add crontab.')
        self.start_agent_server_monitor()
        g.logger.info('Start kill process.')
        self.try_to_kill_process_exist()

        g.logger.info('Start record version info.')
        self.record_version_info()
        g.logger.info('Waiting for start, the service will start in 1 minute...')
        self.waiting_for_start(Constant.DEFAULT_WAIT_SECONDS)


def init_parser():
    """parser command"""
    parser = optparse.OptionParser(conflict_handler='resolve')
    parser.disable_interspersed_args()
    parser.add_option('--param_file', dest='param_file', help='json file path')
    return parser


if __name__ == '__main__':
    try:
        install_parser = init_parser()
        opt, arg = install_parser.parse_args()
        params_file = opt.param_file
        params_dict = CommonTools.json_file_to_dict(params_file)
        installer = Installer(**params_dict)
        installer.run(remote=True)
    except Exception as error:
        g.logger.error(Errors.EXECUTE_RESULT['gauss_0409'] % error)
        raise Exception(Errors.EXECUTE_RESULT['gauss_0409'] % error)



