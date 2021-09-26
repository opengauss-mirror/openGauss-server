#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : cert_generator.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : ca certificate handler
#############################################################################
import os
from definitions.errors import Errors
from definitions.constants import Constant
from tools.common_tools import CommonTools
from tools.global_box import g


class CertGenerator(object):
    @staticmethod
    def get_rand_str():
        """
        function: get random passwd
        input: NA
        output: passwd
        """
        uppercmd = 'openssl rand -base64 12 | tr "[0-9][a-z]" "[A-Z]" | tr -d [/+=] |cut -c 1-3'
        lowercmd = 'openssl rand -base64 12 | tr "[0-9][A-Z]" "[a-z]" | tr -d [/+=] |cut -c 1-4'
        numcmd = 'openssl rand -base64 12 | md5sum | tr "[a-z]" "[0-9]" |cut -c 1-3'
        strcmd = 'openssl rand -base64 48 | tr "[0-9][a-z][A-Z]" "[~@_#*]" | tr -d [/+=] |cut -c 1-1'

        upper_code, upper_output, upper_error = CommonTools.get_status_output_error(uppercmd)
        lower_code, lower_output, lower_error = CommonTools.get_status_output_error(lowercmd)
        num_code, num_output, num_error = CommonTools.get_status_output_error(numcmd)
        str_code, str_output, str_error = CommonTools.get_status_output_error(strcmd)
        if any([upper_code, lower_code, num_code, str_code]):
            raise Exception(Errors.EXECUTE_RESULT['gauss_0412'] % str(
                [upper_code, lower_code, num_code, str_code]))
        rand_pwd = 'G' + upper_output.strip() + lower_output.strip() + \
                 num_output.strip() + str_output.strip()
        if len(rand_pwd) == Constant.RANDOM_PASSWD_LEN:
            return rand_pwd
        rand_pwd = "G"
        cmd_tuple = (uppercmd, lowercmd, numcmd, strcmd)
        out_tuple = (upper_output.strip(), lower_output.strip(),
                     num_output.strip(), str_output.strip())
        str_len = (3, 4, 3, 1)
        for i in range(4):
            if len(out_tuple[i]) != str_len[i]:
                count = 0
                while True:
                    count += 1
                    _, output, _ = CommonTools.get_status_output_error(cmd_tuple[i])
                    if len(output.strip()) == str_len[i]:
                        rand_pwd += output.strip()
                        break
                    if count > 100:
                        raise Exception(Errors.EXECUTE_RESULT[
                                            'gauss_0413'] % (cmd_tuple[i], 'generate rand pwd'))
            else:
                rand_pwd += out_tuple[i].strip()
        return rand_pwd

    @staticmethod
    def create_root_certificate(ca_password, ca_crt_path, ca_key_path, config_path):
        """
        function : create root ca file
        input : rand pass, dir path of certificates, config path
        output : NA
        """
        if not os.path.isfile(config_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % config_path)
        CommonTools.mkdir_with_mode(os.path.dirname(ca_crt_path), Constant.AUTH_COMMON_DIR_STR)
        CommonTools.mkdir_with_mode(os.path.dirname(ca_key_path), Constant.AUTH_COMMON_DIR_STR)
        ca_req_path = os.path.realpath(os.path.join(
            os.path.dirname(ca_crt_path), Constant.CA_ROOT_REQ))
        # create ca key file
        cmd = "%s echo '%s' |openssl genrsa -aes256  -passout stdin -out %s 2048" % (
            Constant.CMD_PREFIX, ca_password, ca_key_path)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, ca_key_path)
        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca root key file')

        # 2 create ca req file
        cmd = "%s echo '%s' | openssl req -new -out %s -key %s -config %s -passin stdin" % (
            Constant.CMD_PREFIX, ca_password, ca_req_path, ca_key_path, config_path)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, ca_req_path)
        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca root req file')

        # 3 create ca crt file
        cmd = "%s echo '%s' | openssl x509 -req -in %s " \
              "-signkey %s -days %s -out %s -passin stdin" % (
                Constant.CMD_PREFIX, ca_password, ca_req_path,
                ca_key_path, Constant.CA_ROOT_VALID_DATE, ca_crt_path)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, ca_crt_path)
        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca root crt file')

        CommonTools.remove_files([ca_req_path])
        g.logger.info('Successfully generate ca root certificate, file path[%s].' % ca_crt_path)

    @staticmethod
    def create_ca_certificate_with_script(ca_password, ssl_password, ca_crt_path, ca_key_path,
                                          config_path, out_crt_path, out_key_path,
                                          ip, crt_type='server'):
        """
        function : create server ca file or client ca file with shell script.
        input : rand pass, dir path of certificates, config path
        """
        if not os.path.isfile(config_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'config file:%s' % config_path)
        if not os.path.isfile(ca_crt_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'ca crt file:%s' % ca_crt_path)
        if not os.path.isfile(ca_key_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'ca key file:%s' % ca_key_path)
        CommonTools.mkdir_with_mode(os.path.dirname(out_key_path), Constant.AUTH_COMMON_DIR_STR)
        CommonTools.mkdir_with_mode(os.path.dirname(out_crt_path), Constant.AUTH_COMMON_DIR_STR)
        ca_req_path = os.path.realpath(os.path.join(os.path.dirname(ca_crt_path), Constant.CA_REQ))
        pwd = "%s %s" % (ca_password, ssl_password)
        script_path = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'bin/gen_certificate.sh')
        cmd = "unset LD_LIBRARY_PATH && echo '%s' | sh %s %s %s %s %s %s %s %s" % (
            pwd, script_path, ca_crt_path, ca_key_path,
            out_crt_path, out_key_path, ca_req_path, ip, crt_type)

        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca crt file' + output)
        g.logger.info("Successfully generate %s ssl cert for node[%s]." % (crt_type, ip))

    @staticmethod
    def create_ca_certificate(ca_password, ssl_password, ca_crt_path, ca_key_path,
                              config_path, out_crt_path, out_key_path, ip, crt_type='server'):
        """
        function : create server ca file or client ca file.
        input : rand pass, dir path of certificates, config path
        output : NA
        """
        if not os.path.isfile(config_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'config file:%s' % config_path)
        if not os.path.isfile(ca_crt_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'ca crt file:%s' % ca_crt_path)
        if not os.path.isfile(ca_key_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % 'ca key file:%s' % ca_key_path)
        CommonTools.mkdir_with_mode(os.path.dirname(out_key_path), Constant.AUTH_COMMON_DIR_STR)
        CommonTools.mkdir_with_mode(os.path.dirname(out_crt_path), Constant.AUTH_COMMON_DIR_STR)
        # create ca key file
        cmd = "%s echo '%s' | openssl genrsa -aes256 -passout stdin -out %s 2048" % (
            Constant.CMD_PREFIX, ssl_password, out_key_path)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, out_key_path)

        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca key file' + output)

        ca_req_path = os.path.realpath(os.path.join(os.path.dirname(ca_crt_path), Constant.CA_REQ))

        # create ca req file
        openssl_conf_env = "export OPENSSL_CONF=%s" % config_path
        cmd = '%s %s && echo "%s" | openssl req -new -out %s -key %s ' \
              '-passin stdin -subj "/C=CN/ST=Some-State/O=%s/CN=%s"' % (
                Constant.CMD_PREFIX, openssl_conf_env, ssl_password,
                ca_req_path, out_key_path, crt_type, ip)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, ca_req_path)

        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca req file' + output)

        # create server or client ca crt file
        cmd = '%s echo "%s" | openssl x509 -req -in %s -out %s -passin stdin ' \
              '-sha256 -CAcreateserial -days %s -CA %s -CAkey %s' % (
                Constant.CMD_PREFIX, ca_password, ca_req_path, out_crt_path,
                Constant.CA_VALID_DATE, ca_crt_path, ca_key_path)
        cmd += " && %s" % Constant.SHELL_CMD_DICT['changeMode'] % (
            Constant.AUTH_COMMON_FILE_STR, out_crt_path)

        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0414'] % 'ca crt file')
        CommonTools.remove_files([ca_req_path])
        g.logger.info("Successfully generate %s ssl cert for node[%s]." % (crt_type, ip))

    @staticmethod
    def deploy_ca_certs(file_path_list, remote_ip, user, password, dest_dir, no_delete=None):
        """
        Copy files to remote node and remove local files
        """
        CommonTools.remote_mkdir_with_mode(
            dest_dir, Constant.AUTH_COMMON_DIR_STR, remote_ip, user, password)

        for file_path in file_path_list:
            dest_file_path = os.path.join(dest_dir, os.path.basename(file_path))
            status, output = CommonTools.remote_copy_files(
                remote_ip, user, password, file_path, dest_file_path)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0406'] % (remote_ip, output))
            g.logger.debug('Successfully copy [%s] to remote node[%s]' % (file_path, remote_ip))
        if no_delete:
            file_path_list = [file for file in file_path_list if file not in no_delete]
        CommonTools.remove_files(file_path_list)


