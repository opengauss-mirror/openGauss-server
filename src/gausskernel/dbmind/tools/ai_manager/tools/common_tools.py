#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : common_tools.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Common tools
#############################################################################

import json
import re
import threading
import sys
import inspect
import pydoc
import os
import subprocess
import time


from subprocess import PIPE
from configparser import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool

sys.path.append(sys.path[0] + "/../")
from definitions.errors import Errors
from definitions.constants import Constant
from tools.fast_popen import FastPopen
from tools.params_checkers import ConfigChecker


class CommonTools:
    """
    Common tools for ai manager.
    """
    @staticmethod
    def get_version_info_from_file(file_path):
        """
        get version from version conf file in package
        """
        with open(file_path, 'r') as file:
            content = file.readlines()
        if len(content) < 3:
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0501'] % content)
        if re.match(r'^\d*\.\d*$', content[1]):
            version_str = ''.join(content[1].split('.'))
            return version_str.strip()
        else:
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0501'] % content[1])

    @staticmethod
    def extract_file_to_dir(pack_path, target_path):
        """
        Unpack file from pack_path to target_path.
        """
        cmd = Constant.SHELL_CMD_DICT['decompressFileToDir'] % (pack_path, target_path)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'extract file', output))
        else:
            return status, output

    @staticmethod
    def copy_file_to_dest_path(path_from, path_to):
        """
        Copy file to dest path.
        """
        cmd = Constant.SHELL_CMD_DICT['copyFile'] % (path_from, os.path.dirname(path_to))
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'copy files', output))

    @staticmethod
    def check_dir_access(path, mode='rw'):
        """
        Check path authority
        full: read & write & execute
        exist: exist
        rw: read & write
        r: read
        """
        if mode == 'full':
            authority = Constant.AUTHORITY_FULL
        elif mode == 'exist':
            authority = Constant.AUTHORITY_EXIST
        elif mode == 'rw':
            authority = Constant.AUTHORITY_RW
        elif mode == 'r':
            authority = Constant.AUTHORITY_R
        else:
            raise Exception(Errors.PARAMETER['gauss_0201'] % mode)
        results = []
        for check_mode in authority.values():
            results.append(os.access(path, check_mode))
        if not all(results):
            raise Exception(Errors.PERMISSION['gauss_0701'] % path)

    @staticmethod
    def mkdir_with_mode(path, mode):
        """
        Create directory with defined mode if not exist.
        """
        cmd = Constant.SHELL_CMD_DICT['createDir'] % (path, path, mode)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'mkdir', output))
        else:
            return status, output

    @staticmethod
    def remote_mkdir_with_mode(path, mode, ip, username, password):
        """
        Create directory with defined mode if not exist.
        """
        cmd = Constant.SHELL_CMD_DICT['createDirSimple'] % (path, mode)
        status, output = CommonTools.remote_execute_cmd(ip, username, password, cmd)
        if status != 0 and 'exist' not in output:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'remote mkdir', output))
        else:
            return status, output

    @staticmethod
    def clean_dir(path):
        """
        Remove files in path dir.
        """
        cmd = Constant.SHELL_CMD_DICT['cleanDir'] % (path, path, path)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'clean dir', output))
        else:
            return status, output

    @staticmethod
    def retry_remote_clean_dir(path, ip, username, password, retry_times=5):
        """
        Retry to clean dir on remote node if clean failed
        """
        remain_count = retry_times
        while remain_count > 0:
            try:
                status, output = CommonTools.remote_clean_dir(path, ip, username, password)
                return status, output
            except Exception as error:
                if remain_count <= 1:
                    raise Exception(str(error))
                time.sleep(1)
            remain_count -= 1

    @staticmethod
    def remote_clean_dir(path, ip, username, password):
        """
        Remove remote nodes files in path dir
        """
        cmd = Constant.SHELL_CMD_DICT['simpleCleanDir'] % path
        status, output = CommonTools.remote_execute_cmd(ip, username, password, cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'remote clean dir', output))
        cmd_check = Constant.SHELL_CMD_DICT['showDirDocs'] % path
        status, output = CommonTools.remote_execute_cmd(ip, username, password, cmd_check)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'check dir', output))
        num = int(output.strip().split()[-1])
        if num:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0419'] % (path, ip, num))
        return status, output

    @staticmethod
    def remove_files(file_path_list):
        """
        Remove files in file path list.
        """
        cmd_list = [Constant.SHELL_CMD_DICT['deleteFile'] % (
            file_path, file_path) for file_path in file_path_list]
        cmd = ' && '.join(cmd_list)
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'remove file', output))
        else:
            return status, output

    @staticmethod
    def json_file_to_dict(file_path):
        """
        Read json file from file_path.
        return: data in dict.
        """
        if not os.path.isfile(file_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % file_path)
        with open(file_path, 'r') as file:
            content = file.read()
        try:
            dict_data = json.loads(content)
            return dict_data
        except Exception as error:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0402'] % error)

    @staticmethod
    def dict_to_json_file(dict_data, file_path):
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            with os.fdopen(os.open(file_path, os.O_WRONLY | os.O_CREAT, 0o600), "w") as fp_json:
                json.dump(dict_data, fp_json)
        except Exception as error:
            raise ValueError(Errors.EXECUTE_RESULT['gauss_0408'] % error)

    @staticmethod
    def add_cron(install_path, cmd, frequency):
        """
        Add cron
        install_path: install path for getting cron script
        cmd: execute cmd by cron
        frequency: execute frequency[1m]
        """
        cron_path = os.path.realpath(os.path.join(install_path, Constant.CRON_PATH))
        cron_cmd = Constant.SHELL_CMD_DICT['addCronCMD'] % (
            Constant.CMD_PREFIX, cron_path, frequency, cmd)
        status, output = subprocess.getstatusoutput(cron_cmd)
        return status, output

    @staticmethod
    def del_cron(install_path, cmd, frequency):
        """
        Delete cron
        install_path: install path for getting cron script
        cmd: execute cmd by cron
        frequency: execute frequency[1m]
        """
        cron_path = os.path.realpath(os.path.join(install_path, Constant.CRON_PATH))
        cron_cmd = Constant.SHELL_CMD_DICT['delCronCMD'] % (
            Constant.CMD_PREFIX, cron_path, frequency, cmd)
        status, output = subprocess.getstatusoutput(cron_cmd)
        return status, output

    @staticmethod
    def delete_early_record(file_path, max_lines_num):
        """
        Resize file, if content lines reach max_lines_num, delete early record.
        """
        if not os.path.exists(file_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % file_path)
        if isinstance(max_lines_num, str):
            if not max_lines_num.isdigit():
                raise Exception(
                    Errors.CONTENT_OR_VALUE['gauss_0503'] % (max_lines_num, 'max record line'))
        with open(file_path, 'r') as file:
            lines = file.readlines()
            if len(lines) > int(max_lines_num):
                new_lines = lines[-max_lines_num:]
                with open(file_path, 'w') as write:
                    write.writelines(new_lines)

    @staticmethod
    def add_content_to_file(file_path, content):
        """
        add content to file, if not exist, create it.
        """
        with os.fdopen(os.open(
                file_path, os.O_WRONLY | os.O_CREAT, Constant.AUTH_COMMON_FILE), "a") as file:
            file.write(content)

    @staticmethod
    def read_last_line_from_file(file_path):
        """
        Read last line of file
        """
        with open(file_path, 'r') as read_file:
            lines = read_file.readlines()
        lines_no_empty = [line for line in lines if line]
        if lines_no_empty:
            return lines_no_empty[-1]
        else:
            return None

    @staticmethod
    def grep_process_and_kill(target):
        """
        Grep process key words and kill the process.
        """
        cmd = Constant.SHELL_CMD_DICT['killProcess'] % target
        status, out_put = subprocess.getstatusoutput(cmd)
        return status, out_put

    @staticmethod
    def check_is_root():
        if os.getuid() == 0:
            return True
        else:
            return False

    @staticmethod
    def check_process(process):
        """
        Check process number.
        """
        cmd = Constant.SHELL_CMD_DICT['checkProcess'] % process
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'check process', output))
        try:
            output = int(output)
        except ValueError:
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0503'] % (output, 'check process num'))
        return output

    @staticmethod
    def check_path_valid(path):
        """
        function: check path valid
        input : path
        output: NA
        """
        if path.strip() == "":
            return
        for rac in Constant.PATH_CHECK_LIST:
            flag = path.find(rac)
            if flag >= 0:
                raise Exception(Errors.ILLEGAL['gauss_0601'] % (rac, path))

    @staticmethod
    def get_funcs(module):
        """
        Acquire functions in python file.
        :param module: python module.
        :return: dict
        in python module.
        """
        funcs = {}
        _object, _ = pydoc.resolve(module)
        _all = getattr(_object, '__all__', None)
        for key, value in inspect.getmembers(_object, inspect.isroutine):
            if _all is not None or inspect.isbuiltin(value) or inspect.getmodule(value) is _object:
                if pydoc.visiblename(key, _all, _object):
                    funcs[key] = value
        return funcs

    @staticmethod
    def parallel_execute(func, para_list, parallel_jobs=10, is_map=False):
        """
        function: Execution of python functions through multiple processes
        input: func, list
        output: list
        """
        if parallel_jobs > len(para_list):
            parallel_jobs = len(para_list)
        trace_id = threading.currentThread().getName()
        pool = ThreadPool(parallel_jobs, initializer=lambda: threading.currentThread().setName(
            threading.currentThread().getName().replace('Thread', trace_id)))
        results = pool.map(func, para_list)
        pool.close()
        pool.join()
        if is_map:
            result_map = {}
            for result in results:
                result_map[result[0]] = result[1]
            return result_map
        return results

    @staticmethod
    def remote_copy_files(remote_ip, user, password, path_from, path_to):
        """
        Copy files to remote node.
        """
        if os.path.isdir(path_from):
            path_from = os.path.join(path_from, '*')
        script_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), Constant.REMOTE_COMMANDER)
        remote_copy_cmd = Constant.SHELL_CMD_DICT['remoteDeploy'] % (
            password, script_path, path_from, user, remote_ip, path_to)
        status, output = CommonTools.get_status_output_error(remote_copy_cmd, mixed=True)
        return status, output

    @staticmethod
    def remote_execute_cmd(remote_ip, user, password, cmd):
        """
        Execute command on remote node.
        """
        script_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), Constant.REMOTE_COMMANDER)
        remote_execute_cmd = Constant.SHELL_CMD_DICT['remoteExecute'] % (
            password, script_path, user, remote_ip, cmd)
        status, output = CommonTools.get_status_output_error(remote_execute_cmd, mixed=True)
        return status, output

    @staticmethod
    def get_local_ips():
        """
        Get all ips in list
        """
        import socket
        hostname = socket.gethostname()
        addrs = socket.getaddrinfo(hostname, None)
        ips = re.findall(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", str(addrs))
        ips = [ip.strip() for ip in set(ips) if ip]
        if ips:
            return ips
        else:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0415'] % 'get local ips.')

    @staticmethod
    def modify_agent_config_file(config_path, content, allow_add=True):
        """
        Modify agent config file, parse collection item different in each node.
        allow_add: allow add new section and option in config file
        """
        parser = ConfigParser()
        parser.read(config_path)
        ips = CommonTools.get_local_ips()
        for section_name, section_values in content.items():
            for option_name, option_value in section_values.items():
                is_valid = parser.get(section_name, option_name, fallback=None)
                if is_valid is None:
                    if not allow_add:
                        raise Exception(Errors.PARAMETER['gauss_0202'] % option_name)
                if option_name == "collection_item":
                    for item in option_value:
                        json_item = json.dumps(item)
                        if item[1].strip() in ips:
                            parser.set(section_name, option_name, json_item)
                else:
                    parser.set(section_name, option_name, str(option_value))
        with open(config_path, 'w') as file:
            parser.write(file)

    @staticmethod
    def modify_config_file(config_path, content, allow_add=True):
        """
        Modify config file
        allow_add: allowed add new section or option
        content: dict {section1:[opt1:value1,opt2:value2], section2:[...]}
        """
        parser = ConfigParser()
        parser.read(config_path)
        for section_name, section_values in content.items():
            for option_name, option_value in section_values.items():
                is_valid = parser.get(section_name, option_name, fallback=None)
                if is_valid is None:
                    if not allow_add:
                        raise Exception(Errors.PARAMETER['gauss_0202'] % option_name)
                parser.set(section_name, option_name, str(option_value))
        with open(config_path, 'w') as file:
            parser.write(file)

    @staticmethod
    def get_status_output_error(cmd, mixed=False):
        """
        Execute command and return in defined results.
        """
        proc = FastPopen(cmd, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid, close_fds=True)
        stdout, stderr = proc.communicate()
        if mixed:
            return proc.returncode, stdout.decode() + stderr.decode()
        else:
            return proc.returncode, stdout.decode(), stderr.decode()

    @staticmethod
    def get_current_usr():
        """
        Get current user by echo $USER
        """
        cmd = Constant.SHELL_CMD_DICT['getUser']
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0 or not output:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (cmd, 'get user', output))
        else:
            return output.strip()

    @staticmethod
    def create_file_if_not_exist(path, mode=Constant.AUTH_COMMON_FILE):
        """
        Create file if not exist.
        """
        if not os.path.exists(path):
            os.mknod(path, mode=mode)
            return True
        else:
            return False

    @staticmethod
    def read_info_from_config_file(file_path, section, option, under_path=None):
        """
        Get config info from file_path
        """
        parser = ConfigParser()
        parser.read(file_path)
        info = parser.get(section, option, fallback=None)
        if info is None:
            raise Exception(Errors.PARAMETER['gauss_0202'] % option)
        else:
            ConfigChecker.check(section, option, info)
            if under_path:
                if not os.path.isabs(under_path):
                    raise Exception(Errors.PARAMETER['gauss_0104'] % under_path)
                base_path = re.sub(r'\./', '', info)
                return os.path.join(under_path, base_path)
        return info

    @staticmethod
    def get_local_ip(ignore=False):
        cmd = Constant.SHELL_CMD_DICT['ifconfig']
        status, output = subprocess.getstatusoutput(cmd)
        if status != 0 or not output:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0413'] % (cmd, 'get local ip'))
        ret = re.search(r'eth0: .*?inet (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', output,
                        flags=re.DOTALL)
        if ret:
            return ret.group(1)
        else:
            if ignore:
                return '127.0.0.1'
            else:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0415'] % 'local ip')

    @staticmethod
    def encrypt_with_path(password, path, encrypt_path, lib_path):
        if not os.path.isfile(encrypt_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0102'] % encrypt_path)
        if not os.path.isdir(lib_path):
            raise Exception(Errors.FILE_DIR_PATH['gauss_0101'] % lib_path)
        CommonTools.chmod_files_with_execute_permission([encrypt_path])
        cmd = 'export LD_LIBRARY_PATH=%s && ' % lib_path
        path = path + '/' if not path.endswith('/') else path
        cmd += '%s %s %s %s' % (Constant.CMD_PREFIX, encrypt_path, password, path)
        cmd += ' && chmod -R %s %s' % (Constant.AUTH_COMMON_ENCRYPT_FILES, path)
        status, output = CommonTools.get_status_output_error(cmd, mixed=True)
        if status != 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0417'] + output)
        return status, output

    @staticmethod
    def chmod_files_with_execute_permission(file_list):
        for file in file_list:
            cmd = Constant.SHELL_CMD_DICT["chmodWithExecute"] % os.path.abspath(file)
            status, output = CommonTools.get_status_output_error(cmd, mixed=True)
            if status != 0:
                raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (
                    cmd, 'change file authority', output))




