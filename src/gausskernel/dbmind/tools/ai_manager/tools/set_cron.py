#!/usr/bin/env python3
# -*- coding:utf-8 -*-

#############################################################################
# Copyright (c): 2012-2021, Huawei Tech. Co., Ltd.
# FileName     : set_cron.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : Contab tools
#############################################################################

import os
import optparse
import re
import sys
import stat
import subprocess
sys.path.append(sys.path[0] + "/../")
from definitions.constants import Constant
from definitions.errors import Errors
from config_cabin.config import TMP_DIR
from tools.common_tools import CommonTools
from tools.log import MainLog

flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
modes = stat.S_IWUSR | stat.S_IRUSR


class Cron:
    def __init__(self, task, cycle, cmd):
        self.task = task
        self.cycle = cycle
        self.cmd = cmd
        self.curr_path = os.path.dirname(os.path.realpath(__file__))
        self.tmp_file = os.path.join(self.curr_path, "cron_tmp_%s" % os.getpid())
        self.logger = MainLog(Constant.CRON_LOG_NAME).get_logger()

    def get_lock_file(self):
        """
        Get lock file path for cron service.
        """
        lock_file_dir = TMP_DIR
        CommonTools.mkdir_with_mode(lock_file_dir, Constant.AUTH_COMMON_DIR_STR)
        lock_file_name = ''
        for name in Constant.TASK_NAME_LIST:
            if name in self.cmd.split('role')[-1]:
                lock_file_name = 'ai_' + name + '.lock'
        if not lock_file_name:
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0502'] % self.cmd)
        else:
            return os.path.join(lock_file_dir, lock_file_name)

    @staticmethod
    def exe_cmd(command, ignore_error=False):
        """
        Execute command in string or list.
        """
        if isinstance(command, list):
            command = ' '.join(command)
        status, output = subprocess.getstatusoutput(command)
        if status != 0 and not ignore_error:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0401'] % (command, 'prepare cron', output))
        return status, output

    def get_old_cron(self):
        """
        Get old cron string by command of crontab -l
        """
        get_cron_cmd = ["crontab", "-l"]
        status, result = self.exe_cmd(get_cron_cmd, ignore_error=True)
        if status != 0 and result.find(Constant.CRON_INFO_EXPECTED) < 0:
            raise Exception(Errors.EXECUTE_RESULT['gauss_0404'] % result)
        if result.find(Constant.CRON_INFO_EXPECTED) >= 0:
            return ""
        return result.strip()

    def del_cron(self):
        """
        Delete old cron by resetting a new cron file
        """
        self.logger.debug("Deleting old cron...")
        crontab_str = self.get_old_cron()
        crontab_list = crontab_str.split('\n')
        crontab_list_new = []
        clean_cmd = self.cmd
        versions = re.findall(r'\d{5}/', self.cmd)
        version = versions[0] if versions else None
        if version:
            clean_cmd = clean_cmd.split(version)[-1]
        self.logger.debug('Clean cron with [%s]' % clean_cmd)

        for old_cron in crontab_list:
            if clean_cmd not in old_cron:
                crontab_list_new.append(old_cron)
        cron_str = "\n".join(crontab_list_new) + "\n"
        with os.fdopen(os.open(self.tmp_file, flags, modes), "w") as fp:
            fp.write(cron_str)
        del_cron_cmd = ["crontab", self.tmp_file]
        status, output = self.exe_cmd(del_cron_cmd)
        if os.path.isfile(self.tmp_file):
            os.remove(self.tmp_file)
        self.logger.info("Delete cron result:status[%s]-output[%s]." % (
            status or Constant.SUCCESS, output))

    def add_new_cron(self):
        """
        Add new cron with lock
        """
        # Supplement newline
        self.logger.debug("Adding new cron...")
        old_cron = self.get_old_cron()
        if old_cron and not old_cron.endswith("\n"):
            old_cron += "\n"
        # Check cycle info
        cycle = self.cycle[:-1]
        unit = self.cycle[-1]
        if not cycle.isdigit():
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0503'] % (cycle, 'cron time setting'))
        if unit not in ["m", "h", "d", "M", "w"]:
            raise Exception(Errors.CONTENT_OR_VALUE['gauss_0503'] % (unit, 'cron time unit'))

        # set cycle string
        new_cron = ""
        if unit == "m":
            new_cron = "*/%s * * * * " % cycle
        elif unit == "h":
            new_cron = "* */%s * * * " % cycle
        elif unit == "d":
            new_cron = "* * */%s * * " % cycle
        elif unit == "M":
            new_cron = "* * * */%s * " % cycle
        elif unit == "w":
            new_cron = "* * * * */%s " % cycle
        lock_file = self.get_lock_file()
        if lock_file != "":
            new_cron += "flock -nx %s -c '%s' >> /dev/null 2>&1 &\n" \
                        % (lock_file, self.cmd)
        else:
            new_cron += "%s >> /dev/null 2>&1 &\n" % self.cmd
        self.logger.debug("new cron is : %s" % new_cron)

        all_cron = old_cron + new_cron
        # Write cron file
        with os.fdopen(os.open(self.tmp_file, flags, modes), "w") as fp:
            fp.write(all_cron)
        add_cron_cmd = ["crontab", self.tmp_file]
        status, output = self.exe_cmd(add_cron_cmd)
        if os.path.exists(self.tmp_file):
            os.remove(self.tmp_file)
        self.logger.info("Add cron result:status[%s]-output[%s]." % (
            status or Constant.SUCCESS, output))


if __name__ == "__main__":
    parser = optparse.OptionParser(conflict_handler='resolve')
    parser.disable_interspersed_args()
    parser.usage = "%prog -t [add|del] --cycle interval command"
    parser.epilog = "Example: set_cron.py -t add --cycle 1d 'reboot'"
    parser.add_option('-t', dest='task', help='Specify add or delete tasks')
    parser.add_option('-c', "--cycle", dest='cycle',
                      help='Specify the execution interval of the task, '
                           'unit: m:minute; h:hour; d:day; M:month; w:week')
    opts, args = parser.parse_args()
    cron = Cron(opts.task, opts.cycle, " ".join(args))
    try:
        if cron.task == "add":
            cron.del_cron()
            cron.add_new_cron()
        elif cron.task == "del":
            cron.del_cron()
        else:
            raise Exception(Errors.PARAMETER['gauss_0201'] % '-t')

    except Exception as e:
        if os.path.isfile(cron.tmp_file):
            os.remove(cron.tmp_file)
        raise Exception(str(e))
