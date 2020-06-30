# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms
# and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS,
# WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#############################################################################
import sys


class CheckException(Exception):
    def __init__(self, content):
        self.code = "GAUSS-53000"
        self.content = content

    def __str__(self):
        return "[%s]: ERROR: " % self.code + self.content


class ParameterException(CheckException):
    def __init__(self, content):
        self.code = "GAUSS-53012"
        self.content = "Errors occurred when parsing parameters: %s." % content


class UnknownParameterException(CheckException):
    def __init__(self, param):
        self.code = "GAUSS-53013"
        self.content = "Unknown parameters were set: %s." % param


class EmptyParameterException(CheckException):
    def __init__(self):
        self.code = "GAUSS-53014"
        self.content = "The parameters cannot be empty."


class UseBothParameterException(CheckException):
    def __init__(self, params):
        self.code = "GAUSS-53015"
        self.content = \
            " The parameter '-%s' and '-%s' can not be used together." % (
                params[0], params[1])


class AvailableParameterException(CheckException):
    def __init__(self, parent, subs):
        self.code = "GAUSS-53016"
        self.content = " The parameter '%s' were not available for '%s'." % (
            ",".join(subs), parent)


class SceneNotFoundException(CheckException):
    def __init__(self, scene, supportScenes):
        self.code = "GAUSS-53017"
        self.content = \
            "The scene %s and its configuaration file scene_%s.xml " \
            "were not found in config folder." % (
                scene, scene) + "\nThe support scenes is: [%s]" % ",".join(
                supportScenes)


class ParseItemException(CheckException):
    def __init__(self, items):
        self.code = "GAUSS-53017"
        self.content = \
            "There were errors when parsing these items: %s." % ",".join(
                items) + \
            " maybe items name is incorrect."


class NotEmptyException(CheckException):
    def __init__(self, elem, detail=""):
        self.code = "GAUSS-53018"
        self.content = "The %s cannot be empty. %s" % (elem, detail)


class NotExistException(CheckException):
    def __init__(self, elem, List):
        self.code = "GAUSS-53019"
        self.content = "The %s does not exist in %s." % (elem, List)


class InterruptException(CheckException):
    def __init__(self):
        self.code = "GAUSS-53020"
        self.content = \
            "The checking process was interrupted by user with Ctrl+C command"


class TrustException(CheckException):
    def __init__(self, hosts):
        self.code = "GAUSS-53021"
        self.content = "Faild to verified SSH trust on hosts: %s" % hosts


class ShellCommandException(CheckException):
    def __init__(self, cmd, output):
        self.code = "GAUSS-53025"
        self.cmd = cmd
        self.output = output
        self.content = \
            "Execute Shell command faild: %s , the exception is: %s" % (
                self.cmd, self.output)


class SshCommandException(CheckException):
    def __init__(self, host, cmd, output):
        self.code = "GAUSS-53026"
        self.cmd = cmd
        self.host = host
        self.output = output
        self.content = \
            "Execute SSH command on host %s faild. The exception is: %s" % (
                self.host, self.output)


class SQLCommandException(CheckException):
    def __init__(self, sql, output):
        self.code = "GAUSS-53027"
        self.sql = sql
        self.output = output
        self.content = \
            "Execute SQL command faild: %s , the exception is: %s" % (
                self.sql, self.output)


class TimeoutException(CheckException):
    def __init__(self, nodes):
        self.code = "GAUSS-53028"
        self.content = "The node[%s] execute timeout." % ",".join(nodes)


class ThreadCheckException(CheckException):
    def __init__(self, thread, exception):
        self.code = "GAUSS-53020"
        if (isinstance(exception, ShellCommandException)
                or isinstance(exception, SQLCommandException)
                or isinstance(exception, SshCommandException)):
            output = exception.output
        elif (isinstance(exception, TimeoutException)):
            output = exception.content
        elif (isinstance(exception, CheckException)):
            output = exception.content
        else:
            output = str(exception)
        self.content = \
            "The thread %s running checking item but occurs errors: %s" % (
                thread, output)


class ContextDumpException(CheckException):
    def __init__(self, errors):
        self.code = "GAUSS-53030"
        self.content = "Dumping context has errors: %s." % str(errors)


class ContextLoadException(CheckException):
    def __init__(self, errors):
        self.code = "GAUSS-53031"
        self.content = "Loading context has errors: %s." % str(errors)


class CheckErrorException(CheckException):
    def __init__(self):
        self.code = "GAUSS-53032"
        self.content = "An internal error occurred during the checking process"


class CheckNAException(CheckException):
    def __init__(self, item):
        self.code = "GAUSS-53033"
        self.content = \
            "Check item %s are not needed at the current node" % item
