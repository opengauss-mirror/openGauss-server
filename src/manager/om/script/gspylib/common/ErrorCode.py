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
# ----------------------------------------------------------------------------
# Description  : ErrorCode.py is utility to register the error message
#############################################################################
import re
import sys


class ErrorCode():
    """
    Class to define output about the error message
    """

    def __init__(self):
        pass

    @staticmethod
    def getErrorCodeAsInt(ex, default_error_code):
        """
        Resolve the exit code from the exception instance or error message.

        In linux, the range of return values is between 0 and 255.
        So we can only use each type of error code as exit code.Such as:
            ErrorCode.GAUSS_500 : 10
            ErrorCode.GAUSS_501 : 11

        :param ex:                  Exception instance or error message
        :param default_error_code:  If the exception instance does not contain
        the exit code, use this parameter.

        :type ex:                   Exception | str
        :type default_error_code:   int

        :return:    Return the error code.
            9 represents undefined exit code.
            other number between 0 and 255 represent the specific gauss error.
        :type:      int
        """
        error_message = str(ex)
        pattern = r"^[\S\s]*\[GAUSS-(\d+)\][\S\s]+$"
        match = re.match(pattern, error_message)

        if match is not None and len(match.groups()) == 1:
            error_code = int(match.groups()[0])
        else:
            error_code = default_error_code

        if 50000 < error_code < 60000:
            return error_code // 100 - 500 + 10
        else:
            return 9

    ###########################################################################
    # parameter
    ###########################################################################
    GAUSS_500 = {
        'GAUSS_50000': "[GAUSS-50000] : Unrecognized parameter: %s.",
        'GAUSS_50001': "[GAUSS-50001] : Incorrect parameter. Parameter "
                       "'-%s' is required",
        'GAUSS_50002': "[GAUSS-50002] : Incorrect parameter. Parameter "
                       "'-%s' is not required",
        'GAUSS_50003': "[GAUSS-50003] : The parameter '-%s' type should be "
                       "%s.",
        'GAUSS_50004': "[GAUSS-50004] : The parameter '-%s' value is "
                       "incorrect.",
        'GAUSS_50005': "[GAUSS-50005] : The parameter '-%s' and '-%s' "
                       "can not be used together.",
        'GAUSS_50006': "[GAUSS-50006] : Too many command-line arguments "
                       "(first is \"%s\").",
        'GAUSS_50007': "[GAUSS-50007] : Failed to set %s parameter.",
        'GAUSS_50008': "[GAUSS-50008] : Failed to reload parameter.",
        'GAUSS_50009': "[GAUSS-50009] : Parameter format error.",
        'GAUSS_50010': "[GAUSS-50010] : Failed to check %s parameter.",
        'GAUSS_50011': "[GAUSS-50011] : The parameter[%s] value[%s] "
                       "is invalid.",
        'GAUSS_50012': "[GAUSS-50012] : The parameter '%s' value can't "
                       "be empty.",
        'GAUSS_50013': "[GAUSS-50013] : The parameter '%s' have not "
                       "been initialized.",
        'GAUSS_50014': "[GAUSS-50014] : Parameters of '%s' can not be empty.",
        'GAUSS_50015': "[GAUSS-50015] : The command line parser error: %s.",
        'GAUSS_50016': "[GAUSS-50016] : The re-entrant parameter '-%s' "
                       "is not same with the previous command.",
        'GAUSS_50017': "[GAUSS-50017] : Incorrect value '%s' specified "
                       "by the parameter '-%s'.",
        'GAUSS_50018': "[GAUSS-50018] : The parameter value of %s is Null.",
        'GAUSS_50019': "[GAUSS-50019] : The value of %s is error.",
        'GAUSS_50020': "[GAUSS-50020] : The value of %s must be a digit."

    }

    ###########################################################################
    # permission
    ###########################################################################
    GAUSS_501 = {
        'GAUSS_50100': "[GAUSS-50100] : The %s is not readable for %s.",
        'GAUSS_50101': "[GAUSS-50101] : The %s is not executable for %s.",
        'GAUSS_50102': "[GAUSS-50102] : The %s is not writable for %s.",
        'GAUSS_50103': "[GAUSS-50103] : The %s has unexpected rights.",
        'GAUSS_50104': "[GAUSS-50104] : Only a user with the root permission "
                       "can run this script.",
        'GAUSS_50105': "[GAUSS-50105] : Cannot run this script as a user "
                       "with the root permission.",
        'GAUSS_50106': "[GAUSS-50106] : Failed to change the owner of %s.",
        'GAUSS_50107': "[GAUSS-50107] : Failed to change the "
                       "permission of %s.",
        'GAUSS_50108': "[GAUSS-50108] : Failed to change the owner and "
                       "permission of %s.",
        'GAUSS_50109': "[GAUSS-50109] : Only a user with the root permission "
                       "can check SSD information.",
        'GAUSS_50110': "[GAUSS-50110] : Cannot execute this script on %s."
    }

    ###########################################################################
    # file or directory
    ###########################################################################
    GAUSS_502 = {
        'GAUSS_50200': "[GAUSS-50200] : The %s already exists.",
        'GAUSS_50201': "[GAUSS-50201] : The %s does not exist.",
        'GAUSS_50202': "[GAUSS-50202] : The %s must be empty.",
        'GAUSS_50203': "[GAUSS-50203] : The %s cannot be empty.",
        'GAUSS_50204': "[GAUSS-50204] : Failed to read %s.",
        'GAUSS_50205': "[GAUSS-50205] : Failed to write %s.",
        'GAUSS_50206': "[GAUSS-50206] : Failed to create %s.",
        'GAUSS_50207': "[GAUSS-50207] : Failed to delete %s.",
        'GAUSS_50208': "[GAUSS-50208] : Failed to create the %s directory.",
        'GAUSS_50209': "[GAUSS-50209] : Failed to delete the %s directory.",
        'GAUSS_50210': "[GAUSS-50210] : The %s must be a file.",
        'GAUSS_50211': "[GAUSS-50211] : The %s must be a directory.",
        'GAUSS_50212': "[GAUSS-50212] : The suffix of the file [%s] "
                       "should be '%s'.",
        'GAUSS_50213': "[GAUSS-50213] : The %s path must be an absolute path.",
        'GAUSS_50214': "[GAUSS-50214] : Failed to copy %s.",
        'GAUSS_50215': "[GAUSS-50215] : Failed to back up %s.",
        'GAUSS_50216': "[GAUSS-50216] : Failed to remote copy %s.",
        'GAUSS_50217': "[GAUSS-50217] : Failed to decompress %s.",
        'GAUSS_50218': "[GAUSS-50218] : Failed to rename %s.",
        'GAUSS_50219': "[GAUSS-50219] : Failed to obtain %s.",
        'GAUSS_50220': "[GAUSS-50220] : Failed to restore %s.",
        'GAUSS_50221': "[GAUSS-50221] : Failed to obtain file type.",
        'GAUSS_50222': "[GAUSS-50222] : The content of file %s is not "
                       "correct.",
        'GAUSS_50223': "[GAUSS-50223] : Failed to update %s files.",
        'GAUSS_50224': "[GAUSS-50224] : The file name is incorrect.",
        'GAUSS_50225': "[GAUSS-50225] : Failed to back up remotely.",
        'GAUSS_50226': "[GAUSS-50226] : Failed to restore remotely.",
        'GAUSS_50227': "[GAUSS-50227] : Failed to compress %s.",
        'GAUSS_50228': "[GAUSS-50228] : The %s does not exist or is empty.",
        'GAUSS_50229': "[GAUSS-50229] : Cannot specify the file [%s] to "
                       "the cluster path %s.",
        'GAUSS_50230': "[GAUSS-50230] : Failed to read/write %s.",
        'GAUSS_50231': "[GAUSS-50231] : Failed to generate %s file.",
        'GAUSS_50232': "[GAUSS-50232] : The instance directory [%s] "
                       "cannot set in app directory [%s].Please check "
                       "the xml.",
        'GAUSS_50233': "[GAUSS-50233] : The directory name %s and %s "
                       "cannot be same.",
        'GAUSS_50234': "[GAUSS-50234] : Cannot execute the script in "
                       "the relevant path of the database.",
        'GAUSS_50235': "[GAUSS-50235] : The log file name [%s] can not contain"
                       " more than one '.'.",
        'GAUSS_50236': "[GAUSS-50236] : %s should be set in scene config "
                       "file.",
        'GAUSS_50237': "[GAUSS-50237] : Send result file failed nodes: %s;"
                       " outputMap: %s",
        'GAUSS_50238': "[GAUSS-50238] : Check integrality of bin "
                       "file %s failed."

    }

    ###########################################################################
    # user and group
    ###########################################################################
    GAUSS_503 = {
        'GAUSS_50300': "[GAUSS-50300] : User %s does not exist.",
        'GAUSS_50301': "[GAUSS-50301] : The cluster user/group cannot "
                       "be a root user/group.",
        'GAUSS_50302': "[GAUSS-50302] : The cluster user cannot be a user "
                       "with the root permission.",
        'GAUSS_50303': "[GAUSS-50303] : Cannot install the program as a "
                       "user with the root permission.",
        'GAUSS_50304': "[GAUSS-50304] : The new user [%s] is not the same "
                       "as the old user [%s].",
        'GAUSS_50305': "[GAUSS-50305] : The user is not matched with the "
                       "user group.",
        'GAUSS_50306': "[GAUSS-50306] : The password of %s is incorrect.",
        'GAUSS_50307': "[GAUSS-50307] : User password has expired.",
        'GAUSS_50308': "[GAUSS-50308] : Failed to obtain user information.",
        'GAUSS_50309': "[GAUSS-50309] : Failed to obtain password "
                       "change times of data base super user",
        'GAUSS_50310': "[GAUSS-50310] : Failed to obtain password "
                       "expiring days.",
        'GAUSS_50311': "[GAUSS-50311] : Failed to change password for %s.",
        'GAUSS_50312': "[GAUSS-50312] : There are other users in the group %s "
                       "on %s, skip to delete group.",
        'GAUSS_50313': "[GAUSS-50313] : Failed to delete %s group.",
        'GAUSS_50314': "[GAUSS-50314] : Failed to delete %s user.",
        'GAUSS_50315': "[GAUSS-50315] : The user %s is not matched with the "
                       "owner of %s.",
        'GAUSS_50316': "[GAUSS-50316] : Group [%s] does not exist.",
        'GAUSS_50317': "[GAUSS-50317] : Failed to check user and password.",
        'GAUSS_50318': "[GAUSS-50318] : Failed to add %s user.",
        'GAUSS_50319': "[GAUSS-50319] : Failed to add %s group.",
        'GAUSS_50320': "[GAUSS-50320] : Failed to set '%s' to '%s' in "
                       "/etc/ssh/sshd_config.",
        'GAUSS_50321': "[GAUSS-50321] : Failed to get configuration of '%s' "
                       "from /etc/ssh/sshd_config.",
        'GAUSS_50322': "[GAUSS-50322] : Failed to encrypt the password for %s",
        'GAUSS_50323': "[GAUSS-50323] : The user %s is not the cluster "
                       "installation user "
    }

    ###########################################################################
    # disk
    ###########################################################################
    GAUSS_504 = {
        'GAUSS_50400': "[GAUSS-50400] : The remaining space of device [%s] "
                       "cannot be less than %s.",
        'GAUSS_50401': "[GAUSS-50401] : The usage of the device [%s] space "
                       "cannot be greater than %s.",
        'GAUSS_50402': "[GAUSS-50402] : The usage of INODE cannot be greater "
                       "than %s.",
        'GAUSS_50403': "[GAUSS-50403] : The IO scheduling policy is "
                       "incorrect.",
        'GAUSS_50404': "[GAUSS-50404] : The XFS mount type must be %s.",
        'GAUSS_50405': "[GAUSS-50405] : The pre-read block size must "
                       "be 16384.",
        'GAUSS_50406': "[GAUSS-50406] : Failed to obtain disk read and "
                       "write rates.",
        'GAUSS_50407': "[GAUSS-50407] : Failed to clean shared semaphore.",
        'GAUSS_50408': "[GAUSS-50408] : Failed to obtain disk read-ahead "
                       "memory block.",
        'GAUSS_50409': "[GAUSS-50409] : The remaining space of dns cannot "
                       "support shrink.",
        'GAUSS_50410': "[GAUSS-50410] : Failed to check if remaining space "
                       "of dns support shrink.",
        'GAUSS_50411': "[GAUSS-50411] : The remaining space cannot be less "
                       "than %s.",
        'GAUSS_50412': "[GAUSS-50412] : Failed to get disk space of database "
                       "node %s.",
        'GAUSS_50413': "[GAUSS-50413] : Failed to analysis"
                       " the disk information."

    }

    ###########################################################################
    # memory
    ###########################################################################
    GAUSS_505 = {
        'GAUSS_50500': "[GAUSS-50500] : The SWAP partition is smaller than "
                       "the actual memory.",
        'GAUSS_50501': "[GAUSS-50501] : Shared_buffers must be less than "
                       "shmmax. Please check it.",
        'GAUSS_50502': "[GAUSS-50502] : Failed to obtain %s information."
    }

    ###########################################################################
    # network
    ###########################################################################
    GAUSS_506 = {
        'GAUSS_50600': "[GAUSS-50600] : The IP address cannot be pinged, "
                       "which is caused by network faults.",
        'GAUSS_50601': "[GAUSS-50601] : The port [%s] is occupied or the ip "
                       "address is incorrectly configured.",
        'GAUSS_50602': "[GAUSS-50602] : Failed to bind network adapters.",
        'GAUSS_50603': "[GAUSS-50603] : The IP address is invalid.",
        'GAUSS_50604': "[GAUSS-50604] : Failed to obtain network interface "
                       "card of backIp(%s).",
        'GAUSS_50605': "[GAUSS-50605] : Failed to obtain back IP subnet mask.",
        'GAUSS_50606': "[GAUSS-50606] : Back IP(s) do not have the same "
                       "subnet mask.",
        'GAUSS_50607': "[GAUSS-50607] : Failed to obtain configuring virtual "
                       "IP line number position of network startup file.",
        'GAUSS_50608': "[GAUSS-50608] : Failed to writing virtual IP setting "
                       "cmds into init file.",
        'GAUSS_50609': "[GAUSS-50609] : Failed to check port: %s.",
        'GAUSS_50610': "[GAUSS-50610] : Failed to get the range of "
                       "random port.",
        'GAUSS_50611': "[GAUSS-50611] : Failed to obtain network card "
                       "bonding information.",
        'GAUSS_50612': "[GAUSS-50612] : Failed to obtain network card %s "
                       "value.",
        'GAUSS_50613': "[GAUSS-50613] : Failed to set network card %s value.",
        'GAUSS_50614': "[GAUSS-50614] : Failed to check network information.",
        'GAUSS_50615': "[GAUSS-50615] : IP %s and IP %s are not in the "
                       "same network segment.",
        'GAUSS_50616': "[GAUSS-50616] : Failed to get network interface.",
        'GAUSS_50617': "[GAUSS-50617] : The node of XML configure file "
                       "has the same virtual IP.",
        'GAUSS_50618': "[GAUSS-50618] : %s. The startup file for SUSE OS"
                       " is /etc/init.d/boot.local. The startup file for "
                       "Redhat OS is /etc/rc.d/rc.local.",
        'GAUSS_50619': "[GAUSS-50619] : Failed to obtain network"
                       " card information.",
        'GAUSS_50620': "[GAUSS-50620] : Failed to check network"
                       " RX drop percentage.",
        'GAUSS_50621': "[GAUSS-50621] : Failed to check network care speed.\n",
        'GAUSS_50622': "[GAUSS-50622] : Failed to obtain network card "
                       "interrupt count numbers. Commands for getting "
                       "interrupt count numbers: %s."

    }

    ###########################################################################
    # firwall
    ###########################################################################
    GAUSS_507 = {
        'GAUSS_50700': "[GAUSS-50700] : The firewall should be disabled.",
        'GAUSS_50701': "[GAUSS-50701] : The firewall should be opened."
    }

    ###########################################################################
    # crontab
    ###########################################################################
    GAUSS_508 = {
        'GAUSS_50800': "[GAUSS-50800] : Regular tasks are not started.",
        'GAUSS_50801': "[GAUSS-50801] : Failed to set up tasks.",
        'GAUSS_50802': "[GAUSS-50802] : Failed to %s service.",
        'GAUSS_50803': "[GAUSS-50803] : Failed to check user cron."
    }

    ###########################################################################
    # Clock service
    ###########################################################################
    GAUSS_509 = {
        'GAUSS_50900': "[GAUSS-50900] : The NTPD service is not installed.",
        'GAUSS_50901': "[GAUSS-50901] : The NTPD service is not started.",
        'GAUSS_50902': "[GAUSS-50902] : The system time is different."
    }

    ###########################################################################
    # THP
    ###########################################################################
    GAUSS_510 = {
        'GAUSS_51000': "[GAUSS-51000] : THP services must be shut down.",
        'GAUSS_51001': "[GAUSS-51001] : Failed to obtain THP service.",
        'GAUSS_51002': "[GAUSS-51002] : Failed to close THP service.",
        'GAUSS_51003': "[GAUSS-51003] : Failed to set session process."
    }

    ###########################################################################
    # SSH trust
    ###########################################################################
    GAUSS_511 = {
        'GAUSS_51100': "[GAUSS-51100] : Failed to verify SSH trust on "
                       "these nodes: %s.",
        'GAUSS_51101': "[GAUSS-51101] : SSH exception: \n%s",
        'GAUSS_51102': "[GAUSS-51102] : Failed to exchange SSH keys "
                       "for user [%s] performing the %s operation.",
        'GAUSS_51103': "[GAUSS-51103] : Failed to execute the PSSH "
                       "command [%s].",
        'GAUSS_51104': "[GAUSS-51104] : Failed to obtain SSH status.",
        'GAUSS_51105': "[GAUSS-51105] : Failed to parse SSH output: %s.",
        'GAUSS_51106': "[GAUSS-51106] : The SSH tool does not exist.",
        'GAUSS_51107': "[GAUSS-51107] : Ssh Paramiko failed.",
        'GAUSS_51108': "[GAUSS-51108] : Ssh-keygen failed.",
        'GAUSS_51109': "[GAUSS-51109] : Failed to check authentication.",
        'GAUSS_51110': "[GAUSS-51110] : Failed to obtain RSA host key "
                       "for local host.",
        'GAUSS_51111': "[GAUSS-51111] : Failed to append local ID to "
                       "authorized_keys on remote node.",
        'GAUSS_51112': "[GAUSS-51112] : Failed to exchange SSH keys "
                       "for user[%s] using hostname."
    }

    ###########################################################################
    # cluster/XML configruation
    ###########################################################################
    GAUSS_512 = {
        'GAUSS_51200': "[GAUSS-51200] : The parameter [%s] in the XML "
                       "file does not exist.",
        'GAUSS_51201': "[GAUSS-51201] : Node names must be configured.",
        'GAUSS_51202': "[GAUSS-51202] : Failed to add the %s instance.",
        'GAUSS_51203': "[GAUSS-51203] : Failed to obtain the %s "
                       "information from static configuration files.",
        'GAUSS_51204': "[GAUSS-51204] : Invalid %s instance type: %d.",
        'GAUSS_51205': "[GAUSS-51205] : Failed to refresh the %s instance ID.",
        'GAUSS_51206': "[GAUSS-51206] : The MPPRC file path must "
                       "be an absolute path: %s.",
        'GAUSS_51207': "[GAUSS-51207] : Failed to obtain backIp "
                       "from node [%s].",
        'GAUSS_51208': "[GAUSS-51208] : Invalid %s number [%d].",
        'GAUSS_51209': "[GAUSS-51209] : Failed to obtain %s "
                       "configuration on the host [%s].",
        'GAUSS_51210': "[GAUSS-51210] : The obtained number does "
                       "not match the instance number.",
        'GAUSS_51211': "[GAUSS-51211] : Failed to save a static "
                       "configuration file.",
        'GAUSS_51212': "[GAUSS-51212] : There is no information about %s.",
        'GAUSS_51213': "[GAUSS-51213] : The port number of XML [%s] "
                       "conflicted.",
        'GAUSS_51214': "[GAUSS-51214] : The number of capacity expansion "
                       "database nodes cannot be less than three",
        'GAUSS_51215': "[GAUSS-51215] : The capacity expansion node [%s] "
                       "cannot contain GTM/CM/ETCD.",
        'GAUSS_51216': "[GAUSS-51216] : The capacity expansion node [%s] "
                       "must contain CN or DN.",
        'GAUSS_51217': "[GAUSS-51217] : The cluster's static configuration "
                       "does not match the new configuration file.",
        'GAUSS_51218': "[GAUSS-51218] : Failed to obtain initialized "
                       "configuration parameter: %s.",
        'GAUSS_51219': "[GAUSS-51219] : There is no CN in cluster.",
        'GAUSS_51220': "[GAUSS-51220] : The IP address %s is incorrect.",
        'GAUSS_51221': "[GAUSS-51221] : Failed to configure hosts "
                       "mapping information.",
        'GAUSS_51222': "[GAUSS-51222] : Failed to check hostname mapping.",
        'GAUSS_51223': "[GAUSS-51223] : Failed to obtain network "
                       "inet addr on the node(%s).",
        'GAUSS_51224': "[GAUSS-51224] : The ip(%s) has been used "
                       "on other nodes.",
        'GAUSS_51225': "[GAUSS-51225] : Failed to set virtual IP.",
        'GAUSS_51226': "[GAUSS-51226] : Virtual IP(s) and Back IP(s) "
                       "do not have the same network segment.",
        'GAUSS_51227': "[GAUSS-51227] : The number of %s on all nodes "
                       "are different.",
        'GAUSS_51228': "[GAUSS-51228] : The number %s does not "
                       "match %s number.",
        'GAUSS_51229': "[GAUSS-51229] : The database node listenIp(%s) is not "
                       "in the virtualIp or backIp on the node(%s).",
        'GAUSS_51230': "[GAUSS-51230] : The number of %s must %s.",
        'GAUSS_51231': "[GAUSS-51231] : Old nodes is less than 2.",
        'GAUSS_51232': "[GAUSS-51232] : XML configuration and static "
                       "configuration are the same.",
        'GAUSS_51233': "[GAUSS-51233] : The Port(%s) is invalid "
                       "on the node(%s).",
        'GAUSS_51234': "[GAUSS-51234] : The configuration file [%s] "
                       "contains parsing errors.",
        'GAUSS_51235': "[GAUSS-51235] : Invalid directory [%s].",
        'GAUSS_51236': "[GAUSS-51236] : Failed to parsing xml.",
        'GAUSS_51239': "[GAUSS-51239] : Failed to parse json. gs_collect "
                       "configuration file (%s) is invalid , "
                       "check key in json file",
        'GAUSS_51240': "[GAUSS-51240] : gs_collect configuration file "
                       "is invalid, TypeName or content must in config file.",
        'GAUSS_51241': "[GAUSS-51241] : The parameter %s(%s) formate "
                       "is wrong, or value is less than 0.",
        'GAUSS_51242': "[GAUSS-51242] : gs_collect configuration file "
                       "is invalid: %s, the key: (%s) is invalid.",
        'GAUSS_51243': "[GAUSS-51243] : content(%s) does not match the "
                       "typename(%s) in gs_collect configuration file(%s).",
        'GAUSS_51244': "[GAUSS-51244] : (%s) doesn't yet support.",
        'GAUSS_51245': "[GAUSS-51245] : There are duplicate key(%s).",
        'GAUSS_51246': "[GAUSS-51246] : %s info only support "
                       "one time collect.",
        'GAUSS_51247': "[GAUSS-51247] : These virtual IP(%s) are not "
                       "accessible after configuring.",
        'GAUSS_51248': "[GAUSS-51248] : The hostname(%s) may be not same with "
                       "hostname(/etc/hostname)",
        'GAUSS_51249': "[GAUSS-51249] : There is no database node instance "
                       "in the current node.",
        'GAUSS_51250': "[GAUSS-51250] : Error: the '%s' is illegal.\nthe path "
                       "name or file name should be letters, number or -_:."


    }

    ###########################################################################
    # SQL exception
    ###########################################################################
    GAUSS_513 = {
        'GAUSS_51300': "[GAUSS-51300] : Failed to execute SQL: %s.",
        'GAUSS_51301': "[GAUSS-51301] : Execute SQL time out. \nSql: %s.",
        'GAUSS_51302': "[GAUSS-51302] : The table '%s.%s' does not exist "
                       "or is private table!",
        'GAUSS_51303': "[GAUSS-51303] : Query '%s' has no record!.",
        'GAUSS_51304': "[GAUSS-51304] : Query '%s' result '%s' is incorrect!.",
        'GAUSS_51305': "[GAUSS-51305] : The table '%s.%s' exists!",
        'GAUSS_51306': "[GAUSS-51306] : %s: Abnormal reason:%s",
        'GAUSS_51307': "[GAUSS-51307] : Error: can not get sql execute "
                       "status.",
        'GAUSS_51308': "[GAUSS-51308] : Error: can not load result data.",
        'GAUSS_51309': "[GAUSS-51309] : Can not get correct result"
                       " by executing sql: %s",
        'GAUSS_51310': "[GAUSS-51310] : Failed to get connection"
                       " with database %s"

    }

    ###########################################################################
    # Shell exception
    ###########################################################################
    GAUSS_514 = {
        'GAUSS_51400': "[GAUSS-51400] : Failed to execute the command: %s.",
        'GAUSS_51401': "[GAUSS-51401] : Failed to do %s.sh.",
        'GAUSS_51402': "[GAUSS-51402]: Failed to generate certs.",
        'GAUSS_51403': "[GAUSS-51403]: commond execute failure,"
                       " check %s failed!",
        'GAUSS_51404': "[GAUSS-51404] : Not supported command %s.",
        'GAUSS_51405': "[GAUSS-51405] : You need to install software:%s\n"

    }

    ###########################################################################
    # interface calls exception
    ###########################################################################
    GAUSS_515 = {
        'GAUSS_51500': "[GAUSS-51500] : Failed to call the interface %s. "
                       "Exception: %s."
    }

    ###########################################################################
    # cluster/instance status
    ###########################################################################
    GAUSS_516 = {
        'GAUSS_51600': "[GAUSS-51600] : Failed to obtain the cluster status.",
        'GAUSS_51601': "[GAUSS-51601] : Failed to check %s status.",
        'GAUSS_51602': "[GAUSS-51602] : The cluster status is Abnormal.",
        'GAUSS_51603': "[GAUSS-51603] : Failed to obtain peer %s instance.",
        'GAUSS_51604': "[GAUSS-51604] : There is no HA status for %s.",
        'GAUSS_51605': "[GAUSS-51605] : Failed to check whether "
                       "the %s process exists.",
        'GAUSS_51606': "[GAUSS-51606] : Failed to kill the %s process.",
        'GAUSS_51607': "[GAUSS-51607] : Failed to start %s.",
        'GAUSS_51608': "[GAUSS-51608] : Failed to lock cluster",
        'GAUSS_51609': "[GAUSS-51609] : Failed to unlock cluster",
        'GAUSS_51610': "[GAUSS-51610] : Failed to stop %s.",
        'GAUSS_51611': "[GAUSS-51611] : Failed to create %s instance.",
        'GAUSS_51612': "[GAUSS-51612] : The node id [%u] are not found "
                       "in the cluster.",
        'GAUSS_51613': "[GAUSS-51613] : There is no instance in %s to "
                       "be built.",
        'GAUSS_51614': "[GAUSS-51614] : Received signal[%d].",
        'GAUSS_51615': "[GAUSS-51615] : Failed to initialize instance.",
        'GAUSS_51616': "[GAUSS-51616] : Failed to dump %s schema.",
        'GAUSS_51617': "[GAUSS-51617] : Failed to rebuild %s.",
        'GAUSS_51618': "[GAUSS-51618] : Failed to get all hostname.",
        'GAUSS_51619': "[GAUSS-51619] : The host name [%s] is not "
                       "in the cluster.",
        'GAUSS_51620': "[GAUSS-51620] : Failed to obtain %s "
                       "instance information.",
        'GAUSS_51621': "[GAUSS-51621] : HA IP is empty.",
        'GAUSS_51622': "[GAUSS-51622] : There is no %s on %s node.",
        'GAUSS_51623': "[GAUSS-51623] : Failed to obtain version.",
        'GAUSS_51624': "[GAUSS-51624] : Failed to get CN connections.",
        'GAUSS_51625': "[GAUSS-51625] : Cluster is running.",
        'GAUSS_51626': "[GAUSS-51626] : Failed to rollback.",
        'GAUSS_51627': "[GAUSS-51627] : Configuration failed.",
        'GAUSS_51628': "[GAUSS-51628] : The version number of new cluster "
                       "is [%s]. It should be float.",
        'GAUSS_51629': "[GAUSS-51629] : The version number of new cluster "
                       "is [%s]. It should be greater than or equal to "
                       "the old version.",
        'GAUSS_51630': "[GAUSS-51630] : No node named %s.",
        'GAUSS_51631': "[GAUSS-51631] : Failed to delete the %s instance.",
        'GAUSS_51632': "[GAUSS-51632] : Failed to do %s.",
        'GAUSS_51633': "[GAUSS-51633] : The step of upgrade "
                       "number %s is incorrect.",
        'GAUSS_51634': "[GAUSS-51634] : Waiting node synchronizing timeout "
                       "lead to failure.",
        'GAUSS_51635': "[GAUSS-51635] : Failed to check SHA256.",
        'GAUSS_51636': "[GAUSS-51636] : Failed to obtain %s node information.",
        'GAUSS_51637': "[GAUSS-51637] : The %s does not match with %s.",
        'GAUSS_51638': "[GAUSS-51638] : Failed to append instance on "
                       "host [%s].",
        'GAUSS_51639': "[GAUSS-51639] : Failed to obtain %s status of "
                       "local node.",
        'GAUSS_51640': "[GAUSS-51640] : Can't connect to cm_server, cluster "
                       "is not running possibly.",
        'GAUSS_51641': "[GAUSS-51641] : Cluster redistributing status is not "
                       "accord with expectation.",
        'GAUSS_51642': "[GAUSS-51642] : Failed to promote peer instances.",
        'GAUSS_51643': "[GAUSS-51643] : Cluster is in read-only mode.",
        'GAUSS_51644': "[GAUSS-51644] : Failed to set resource control "
                       "for the cluster.",
        'GAUSS_51645': "[GAUSS-51645] : Failed to restart %s.",
        'GAUSS_51646': "[GAUSS-51646] : The other OM operation is currently "
                       "being performed in the cluster node:"
                       " '%s'.",
        'GAUSS_51647': "[GAUSS-51647] : The operation step of OM components "
                       "in current cluster nodes do not match"
                       " with each other: %s.",
        'GAUSS_51648': "[GAUSS-51648] : Waiting for redistribution process "
                       "to end timeout.",
        'GAUSS_51649': "[GAUSS-51649] : Capture exceptions '%s' : %s.",
        'GAUSS_51650': "[GAUSS-51650] : Unclassified exceptions: %s.",
        'GAUSS_51651': "[GAUSS-51651] : The node '%s' status is Abnormal.",
        'GAUSS_51652': "[GAUSS-51652] : Failed to get cluster node "
                       "info.exception is: %s.",
        'GAUSS_51653': "[GAUSS-51653] : No database objects "
                       "were found in the cluster!",
        'GAUSS_51654': "[GAUSS-51654] : Cannot query instance process"
                       " version from function."

    }

    ###########################################################################
    # Check system table
    ###########################################################################
    GAUSS_517 = {
        'GAUSS_51700': "[GAUSS-51700] : There must be only one record in the "
                       "pgxc_group table.",
        'GAUSS_51701': "[GAUSS-51701] : The current node group is incorrect.",
        'GAUSS_51702': "[GAUSS-51702] : Failed to obtain node group "
                       "information.",
        'GAUSS_51703': "[GAUSS-51703] : Failed to drop record from "
                       "PGXC_NODE.",
        'GAUSS_51704': "[GAUSS-51704] : Failed to set Cgroup.",
        'GAUSS_51705': "[GAUSS-51705] : Failed to update PGXC_NODE.",
        'GAUSS_51706': "[GAUSS-51706] : Failed to check Cgroup.",
        'GAUSS_51707': "[GAUSS-51707] : Failed to install Cgroup.",
        'GAUSS_51708': "[GAUSS-51708] : Failed to uninstall Cgroup.",
        'GAUSS_51709': "[GAUSS-51709] : Failed to clean Cgroup "
                       "configuration file."
    }

    ###########################################################################
    # environmental variable
    ###########################################################################
    GAUSS_518 = {
        'GAUSS_51800': "[GAUSS-51800] : The environmental variable %s is "
                       "empty. or variable has exceeded maximum length",
        'GAUSS_51801': "[GAUSS-51801] : The environment variable %s exists.",
        'GAUSS_51802': "[GAUSS-51802] : Failed to obtain the environment "
                       "variable %s.",
        'GAUSS_51803': "[GAUSS-51803] : Failed to delete the environment "
                       "variable %s.",
        'GAUSS_51804': "[GAUSS-51804] : Failed to set the environment "
                       "variable %s.",
        'GAUSS_51805': "[GAUSS-51805] : The environmental variable [%s]'s "
                       "value is invalid.",
        'GAUSS_51806': "[GAUSS-51806] : The cluster has been installed.",
        'GAUSS_51807': "[GAUSS-51807] : $GAUSSHOME of user is not equal to "
                       "installation path.",
        'GAUSS_51808': "[GAUSS-51808] : The env file contains errmsg: %s."
    }

    ###########################################################################
    # OS version
    ###########################################################################
    GAUSS_519 = {
        'GAUSS_51900': "[GAUSS-51900] : The current OS is not supported.",
        'GAUSS_51901': "[GAUSS-51901] : The OS versions are different "
                       "among cluster nodes."
    }

    ###########################################################################
    # database version
    ###########################################################################
    GAUSS_520 = {
        'GAUSS_52000': "[GAUSS-52000] : Failed to obtain time zone "
                       "information about the cluster node.",
        'GAUSS_52001': "[GAUSS-52001] : Time zone information is "
                       "different among cluster nodes."
    }

    ###########################################################################
    # OS time zone
    ###########################################################################
    GAUSS_521 = {
        'GAUSS_52100': "[GAUSS-52100] : Failed to obtain cluster node "
                       "character sets.",
        'GAUSS_52101': "[GAUSS-52101] : Character sets are different "
                       "among cluster nodes.",
        'GAUSS_52102': "[GAUSS-52102] : The parameter [%s] value is not equal "
                       "to the expected value.",
        'GAUSS_52103': "[GAUSS-52103] : Failed to forcibly make the character "
                       "sets to take effect."
    }

    ###########################################################################
    # OS character set
    ###########################################################################
    GAUSS_522 = {
        'GAUSS_52200': "[GAUSS-52200] : Unable to import module: %s.",
        'GAUSS_52201': "[GAUSS-52201] : The current python version %s "
                       "is not supported."
    }

    ###########################################################################
    # Operating system parameters
    ###########################################################################
    GAUSS_523 = {
        'GAUSS_52300': "[GAUSS-52300] : Failed to set OS parameters.",
        'GAUSS_52301': "[GAUSS-52301] : Failed to check OS parameters."

    }

    ###########################################################################
    # preinsatll install
    ###########################################################################
    GAUSS_524 = {
        'GAUSS_52400': "[GAUSS-52400] : Installation environment does not "
                       "meet the desired result.",
        'GAUSS_52401': "[GAUSS-52401] : On systemwide basis, the maximum "
                       "number of %s is not correct. the current %s value is:",
        'GAUSS_52402': "[GAUSS-52402] : IP [%s] is not matched "
                       "with hostname [%s]. \n",
        'GAUSS_52403': "[GAUSS-52403] : Command \"%s\" does not exist or the "
                       "user has no execute permission on %s."
    }

    ###########################################################################
    # uninsatll postuninstall
    ###########################################################################
    GAUSS_525 = {
        'GAUSS_52500': "[GAUSS-52500] : Failed to delete regular tasks.",
        'GAUSS_52501': "[GAUSS-52501] : Run %s script before "
                       "executing this script.",
        'GAUSS_52502': "[GAUSS-52502] : Another OM process is being executed. "
                       "To avoid conflicts, this process ends in advance."
    }

    ###########################################################################
    # expand and shrik
    ###########################################################################
    GAUSS_526 = {
        'GAUSS_52600': "[GAUSS-52600] : Can not obtain any cluster ring.",
        'GAUSS_52601': "[GAUSS-52601] : Redistribution failed due to"
                       " user request.",
        'GAUSS_52602': "[GAUSS-52602] : There is no CN in old nodes.",
        'GAUSS_52603': "[GAUSS-52603] : There is no CN on the contraction of "
                       "the remaining nodes.",
        'GAUSS_52604': "[GAUSS-52604] : Parameter '-r'[%s] can not be "
                       "more than the numbers of cluster ring[%s].",
        'GAUSS_52605': "[GAUSS-52605] : Can not contract local node(%s).",
        'GAUSS_52606': "[GAUSS-52606] : Contract too many nodes. "
                       "It should left three nodes to format "
                       "a cluster at least.",
        'GAUSS_52607': "[GAUSS-52607] : [%s] does not at the "
                       "end of instance list.",
        'GAUSS_52608': "[GAUSS-52608] : [%s] contains %s instance.",
        'GAUSS_52609': "[GAUSS-52609] : All contracted nodes do not "
                       "contain database node instance.",
        'GAUSS_52610': "[GAUSS-52610] : The current node group are "
                       "node group after contraction.",
        'GAUSS_52611': "[GAUSS-52611] : There must be only one record "
                       "in the current node group.",
        'GAUSS_52612': "[GAUSS-52612] : All dilatation nodes do not contain "
                       "the database node instance.",
        'GAUSS_52613': "[GAUSS-52613] : Static configuration is not matched "
                       "on some nodes. Please handle it first.",
        'GAUSS_52614': "[GAUSS-52614] : Timeout. The current "
                       "cluster status is %s.",
        'GAUSS_52615': "[GAUSS-52615] : Cluster lock unlocked due to timeout.",
        'GAUSS_52616': "[GAUSS-52616] : Can not find a similar "
                       "instance for [%s %s].",
        'GAUSS_52617': "[GAUSS-52617] : Invalid check type.",
        'GAUSS_52618': "[GAUSS-52618] : Failed to delete etcd from node.",
        'GAUSS_52619': "[GAUSS-52619] : Failed to uninstall application.",
        'GAUSS_52620': "[GAUSS-52620] : Not all nodes found. The following "
                       "is what we found: %s.",
        'GAUSS_52621': "[GAUSS-52621] : No DNs specified in target "
                       "create new group.",
        'GAUSS_52622': "[GAUSS-52622] : No new group name specified in "
                       "target create new group.",
        'GAUSS_52623': "[GAUSS-52623] : Failed to check node group "
                       "numbers: Node group numbers is [%d].",
        'GAUSS_52624': "[GAUSS-52624] : Failed to check %s node "
                       "group members: Invaild group name or nodes.",
        'GAUSS_52625': "[GAUSS-52625] : The local instance and peer instance "
                       "does not both in contracted nodes.",
        'GAUSS_52626': "[GAUSS-52626] : The CN connection on the old "
                       "nodes are abnormal.",
        'GAUSS_52627': "[GAUSS-52627] : The current cluster is locked.",
        'GAUSS_52628': "[GAUSS-52628] : Static configuration has already "
                       "been updated on all nodes, expansion has been "
                       "completed possibly.",
        'GAUSS_52629': "[GAUSS-52629] : Cluster ring(%s) can not obtain "
                       "less than three nodes.",
        'GAUSS_52630': "[GAUSS-52630] : Failed to set the read-only mode "
                       "parameter for all database node instances.",
        'GAUSS_52631': "[GAUSS-52631] : Invalid value for GUC parameter "
                       "comm_max_datanode: %s.",
        'GAUSS_52632': "[GAUSS-52632] : Cluster breakdown or "
                       "abnormal operation during "
                       "expanding online, lock process for expansion is lost.",
        'GAUSS_52633': "[GAUSS-52633] : Can not excute redistribution "
                       "for shrink excuted failed."


    }

    ###########################################################################
    # replace
    ###########################################################################
    GAUSS_527 = {
        'GAUSS_52700': "[GAUSS-52700] : Failed to update ETCD.",
        'GAUSS_52701': "[GAUSS-52701] : All the CMAgents instances are "
                       "abnormal. Cannot fix the cluster.",
        'GAUSS_52702': "[GAUSS-52702] : The cluster status is Normal. "
                       "There is no instance to fix.",
        'GAUSS_52703': "[GAUSS-52703] : The number of normal ETCD must "
                       "be greater than half.",
        'GAUSS_52704': "[GAUSS-52704] : Failed to check the %s condition.",
        'GAUSS_52705': "[GAUSS-52705] : Failed to obtain ETCD key.",
        'GAUSS_52706': "[GAUSS-52706] : Failed to clean ETCD and touch "
                       "flag file on %s.",
        'GAUSS_52707': "[GAUSS-52707] : Failed to install on %s.",
        'GAUSS_52708': "[GAUSS-52708] : Failed to configure on %s.",
        'GAUSS_52709': "[GAUSS-52709] : Failed to check the cluster "
                       "configuration differences:",
        'GAUSS_52710': "[GAUSS-52710] : Replacement failed.",
        'GAUSS_52711': "[GAUSS-52711] : Failed to set CMAgent start mode."
    }

    ###########################################################################
    # manageCN and changeIP
    ###########################################################################
    GAUSS_528 = {
        'GAUSS_52800': "[GAUSS-52800] : Cluster is %s(%s) now.",
        'GAUSS_52801': "[GAUSS-52801] : Only allow to %s one CN. The %s "
                       "is not matched.",
        'GAUSS_52802': "[GAUSS-52802] : Only allow to add one CN at the end.",
        'GAUSS_52803': "[GAUSS-52803] : There is at least one Normal "
                       "CN after delete CN.",
        'GAUSS_52804': "[GAUSS-52804] : Failed to add the Abnormal CN.",
        'GAUSS_52805': "[GAUSS-52805] : Failed to find another instance as "
                       "model for instance(%s).",
        'GAUSS_52806': "[GAUSS-52806] : Invalid rollback step: %s.",
        'GAUSS_52807': "[GAUSS-52807] : There is no IP changed.",
        'GAUSS_52808': "[GAUSS-52808] : Detected CN %s, but the action is %s.",
        'GAUSS_52809': "[GAUSS-52809] : Only allow to add or delete one CN.",
        'GAUSS_52810': "[GAUSS-52810] : There is Abnormal coodinator(s) "
                       "in cluster, please delete it firstly."
    }

    ###########################################################################
    # upgrade
    ###########################################################################
    GAUSS_529 = {
        'GAUSS_52900': "[GAUSS-52900] : Failed to upgrade strategy: %s.",
        'GAUSS_52901': "[GAUSS-52901] : New cluster commitid cannot be same "
                       "with old cluster commitid.",
        'GAUSS_52902': "[GAUSS-52902] : Can not support upgrade from %s to %s",
        'GAUSS_52903': "[GAUSS-52903] : The new cluster version number[%s] "
                       "should be bigger than the old cluster[%s].",
        'GAUSS_52904': "[GAUSS-52904] : Please choose right upgrade strategy.",
        'GAUSS_52905': "[GAUSS-52905] : Upgrade nodes number cannot "
                       "be more than %d.",
        'GAUSS_52906': "[GAUSS-52906] : Grey upgrade nodes number cannot "
                       "be more than cluster nodes.",
        'GAUSS_52907': "[GAUSS-52907] : Failed to cancel the cluster "
                       "read-only mode",
        'GAUSS_52908': "[GAUSS-52908] : Failed to set cluster read-only mode.",
        'GAUSS_52909': "[GAUSS-52909] : Specified upgrade nodes with "
                       "same step can do upgrade task.",
        'GAUSS_52910': "[GAUSS-52910] : These nodes %s have been successfully "
                       "upgraded to new version, no need to upgrade again.",
        'GAUSS_52911': "[GAUSS-52911] : Last unsuccessfully upgrade nodes "
                       "%s are not same with current upgrade nodes.",
        'GAUSS_52912': "[GAUSS-52912] : Some nodes were upgraded but "
                       "were unsuccessfully, cannot use --continue.",
        'GAUSS_52913': "[GAUSS-52913] : All nodes have been upgraded. "
                       "No need to use --continue.",
        'GAUSS_52914': "[GAUSS-52914] : The record commitid is not same "
                       "with current commitid.",
        'GAUSS_52915': "[GAUSS-52915] : $GAUSSHOME is not a symbolic link.",
        'GAUSS_52916': "[GAUSS-52916] : Current upgrade status is "
                       "not pre commit.",
        'GAUSS_52917': "[GAUSS-52917] : Failed to drop old pmk schema.",
        'GAUSS_52918': "[GAUSS-52918] : Failed to record node upgrade step "
                       "in table %s.%s.",
        'GAUSS_52919': "[GAUSS-52919] : Upgrade has already been committed "
                       "but not finished commit.",
        'GAUSS_52920': "[GAUSS-52920] : Can not use grey upgrade option "
                       "--continue before upgrade grey nodes.",
        'GAUSS_52921': "[GAUSS-52921] : Failed to query disk usage "
                       "with gs_check tool.",
        'GAUSS_52922': "[GAUSS-52922] : Disk usage exceeds %s, "
                       "please clean up before upgrading.",
        'GAUSS_52923': "[GAUSS-52923] : .",
        'GAUSS_52924': "[GAUSS-52924] : .",
        'GAUSS_52925': "[GAUSS-52925] : Input upgrade type [%s] is not same "
                       "with record upgrade type [%s].",
        'GAUSS_52926': "[GAUSS-52926] : The step of upgrade should be digit.",
        'GAUSS_52927': "[GAUSS-52927] : ",
        'GAUSS_52928': "[GAUSS-52928] : .",
        'GAUSS_52929': "[GAUSS-52929] : Failed to check application version. "
                       "Output: \n%s.",
        'GAUSS_52930': "[GAUSS-52930] : .",
        'GAUSS_52931': "[GAUSS-52931] : .",
        'GAUSS_52932': "[GAUSS-52932] : There is no CN in the remaining "
                       "old nodes.",
        'GAUSS_52933': "[GAUSS-52933] : There is not a majority of %s on the "
                       "remaining old nodes.",
        'GAUSS_52934': "[GAUSS-52934] : .",
        'GAUSS_52935': "[GAUSS-52935] : Current upgrade version is not same "
                       "with unfinished upgrade version record.",
        'GAUSS_52936': "[GAUSS-52936] : Upgrade is not finished, "
                       "cannot do another task.",
        'GAUSS_52937': "[GAUSS-52937] : Clean install directory option is "
                       "invalid, can only be 'new' or 'old'!",
        'GAUSS_52938': "[GAUSS-52938] : Can not find %s.",
        'GAUSS_52939': "[GAUSS-52939] : Can not get %s.",
        'GAUSS_52940': "[GAUSS-52940] : Invalid node type:%s.",
        'GAUSS_52941': "[GAUSS-52941] : Invalid node role:%s.",
        'GAUSS_52942': "[GAUSS-52942] : No such key to check guc value.",
        'GAUSS_52943': "[GAUSS-52943] : Invalid instance type:%s."

    }

    ###########################################################################
    # check
    ###########################################################################
    GAUSS_530 = {
        'GAUSS_53000': "[GAUSS-53000] : The database user [%s] is not "
                       "match with the old user [%s].",
        'GAUSS_53001': "[GAUSS-53001] : The result of query table "
                       "is incorrect: %s.",
        'GAUSS_53002': "[GAUSS-53002] : Failed to obtain SSD device.",
        'GAUSS_53003': "[GAUSS-53003] : The checked item does not meet "
                       "the standards.",
        'GAUSS_53004': "[GAUSS-53004] : Failed to collect statistics "
                       "on all nodes.",
        'GAUSS_53005': "[GAUSS-53005] : Unable to obtain SSD disk "
                       "on current node.",
        'GAUSS_53006': "[GAUSS-53006] : No database node instance uses data "
                       "directory %s on %s.",
        'GAUSS_53007': "[GAUSS-53007] : Failed to switch %s.",
        'GAUSS_53008': "[GAUSS-53008] : The current node do not install SSD. "
                       "Can not check SSD performance.",
        'GAUSS_53009': "[GAUSS-53009] : Failed to format cu of directory: %s.",
        'GAUSS_53010': "[GAUSS-53010] : The function name of %s is not exist "
                       "in the %s.",
        'GAUSS_53011': "[GAUSS-53011] : Failed to check %s.",
        'GAUSS_53012': "[GAUSS-53012] : Failed to insert pmk data to "
                       "database.",
        'GAUSS_53013': "[GAUSS-53013] : %s can not be empty.",
        'GAUSS_53014': "[GAUSS-53014] : %s must be a nonnegative integer.",
        'GAUSS_53015': "[GAUSS-53015] : The threshold Threshold_NG[%d] "
                       "must be greater than Threshold_Warning[%d].",
        'GAUSS_53016': "[GAUSS-53016] : The threshold Threshold_NG[%d] and "
                       "Threshold_Warning[%d] must be integer from 1 to 99.",
        'GAUSS_53017': "[GAUSS-53017] : Unsupported operating system %s.",
        'GAUSS_53018': "[GAUSS-53018] : Failed to get file handler "
                       "of process %s by use cmd %s.",
        'GAUSS_53019': "[GAUSS-53019] : Failed to delete variable '%s %s'"
                       " from /etc/sysctl.conf.",
        'GAUSS_53020': "[GAUSS-53020] : Failed to set %s.",
        'GAUSS_53021': "[GAUSS-53021] : %s only can be supported"
                       " on %s Platform.",
        'GAUSS_53022': "[GAUSS-53022] : Platform %s%s is not supported.",
        'GAUSS_53023': "[GAUSS-53023] : Failed to get CPUcores and MemSize."
                       " Error: %s",
        'GAUSS_53024': "[GAUSS-53024] : Failed to get ip string for"
                       " config pg_hba.conf.",
        'GAUSS_53025': "[GAUSS-53025] : content's type must be bytes.",
        'GAUSS_53026': "[GAUSS-53026] : bytes's type must be in (bytes, str).",
        'GAUSS_53027': "[GAUSS-53027] : content's len must >= (iv_len + 16).",
        'GAUSS_53028': "[GAUSS-53028] : Test PMK schema failed. "
                       "Output: \n%s",
        'GAUSS_53029': "[GAUSS-53029] : Failed to install pmk schema,"
                       "Error: \n%s",
        'GAUSS_53030': "[GAUSS-53030] : The class must have %s",
        'GAUSS_53031': "[GAUSS-53031] : The cluster is None.",
        'GAUSS_53032': "[GAUSS-53032] : The speed limit must "
                       "be a nonnegative integer.",
        'GAUSS_53033': "[GAUSS-53033] : Invalid User : %s."

    }

    ###########################################################################
    # check interface
    ###########################################################################
    GAUSS_531 = {
        'GAUSS_53100': "[GAUSS-53100] : [%s] is not supported in single "
                       "cluster.",
        'GAUSS_53101': "[GAUSS-53101] : This interface is not supported "
                       "in %s cluster.",
        'GAUSS_53102': "[GAUSS-53102] : [%s] is not supported in "
                       "express cluster.",
        'GAUSS_53103': "[GAUSS-53103] : The single primary multi standby "
                       "cluster does not support the product"
                       " version '%s'.",
        'GAUSS_53104': "[GAUSS-53104] : [%s] is not supported in "
                       "single instance cluster."
    }
    ###########################################################################
    # Single Primary MultiStandby cluster
    ###########################################################################
    GAUSS_532 = {
        'GAUSS_53200': "[GAUSS-53200] : The number of standbys for each "
                       "database node instance must be the same. "
                       "Please set it.",
        'GAUSS_53201': "[GAUSS-53201] : The number of database node standbys "
                       "and the AZ settings are incorrect. Please set it.",
        'GAUSS_53202': "[GAUSS-53202] : The AZ information is incorrect. "
                       "Please set it.",
        'GAUSS_53203': "[GAUSS-53203] : The number of ETCD in %s. "
                       "Please set it.",
        'GAUSS_53204': "[GAUSS-53204] : [%s] is not supported in single "
                       "primary multistandby cluster.",
        'GAUSS_53205': "[GAUSS-53205] : The priority of %s must be higher "
                       "than %s. Please set it.",
        'GAUSS_53206': "[GAUSS-53206] : The value of %s must be greater "
                       "than 0 and less than 11. Please set it."
    }
    ###########################################################################
    # License
    ###########################################################################
    GAUSS_533 = {
        'GAUSS_53300': "[GAUSS-53300] : The current product version '%s' "
                       "does not support the license "
                       "register/unregister operation.",
        'GAUSS_53301': "[GAUSS-53301] : The license control files are not "
                       "consistent on the cluster.",
        'GAUSS_53302': "[GAUSS-53302] : The current cluster does not apply "
                       "the license control, please upgrade it"
                       " first.",
        'GAUSS_53303': "[GAUSS-53303] : The DWS cluster does not support the "
                       "license register/unregister operation.",
        'GAUSS_53304': "[GAUSS-53304] : Can not register the enabled "
                       "features.",
        'GAUSS_53305': "[GAUSS-53304] : Can not un-register "
                       "the disabled features.",
        'GAUSS_53306': "[GAUSS-53306] : Can not register the unsupported "
                       "features of the product version '%s'.",
        'GAUSS_53307': "[GAUSS-53307] : No need to un-register the "
                       "unsupported "
                       "features of the product version '%s'."
    }
    ###########################################################################
    # ROACH
    # [GAUSS-53400] : Roach etcd operator failded
    ###########################################################################
    GAUSS_534 = {
        'GAUSS_53400': "[GAUSS-53400] : Roach ETCD term operate failed.",
        'GAUSS_53401': "[GAUSS-53401] : Roach delete/clean operate failed, "
                       "Failed to clean %s",
        'GAUSS_53402': "[GAUSS-53402] : Get %s cluster infomation/env "
                       "failed, %s",
        'GAUSS_53403': "[GAUSS-53403] : Cluster balance check failed",
        'GAUSS_53404': "[GAUSS-53404] : backup key %s does not exist"
    }
    ##########################################################################
    # gs_collector
    # [GAUSS-53500] : gs_collector failed
    ##########################################################################
    GAUSS_535 = {
        'GAUSS_53500': "[GAUSS-53500] : Relation %s does not exist.",
        'GAUSS_53501': "[GAUSS-53501] : Connect to server failed, "
                       "connection refused",
        'GAUSS_53502': "[GAUSS-53502] : Please check database status",
        'GAUSS_53503': "[GAUSS-53503] : There is no coordinator inst in "
                       "this host",
        'GAUSS_53504': "[GAUSS-53504] : There is no %s files: "
                       "please check start-time and end-time.",
        'GAUSS_53505': "[GAUSS-53505] : There is no log files: "
                       "please check cluster info.",
        'GAUSS_53506': "[GAUSS-53506] : Failed to mkdir.",
        'GAUSS_53507': "[GAUSS-53507] : Failed to execute %s command.",
        'GAUSS_53508': "[GAUSS-53508] : Core pattern is not core-e-p-t.",
        'GAUSS_53509': "[GAUSS-53509] : There is no core files: "
                       "please check core file name pattern.",
        'GAUSS_53510': "[GAUSS-53510] : Please check db status or "
                       "database name.",
        'GAUSS_53511': "[GAUSS-53511] : There is no %s process.",
        'GAUSS_53512': "[GAUSS-53512] : Gstack command not found.",
        'GAUSS_53513': "[GAUSS-53513] : Schema '%s' is not in white list.",
        'GAUSS_53514': "[GAUSS-53514] : Relation '%s' does not belong "
                       "to '%s' schema.",
        'GAUSS_53515': "[GAUSS-53515] : Database content '%s' is invalid, "
                       "only support 'schema.relation'.",
        'GAUSS_53516': "[GAUSS-53516] : There is no info should be collected ,"
                                        "gs_collector is finished. "
    }

    GAUSS_536 = {
        'GAUSS_53600': "[GAUSS-53600]: Can not start the database, "
                       "the cmd is %s,  Error:\n%s.",
        'GAUSS_53601': "[GAUSS-53601]: Can not start the primary database, "
                       "Error:\n%s.",
        'GAUSS_53602': "[GAUSS-53602]: Can not start the standby database, "
                       "Error:\n%s.",
        'GAUSS_53603': "[GAUSS-53603]: The dataDir can not be empty.",
        'GAUSS_53604': "[GAUSS_53604]: The hostName %s has not %s process.",
        'GAUSS_53605': "[GAUSS_53605]: The %s in hostName %s is running.",
        'GAUSS_53606': "[GAUSS-53606]: Can not stop the database, "
                       "the cmd is %s,  Error:\n%s.",
        'GAUSS_53607': "[GAUSS-53607]: Fail to remove the file %s, "
                       "Error:\n%s.",
        'GAUSS_53608': "[GAUSS-53608]: Can not start the database, "
                       "Error:\n%s.",
        'GAUSS_53609': "[GAUSS-53609]: Can not stop the database, "
                       "Error:\n%s.",
        'GAUSS_53610': "[GAUSS-53610]: The input dataDir(%s) "
                       "may be incorrect.",
        'GAUSS_53611': "[GAUSS-53611]: Error information is :\n%s",
        'GAUSS_53612': "[GAUSS-53612]: Can not find any catalog in database %s"
    }

    ##########################################################################
    # gs_expansion
    # [GAUSS-537] : gs_expansion failed
    ##########################################################################
    GAUSS_357 = {
        "GAUSS_35700": "[GAUSS-35700] Expansion standby node failed.",
        "GAUSS_35701": "[GAUSS-35701] Empty parameter. The %s parameter is" 
                       "missing in the command.",
        "GAUSS_35702": "[GAUSS-35702] Unrecognized parameter, standby host "
                       "backip %s is not in the "
                       "XML configuration file",
        "GAUSS_35703": "[GAUSS-35703] Check standby database Failed. The "
                       "database on node is abnormal. \n"
                       "node [%s], user [%s], dataNode [%s]. \n"
                       "You can use command \"gs_ctl query -D %s\" for more "
                       "detail.",
        "GAUSS_35704": "[GAUSS-35704] %s [%s] does not exist on node [%s].",
        "GAUSS_35705": "[GAUSS-35705] Error, the database version is "
                       "inconsistent in %s: %s" 
                       
    }

    ##########################################################################
    # gs_dropnode
    # [GAUSS-358] : gs_dropnode failed
    ##########################################################################
    GAUSS_358 = {
        "GAUSS_35800": "[GAUSS-35800] Expansion standby node failed.",
        "GAUSS_35801": "[GAUSS-35801] Empty parameter. The %s parameter is "
                       "missing in the command.",
        "GAUSS_35802": "[GAUSS-35802] The IP list of target node: %s"
                       "is not in the current cluster. Please check!",
        "GAUSS_35803": "[GAUSS-35803] The IP of local host %s is in the "
                       "target node list. \n"
                       "Can not drop local host!\n",
        "GAUSS_35804": "[GAUSS-35804] The dropnode operation can only be executed"
                       " at the primary node. \n ",
        "GAUSS_35805": "[GAUSS-35805] Input %s. Operation aborted. ",
        "GAUSS_35806": "[GAUSS-35806] Current status of cluster is %s .\n"
                       "It doesn't meet the requirement.! ",
        "GAUSS_35807": "[GAUSS-35807] The host %s which still exist in the "
                       "cluster can't be connected.\n"
                       "It doesn't meet the requirement! ",
        "GAUSS_35808": "[GAUSS-35808] The %s is running switchover/failover!\n"
                       "The dropnode operation can only be executed when there is"
                       " no such operation!",
        "GAUSS_35809": "[GAUSS-35809] Some important steps failed to execute. "
                       "Please refer to log for detail!",
        "GAUSS_35810": "[GAUSS-35810] A same process is already running! "

    }


class OmError(BaseException):
    """
    Used to record OM exception information and support ErrorCode
    keywords as message information.
    """

    def __init__(self, _message, *args, **kwargs):
        """
        Initialize the OmError instance.

        :param _message:    The input error message, it can be the error
                            message string, or the ErrorCode keywords,
                            or the Exception instance.
        :param args:        The additional unnamed parameters that use
                            to format the error message.
        :param kwargs:      The additional named parameters that use to format
                            the error message or extend to other
                            functions.

        :type _message:     str | BaseException
        :type args:         str | int
        :type kwargs:       str | int
        """
        # If we catch an unhandled exception.
        if isinstance(_message, Exception):
            # Store the error code.
            self._errorCode = ""
            # Store the error message.
            self._message = self.__getErrorMessage(str(_message), args, kwargs)
            # If can not parse the error code.
            if not self._errorCode:
                # Store the error code.
                self._errorCode = "GAUSS_51649"
                # Store the error message.
                self._message = ErrorCode.GAUSS_516[self._errorCode] % (
                    type(_message).__name__, repr(_message))
        else:
            # Store the error code.
            self._errorCode = ""
            # Store the error message.
            self._message = self.__getErrorMessage(_message, args, kwargs)

        # Store the stack information.
        self._stackInfo = sys.exc_info()[2]

    @property
    def message(self):
        """
        Getter, get the error message.

        :return:    Return the error message.
        :rtype:     str
        """
        return self._message

    @property
    def errorCode(self):
        """
        Getter, get the error code.

        :return:    Return the error code.
        :rtype:     str
        """
        return self._errorCode

    def __getErrorMessage(self, _errorCode, args, kwargs):
        """
        Get error information through error code.

        :param _errorCode:  Error code.
        :param args:        Additional parameters.
        :param kwargs:      Additional parameters.

        :type _errorCode:   str
        :type args:         tuple
        :type kwargs:       dict | None

        :return:    Return the error message.
        :rtype:     str
        """
        # Get base error information through error code.
        pattern = r"^[\S\s]*\[(GAUSS-\d+)\][\S\s]+$"
        match = re.match(pattern, str(_errorCode))
        if match and len(match.groups()) == 1:
            self._errorCode = match.groups()[0]
            message = _errorCode
        else:
            self._errorCode = "GAUSS_51650"
            message = ErrorCode.GAUSS_516[self._errorCode] % _errorCode

        # Format parameter which type is "%(param)s".
        if kwargs:
            for key, value in kwargs.items():
                if value is not None:
                    message = message.replace("%(" + key + ")s", str(value))
                else:
                    message = message.replace("%(" + key + ")s", "'None'")

        # Format standard type parameters.
        if args:
            # Convert tuple to list.
            args = list(args)
            # Travel the list.
            for i, arg in enumerate(args):
                if arg is None:
                    args[i] = "'None'"
                else:
                    args[i] = str(arg)

            # Format the message.
            message %= tuple(args)

        return message

    def __str__(self):
        """
        Show this instance as a string.

        :return:    Return this instance as a string.
        :rtype:     str
        """
        return self.message

    def __repr__(self):
        """
        Show this instance as a string.

        :return:    Return this instance as a string.
        :rtype:     str
        """
        return self.__str__()
