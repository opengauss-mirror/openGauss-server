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
# Description  : DropnodeImpl.py
#############################################################################

import subprocess
import sys
import re
import os
import pwd
import datetime
import grp
import socket
import stat

sys.path.append(sys.path[0] + "/../../../../")
from gspylib.threads.SshTool import SshTool
from gspylib.common.ErrorCode import ErrorCode
from gspylib.common.Common import DefaultValue, ClusterCommand
from gspylib.common.GaussLog import GaussLog
from gspylib.inspection.common.SharedFuncs import cleanFile
from gspylib.inspection.common.Exception import CheckException, \
    SQLCommandException

sys.path.append(sys.path[0] + "/../../../lib/")
DefaultValue.doConfigForParamiko()
import paramiko

# mode
MODE_PRIMARY = "primary"
MODE_STANDBY = "standby"
MODE_NORMAL = "normal"

SWITCHOVER_FILE = "/switchover"
FAILOVER_FILE = "/failover"
PROMOTE_FILE = "/promote"

# db state
STAT_NORMAL = "normal"

# master 
MASTER_INSTANCE = 0
# standby 
STANDBY_INSTANCE = 1

# status failed
STATUS_FAIL = "Failure"


class DropnodeImpl():
    """
    class for drop a standby node.
    step:
        1. check whether all standby can be reached or the switchover/failover is happening
        2. shutdown the program of the target node if it can be reached
        3. flush the configuration on all nodes if it is still a HA cluster
        4. flush the configuration on primary if it is the only one left
    """

    def __init__(self, dropnode):
        """
        """
        self.context = dropnode
        self.user = self.context.user
        self.userProfile = self.context.userProfile
        self.group = self.context.group
        self.backupFilePrimary = ''
        self.localhostname = DefaultValue.GetHostIpOrName()
        self.logger = self.context.logger
        self.resultDictOfPrimary = []
        self.replSlot = ''
        envFile = DefaultValue.getEnv("MPPDB_ENV_SEPARATE_PATH")
        if envFile:
            self.envFile = envFile
        else:
            self.envFile = "/etc/profile"
        gphomepath = DefaultValue.getEnv("GPHOME")
        if gphomepath:
            self.gphomepath = gphomepath
        else:
            (status, output) = subprocess.getstatusoutput("which gs_om")
            if "no gs_om in" in output:
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51800"] % "$GPHOME")
            self.gphomepath = os.path.normpath(output.replace("/gs_om", ""))
        self.appPath = self.context.clusterInfo.appPath
        self.gsql_path = "source %s;%s/bin/gsql" % (self.userProfile, self.appPath)

        currentTime = str(datetime.datetime.now()).replace(" ", "_").replace(
            ".", "_")
        self.dnIdForDel = []
        for hostDelName in self.context.hostMapForDel.keys():
            self.dnIdForDel += self.context.hostMapForDel[hostDelName]['dn_id']
        self.commonOper = OperCommon(dropnode)

    def change_user(self):
        if os.getuid() == 0:
            user = self.user
            try:
                pw_record = pwd.getpwnam(user)
            except CheckException:
                GaussLog.exitWithError(ErrorCode.GAUSS_503["GAUSS_50300"] % user)
            user_uid = pw_record.pw_uid
            user_gid = pw_record.pw_gid
            os.setgid(user_gid)
            os.setuid(user_uid)

    def checkAllStandbyState(self):
        """
        check all standby state whether switchover is happening
        """
        for hostNameLoop in self.context.hostMapForExist.keys():
            sshtool_host = SshTool([hostNameLoop])
            for i in self.context.hostMapForExist[hostNameLoop]['datadir']:
                # check whether switchover/failover is happening
                self.commonOper.checkStandbyState(hostNameLoop, i,
                                                  sshtool_host,
                                                  self.userProfile)
            self.cleanSshToolFile(sshtool_host)

        for hostNameLoop in self.context.hostMapForDel.keys():
            if hostNameLoop not in self.context.failureHosts:
                sshtool_host = SshTool([hostNameLoop])
                for i in self.context.hostMapForDel[hostNameLoop]['datadir']:
                    # check whether switchover/failover is happening
                    self.commonOper.checkStandbyState(hostNameLoop, i,
                                                      sshtool_host,
                                                      self.userProfile, True)
                    self.commonOper.stopInstance(hostNameLoop, sshtool_host, i,
                                                 self.userProfile)
                self.cleanSshToolFile(sshtool_host)

    def dropNodeOnAllHosts(self):
        """
        drop the target node on the other host
        """
        for hostNameLoop in self.context.hostMapForExist.keys():
            sshtool_host = SshTool([hostNameLoop])
            # backup
            backupfile = self.commonOper.backupConf(
                self.gphomepath, self.user,
                hostNameLoop, self.userProfile, sshtool_host)
            self.logger.log(
                "[gs_dropnode]The backup file of " + hostNameLoop + " is " + backupfile)
            if hostNameLoop == self.localhostname:
                self.backupFilePrimary = backupfile
            indexForuse = 0
            for i in self.context.hostMapForExist[hostNameLoop]['datadir']:
                # parse
                resultDict = self.commonOper.parseConfigFile(hostNameLoop, i,
                                                             self.dnIdForDel,
                                                             self.context.hostIpListForDel,
                                                             sshtool_host,
                                                             self.envFile)
                resultDictForRollback = self.commonOper.parseBackupFile(
                    hostNameLoop, backupfile,
                    self.context.hostMapForExist[hostNameLoop][
                        'dn_id'][indexForuse],
                    resultDict['replStr'], sshtool_host,
                    self.envFile)
                if hostNameLoop == self.localhostname:
                    self.resultDictOfPrimary.append(resultDict)
                # try set
                try:
                    self.commonOper.SetPgsqlConf(resultDict['replStr'],
                                                 hostNameLoop,
                                                 resultDict['syncStandbyStr'],
                                                 sshtool_host,
                                                 self.userProfile,
                                                 self.context.hostMapForExist[
                                                     hostNameLoop]['port'][
                                                     indexForuse],
                                                 '',
                                                 self.context.flagOnlyPrimary)
                except ValueError:
                    self.logger.log("[gs_dropnode]Rollback pgsql process.")
                    self.commonOper.SetPgsqlConf(resultDict['replStr'],
                                                 hostNameLoop,
                                                 resultDict['syncStandbyStr'],
                                                 sshtool_host,
                                                 self.userProfile,
                                                 self.context.hostMapForExist[
                                                     hostNameLoop]['port'][
                                                     indexForuse],
                                                 resultDictForRollback[
                                                     'rollbackReplStr'])
                try:
                    repl_slot = self.commonOper.get_repl_slot(hostNameLoop,
                        sshtool_host, self.userProfile, self.gsql_path,
                        self.context.hostMapForExist[hostNameLoop]['port'][
                            indexForuse])
                    self.commonOper.SetReplSlot(hostNameLoop, sshtool_host,
                                                self.userProfile, self.gsql_path,
                                                self.context.hostMapForExist[
                                                hostNameLoop]['port'][indexForuse
                                                ], self.dnIdForDel, repl_slot)
                except ValueError:
                    self.logger.log("[gs_dropnode]Rollback replslot")
                    self.commonOper.SetReplSlot(hostNameLoop, sshtool_host,
                                                self.userProfile, self.gsql_path,
                                                self.context.hostMapForExist[
                                                hostNameLoop]['port'][indexForuse
                                                ], self.dnIdForDel, repl_slot, True)
                indexForuse += 1
            self.cleanSshToolFile(sshtool_host)

    def operationOnlyOnPrimary(self):
        """
        operation only need to be executed on primary node
        """
        for hostNameLoop in self.context.hostMapForExist.keys():
            sshtool_host = SshTool([hostNameLoop])
            try:
                self.commonOper.SetPghbaConf(self.userProfile, hostNameLoop,
                                             self.resultDictOfPrimary[0][
                                             'pghbaStr'], False)
            except ValueError:
                self.logger.log("[gs_dropnode]Rollback pghba conf.")
                self.commonOper.SetPghbaConf(self.userProfile, hostNameLoop,
                                             self.resultDictOfPrimary[0][
                                             'pghbaStr'], True)
            self.cleanSshToolFile(sshtool_host)

    def modifyStaticConf(self):
        """
        Modify the cluster static conf and save it
        """
        self.logger.log("[gs_dropnode]Start to modify the cluster static conf.")
        staticConfigPath = "%s/bin/cluster_static_config" % self.appPath
        # first backup, only need to be done on primary node
        tmpDir = DefaultValue.getEnvironmentParameterValue("PGHOST", self.user,
                                                           self.userProfile)
        cmd = "cp %s %s/%s_BACKUP" % (
        staticConfigPath, tmpDir, 'cluster_static_config')
        (status, output) = subprocess.getstatusoutput(cmd)
        if status:
            self.logger.debug("[gs_dropnode]Backup cluster_static_config failed"
                              + output)
        backIpDict = self.context.backIpNameMap
        backIpDict_values = list(backIpDict.values())
        backIpDict_keys = list(backIpDict.keys())
        for ipLoop in self.context.hostIpListForDel:
            nameLoop = backIpDict_keys[backIpDict_values.index(ipLoop)]
            dnLoop = self.context.clusterInfo.getDbNodeByName(nameLoop)
            self.context.clusterInfo.dbNodes.remove(dnLoop)
        for dbNode in self.context.clusterInfo.dbNodes:
            if dbNode.name == self.localhostname:
                self.context.clusterInfo.saveToStaticConfig(staticConfigPath,
                                                            dbNode.id)
                continue
            staticConfigPath_dn = "%s/cluster_static_config_%s" % (
                tmpDir, dbNode.name)
            self.context.clusterInfo.saveToStaticConfig(staticConfigPath_dn,
                                                        dbNode.id)
        self.logger.debug(
            "[gs_dropnode]Start to scp the cluster static conf to any other node.")

        if not self.context.flagOnlyPrimary:
            sshtool = SshTool(self.context.clusterInfo.getClusterNodeNames())
            cmd = "%s/script/gs_om -t refreshconf" % self.gphomepath
            (status, output) = subprocess.getstatusoutput(cmd)
            for hostName in self.context.hostMapForExist.keys():
                hostSsh = SshTool([hostName])
                if hostName != self.localhostname:
                    staticConfigPath_name = "%s/cluster_static_config_%s" % (
                tmpDir, hostName)
                    hostSsh.scpFiles(staticConfigPath_name, staticConfigPath,
                                     [hostName], self.envFile)
                    try:
                        os.unlink(staticConfigPath_name)
                    except FileNotFoundError:
                        pass
                self.cleanSshToolFile(hostSsh)

        self.logger.log("[gs_dropnode]End of modify the cluster static conf.")

    def cleanSshToolFile(self, sshTool):
        """
        """
        try:
            sshTool.clenSshResultFiles()
        except Exception as e:
            self.logger.debug(str(e))

    def checkUserAndGroupExists(self):
        """
        check system user and group exists and be same 
        on primary and standby nodes
        """
        inputUser = self.user
        inputGroup = self.group
        user_group_id = ""
        isUserExits = False
        localHost = socket.gethostname()
        for user in pwd.getpwall():
            if user.pw_name == self.user:
                user_group_id = user.pw_gid
                isUserExits = True
                break
        if not isUserExits:
            GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                                   % ("User", self.user, localHost))

        isGroupExits = False
        group_id = ""
        for group in grp.getgrall():
            if group.gr_name == self.group:
                group_id = group.gr_gid
                isGroupExits = True
        if not isGroupExits:
            GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                                   % ("Group", self.group, localHost))
        if user_group_id != group_id:
            GaussLog.exitWithError("User [%s] is not in the group [%s]." \
                                   % (self.user, self.group))

        hostNames = list(self.context.hostMapForExist.keys())
        envfile = self.envFile
        sshTool = SshTool(hostNames)

        # get username in the other standy nodes
        getUserNameCmd = "cat /etc/passwd | grep -w %s" % inputUser
        resultMap, outputCollect = sshTool.getSshStatusOutput(getUserNameCmd,
                                                              [], envfile)

        for hostKey in resultMap:
            if resultMap[hostKey] == STATUS_FAIL:
                self.cleanSshToolFile(sshTool)
                GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                                       % ("User", self.user, hostKey))

        # get groupname in the other standy nodes
        getGroupNameCmd = "cat /etc/group | grep -w %s" % inputGroup
        resultMap, outputCollect = sshTool.getSshStatusOutput(getGroupNameCmd,
                                                              [], envfile)
        for hostKey in resultMap:
            if resultMap[hostKey] == STATUS_FAIL:
                self.cleanSshToolFile(sshTool)
                GaussLog.exitWithError(ErrorCode.GAUSS_357["GAUSS_35704"] \
                                       % ("Group", self.group, hostKey))
        self.cleanSshToolFile(sshTool)

    def restartInstance(self):
        if self.context.flagOnlyPrimary:
            self.logger.log("[gs_dropnode]Remove the dynamic conf.")
            dynamicConfigPath = "%s/bin/cluster_dynamic_config" % self.appPath
            try:
                os.unlink(dynamicConfigPath)
            except FileNotFoundError:
                pass
            flag = input(
                "Only one primary node is left."
                "It is recommended to restart the node."
                "\nDo you want to restart the primary node now (yes/no)? ")
            count_f = 2
            while count_f:
                if (
                        flag.upper() != "YES"
                        and flag.upper() != "NO"
                        and flag.upper() != "Y" and flag.upper() != "N"):
                    count_f -= 1
                    flag = input("Please type 'yes' or 'no': ")
                    continue
                break
            if flag.upper() != "YES" and flag.upper() != "Y":
                GaussLog.exitWithError(
                    ErrorCode.GAUSS_358["GAUSS_35805"] % flag.upper())
            sshTool = SshTool([self.localhostname])
            for i in self.context.hostMapForExist[self.localhostname]['datadir']:
                self.commonOper.stopInstance(self.localhostname, sshTool, i,
                                             self.userProfile)
                self.commonOper.startInstance(i, self.userProfile)
            self.cleanSshToolFile(sshTool)
        else:
            pass

    def run(self):
        """
        start dropnode
        """
        self.change_user()
        self.logger.log("[gs_dropnode]Start to drop nodes of the cluster.")
        self.checkAllStandbyState()
        self.dropNodeOnAllHosts()
        self.operationOnlyOnPrimary()
        self.modifyStaticConf()
        self.restartInstance()
        self.logger.log("[gs_dropnode]Success to drop the target nodes.")


class OperCommon:

    def __init__(self, dropnode):
        """
        """
        self.logger = dropnode.logger
        self.user = dropnode.user

    def checkStandbyState(self, host, dirDn, sshTool, envfile, isForDel=False):
        """
        check the existed standby node state
        Exit if the role is not standby or the state of database is not normal
        """
        sshcmd = "gs_ctl query -D %s" % dirDn
        (statusMap, output) = sshTool.getSshStatusOutput(sshcmd, [host],
                                                         envfile)
        if 'Is server running?' in output and not isForDel:
            GaussLog.exitWithError(ErrorCode.GAUSS_516["GAUSS_51651"] % host)
        elif 'Is server running?' in output and isForDel:
            return
        else:
            res = re.findall(r'db_state\s*:\s*(\w+)', output)
            if not len(res) and isForDel:
                return
            elif not len(res):
                GaussLog.exitWithError(ErrorCode.GAUSS_516["GAUSS_51651"] % host)
            dbState = res[0]
            if dbState in ['Promoting', 'Wait', 'Demoting']:
                GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35808"] % host)

    def backupConf(self, appPath, user, host, envfile, sshTool):
        """
        backup the configuration file (postgresql.conf and pg_hba.conf)
        The Backup.py can do this
        """
        self.logger.log(
            "[gs_dropnode]Start to backup parameter config file on %s." % host)
        tmpPath = '/tmp/gs_dropnode_backup' + \
                  str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        backupPyPath = os.path.join(appPath, './script/local/Backup.py')
        cmd = "(find /tmp -type d | grep gs_dropnode_backup | xargs rm -rf;" \
              "if [ ! -d '%s' ]; then mkdir -p '%s' -m %s;fi)" \
              % (tmpPath, tmpPath, DefaultValue.KEY_DIRECTORY_MODE)
        sshTool.executeCommand(cmd, "", DefaultValue.SUCCESS, [host], envfile)
        logfile = os.path.join(tmpPath, 'gs_dropnode_call_Backup_py.log')
        cmd = "python3 %s -U %s -P %s -p --nodeName=%s -l %s" \
              % (backupPyPath, user, tmpPath, host, logfile)
        (statusMap, output) = sshTool.getSshStatusOutput(cmd, [host], envfile)
        if statusMap[host] != 'Success':
            self.logger.debug(
                "[gs_dropnode]Backup parameter config file failed." + output)
            GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
        self.logger.log(
            "[gs_dropnode]End to backup parameter config file on %s." % host)
        return '%s/parameter_%s.tar' % (tmpPath, host)

    def parseConfigFile(self, host, dirDn, dnId, hostIpListForDel, sshTool,
                        envfile):
        """
        parse the postgresql.conf file and get the replication info
        """
        self.logger.log(
            "[gs_dropnode]Start to parse parameter config file on %s." % host)
        resultDict = {'replStr': '', 'syncStandbyStr': '', 'pghbaStr': ''}
        pgConfName = os.path.join(dirDn, 'postgresql.conf')
        pghbaConfName = os.path.join(dirDn, 'pg_hba.conf')

        cmd = "grep -o '^replconninfo.*' %s | egrep -o '^replconninfo.*'" \
              % pgConfName
        (statusMap, output1) = sshTool.getSshStatusOutput(cmd, [host], envfile)
        if statusMap[host] != 'Success':
            self.logger.debug("[gs_dropnode]Parse replconninfo failed:" + output1)
            GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
        cmd = "grep -o '^synchronous_standby_names.*' %s" % pgConfName
        (statusMap, output) = sshTool.getSshStatusOutput(cmd, [host], envfile)
        if statusMap[host] != 'Success':
            self.logger.debug(
                "[gs_dropnode]Parse synchronous_standby_names failed:" + output)
            GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
        output_v = output.split("'")[-2]
        if output_v == '*':
            resultDict['syncStandbyStr'] = output_v
        else:
            resultDict['syncStandbyStr'] = self.check_syncStandbyStr(dnId,
                                                                     output_v)

        cmd = "grep '^host.*trust' %s" % pghbaConfName
        (statusMap, output) = sshTool.getSshStatusOutput(cmd, [host], envfile)
        if statusMap[host] != 'Success':
            self.logger.debug("[gs_dropnode]Parse pg_hba file failed:" + output)
        for ip in hostIpListForDel:
            if ip in output1:
                i = output1.rfind('replconninfo', 0, output1.find(ip)) + 12
                resultDict['replStr'] += output1[i]
            if ip in output:
                s = output.rfind('host', 0, output.find(ip))
                e = output.find('\n', output.find(ip), len(output))
                resultDict['pghbaStr'] += output[s:e] + '|'
        self.logger.log(
            "[gs_dropnode]End to parse parameter config file on %s." % host)
        return resultDict

    def check_syncStandbyStr(self, dnlist, output):
        output_no = '0'
        output_result = output
        output_new_no = '1'
        if 'ANY' in output or 'FIRST' in output:
            output_dn = re.findall(r'\((.*)\)', output)[0]
            output_no = re.findall(r'.*(\d) *\(.*\)', output)[0]
        else:
            output_dn = output
        output_dn_nospace = re.sub(' *', '', output_dn)
        init_no = len(output_dn_nospace.split(','))
        quorum_no = int(init_no / 2) + 1
        half_no = quorum_no - 1
        count_dn = 0
        list_output1 = '*'
        for dninst in dnlist:
            if dninst in output_dn_nospace:
                list_output1 = output_dn_nospace.split(',')
                list_output1.remove(dninst)
                list_output1 = ','.join(list_output1)
                output_dn_nospace = list_output1
                init_no -= 1
                count_dn += 1
        if count_dn == 0 or list_output1 == '':
            return ''
        if list_output1 != '*':
            output_result = output.replace(output_dn, list_output1)
        if output_no == '0':
            return output_result
        if int(output_no) == quorum_no:
            output_new_no = str(int(init_no / 2) + 1)
            output_result = output_result.replace(output_no, output_new_no, 1)
            return output_result
        elif int(output_no) > half_no and (int(output_no) - count_dn) > 0:
            output_new_no = str(int(output_no) - count_dn)
        elif int(output_no) > half_no and (int(output_no) - count_dn) <= 0:
            output_new_no = '1'
        elif int(output_no) < half_no and int(output_no) <= init_no:
            output_new_no = output_no
        elif half_no > int(output_no) > init_no:
            output_new_no = str(init_no)
        output_result = output_result.replace(output_no, output_new_no, 1)
        return output_result

    def parseBackupFile(self, host, backupfile, dnId, replstr, sshTool,
                        envfile):
        """
        parse the backup file eg.parameter_host.tar to get the value for rollback
        """
        self.logger.log(
            "[gs_dropnode]Start to parse backup parameter config file on %s." % host)
        resultDict = {'rollbackReplStr': '', 'syncStandbyStr': ''}
        backupdir = os.path.dirname(backupfile)
        cmd = "tar xf %s -C %s;grep -o '^replconninfo.*' %s/%s/%s_postgresql.conf;" \
              "grep -o '^synchronous_standby_names.*' %s/%s/%s_postgresql.conf;" \
              % (
              backupfile, backupdir, backupdir, 'parameter_' + host, dnId[3:],
              backupdir, 'parameter_' + host, dnId[3:])
        (statusMap, output) = sshTool.getSshStatusOutput(cmd, [host], envfile)
        if statusMap[host] != 'Success':
            self.logger.log(
                "[gs_dropnode]Parse backup parameter config file failed:" + output)
            GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
        for i in replstr:
            tmp_v = 'replconninfo' + i
            s = output.index(tmp_v)
            e = output.find('\n', s, len(output))
            resultDict['rollbackReplStr'] += output[s:e].split("'")[-2] + '|'
        s = output.index('synchronous_standby_names')
        resultDict['syncStandbyStr'] = output[s:].split("'")[-2]
        self.logger.log(
            "[gs_dropnode]End to parse backup parameter config file %s." % host)
        return resultDict

    def SetPgsqlConf(self, replNo, host, syncStandbyValue, sshTool, envfile,
                     port, replValue='', singleLeft=False):
        """
        Set the value of postgresql.conf
        """
        self.logger.log(
            "[gs_dropnode]Start to set postgresql config file on %s." % host)
        sqlExecFile = '/tmp/gs_dropnode_sqlExecFile_' + \
                      str(datetime.datetime.now().strftime(
                          '%Y%m%d%H%M%S')) + host
        checkResultFile = '/tmp/gs_dropnode_sqlResultFile_' + \
                          str(datetime.datetime.now().strftime(
                              '%Y%m%d%H%M%S')) + host
        sqlvalue = ''
        if not replValue and replNo != '':
            for i in replNo:
                sqlvalue += "ALTER SYSTEM SET replconninfo%s = '';" % i
        if len(replValue) > 0:
            count = 0
            for i in replNo:
                sqlvalue += "ALTER SYSTEM SET replconninfo%s = '%s';" % (
                i, replValue[:-1].split('|')[count])
                count += 1
        if not singleLeft and syncStandbyValue != '':
            sqlvalue += "ALTER SYSTEM SET synchronous_standby_names = '%s';" \
                        % syncStandbyValue
        if singleLeft:
            sqlvalue += "ALTER SYSTEM SET synchronous_standby_names = '';"
        if sqlvalue != '':
            cmd = "touch %s && chmod %s %s" % \
                  (sqlExecFile, DefaultValue.MAX_DIRECTORY_MODE, sqlExecFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                self.logger.log(
                    "[gs_dropnode]Create the SQL command file failed:" + output)
                GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
            try:
                with os.fdopen(
                    os.open("%s" % sqlExecFile, os.O_WRONLY | os.O_CREAT,
                                stat.S_IWUSR | stat.S_IRUSR), 'w') as fo:
                    fo.write(sqlvalue)
                    fo.close()
            except Exception as e:
                cleanFile(sqlExecFile)
                raise SQLCommandException(sqlExecFile,
                                          "write into sql query file failed. "
                                          + str(e))
            self.logger.debug(
                "[gs_dropnode]Start to send the SQL command file to all hosts.")
            sshTool.scpFiles(sqlExecFile, '/tmp', [host])
            cmd = "gsql -p %s -d postgres -f %s --output %s;cat %s" % (
            port, sqlExecFile, checkResultFile, checkResultFile)
            (statusMap, output) = sshTool.getSshStatusOutput(cmd, [host], envfile)
            if "ERROR" in output:
                self.logger.debug(
                    "[gs_dropnode]Failed to execute the SQL command file on all "
                    "hosts:" + output)
                raise ValueError(output)
            cmd = "ls /tmp/gs_dropnode_sql* | xargs rm -rf"
            sshTool.executeCommand(cmd, "", DefaultValue.SUCCESS, [host], envfile)
            try:
                os.unlink(sqlExecFile)
                os.unlink(checkResultFile)
            except FileNotFoundError:
                pass
        self.logger.log(
            "[gs_dropnode]End of set postgresql config file on %s." % host)

    def SetPghbaConf(self, envProfile, host, pgHbaValue,
                     flagRollback=False):
        """
        Set the value of pg_hba.conf
        """
        self.logger.log(
            "[gs_dropnode]Start of set pg_hba config file on %s." % host)
        cmd = 'source %s;' % envProfile
        if len(pgHbaValue):
            if not flagRollback:
                for i in pgHbaValue[:-1].split('|'):
                    v = i[0:i.find('/32') + 3]
                    cmd += "gs_guc set -N %s -I all -h '%s';" % (host, v)
            if flagRollback:
                for i in pgHbaValue[:-1].split('|'):
                    cmd += "gs_guc set -N %s -I all -h '%s';" \
                           % (host, i.strip())
            (status, output) = subprocess.getstatusoutput(cmd)
            result_v = re.findall(r'Failed instances: (\d)\.', output)
            if status:
                self.logger.debug(
                    "[gs_dropnode]Set pg_hba config file failed:" + output)
                raise ValueError(output)
            if len(result_v):
                if result_v[0] != '0':
                    self.logger.debug(
                        "[gs_dropnode]Set pg_hba config file failed:" + output)
                    raise ValueError(output)
            else:
                self.logger.debug(
                    "[gs_dropnode]Set pg_hba config file failed:" + output)
                raise ValueError(output)
        else:
            self.logger.log(
                "[gs_dropnode]Nothing need to do with pg_hba config file.")
        self.logger.log(
            "[gs_dropnode]End of set pg_hba config file on %s." % host)

    def get_repl_slot(self, host, ssh_tool, envfile, gsql_path, port):
        """
        Get the replication slot on primary node only
        """
        self.logger.log("[gs_dropnode]Start to get repl slot on primary node.")
        selectSQL = "SELECT slot_name,plugin,slot_type FROM pg_replication_slots;"
        querycmd = "%s -p %s postgres -A -t -c '%s'" % (gsql_path, port, selectSQL)
        (status, output) = ssh_tool.getSshStatusOutput(querycmd, [host], envfile)
        if status[host] != 'Success' or "ERROR" in output:
            self.logger.debug(
                "[gs_dropnode]Get repl slot failed:" + output)
            GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
        return ','.join(output.split('\n')[1:])

    def SetReplSlot(self, host, sshTool, envfile, gsqlPath, port, dnid,
                    replslot_output, flag_rollback=False):
        """
        Drop the replication slot on primary node only
        """
        self.logger.log("[gs_dropnode]Start to set repl slot on primary node.")
        setcmd = ''
        if not flag_rollback:
            for i in dnid:
                if i in replslot_output:
                    setcmd += "%s -p %s postgres -A -t -c \\\"SELECT pg_drop_" \
                              "replication_slot('%s');\\\";" % \
                              (gsqlPath, port, i)
        if flag_rollback:
            list_o = [i.split('|') for i in replslot_output.split(',')]
            for r in list_o:
                if r[0] in dnid and r[2] == 'physical':
                    setcmd += "%s -p %s postgres -A -t -c \\\"SELECT * FROM " \
                                  "pg_create_physical_replication_slot('%s', false);\\\";" % \
                                  (gsqlPath, port, r[0])
                elif r[0] in dnid and r[2] == 'logical':
                    setcmd += "%s -p %s postgres -A -t -c \\\"SELECT * FROM " \
                                  "pg_create_logical_replication_slot('%s', '%s');\\\";" % \
                                  (gsqlPath, port, r[0], r[1])
        if setcmd != '':
            if host == DefaultValue.GetHostIpOrName():
                setcmd = setcmd.replace("\\", '')
            (status, output) = sshTool.getSshStatusOutput(setcmd, [host], envfile)
            if status[host] != 'Success' or "ERROR" in output:
                self.logger.debug("[gs_dropnode]Set repl slot failed:" + output)
                raise ValueError(output)
        self.logger.log("[gs_dropnode]End of set repl slot on primary node.")

    def SetSyncCommit(self, dirDn):
        """
        Set the synccommit to local when only primary server be left
        """
        self.logger.log("[gs_dropnode]Start to set sync_commit on primary node.")
        command = "gs_guc set -D %s -c 'synchronous_commit = local'" % dirDn
        (status, output) = subprocess.getstatusoutput(command)
        if status or '0' not in re.findall(r'Failed instances: (\d)\.', output):
            self.logger.debug("[gs_dropnode]Set sync_commit failed:" + output)
            raise ValueError(output)
        self.logger.log("[gs_dropnode]End of set sync_commit on primary node.")

    def stopInstance(self, host, sshTool, dirDn, env):
        """
        """
        self.logger.log("[gs_dropnode]Start to stop the target node %s." % host)
        command = "source %s ; gs_ctl stop -D %s -M immediate" % (env, dirDn)
        resultMap, outputCollect = sshTool.getSshStatusOutput(command, [host],
                                                              env)
        if resultMap[host] != 'Success':
            self.logger.debug(outputCollect)
            self.logger.log(
                "[gs_dropnode]Cannot connect the target node %s." % host)
            self.logger.log(
                "[gs_dropnode]It may be still running.")
            return
        self.logger.log("[gs_dropnode]End of stop the target node %s." % host)

    def startInstance(self, dirDn, env):
        """
        """
        self.logger.log("[gs_dropnode]Start to start the target node.")
        start_retry_num = 1
        command = "source %s ; gs_ctl start -D %s" % (env, dirDn)
        while start_retry_num <= 3:
            (status, output) = subprocess.getstatusoutput(command)
            self.logger.debug(output)
            if 'done' in output and 'server started' in output:
                self.logger.log("[gs_dropnode]End of start the target node.")
                break
            else:
                self.logger.debug("[gs_dropnode]Failed to start the node.")
                GaussLog.exitWithError(ErrorCode.GAUSS_358["GAUSS_35809"])
            start_retry_num += 1
