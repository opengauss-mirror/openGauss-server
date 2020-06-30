#!/usr/bin/env python3
#-*- coding:utf-8 -*-
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
#-------------------------------------------------------------------------
#
# KerberosUtility.py
#    KerberosUtility.py is a utility to handler kerberos things
#
# IDENTIFICATION
#    src/manager/om/script/local/KerberosUtility.py
#
#-------------------------------------------------------------------------

import sys
import os
import getopt
import subprocess
import shutil
import pwd

sys.path.append(sys.path[0] + "/../")
from gspylib.common.VersionInfo import VersionInfo
from gspylib.common.DbClusterInfo import initParserXMLFile, dbClusterInfo
from gspylib.common.Common import DefaultValue
from gspylib.common.DbClusterInfo import dbClusterInfo
from gspylib.common.GaussLog import GaussLog
from gspylib.common.ErrorCode import ErrorCode
from gspylib.threads.SshTool import SshTool
from gspylib.os.gsfile import g_file
from multiprocessing.dummy import Pool as ThreadPool

METHOD_TRUST = "trust"
BIGDATA_HOME = "$BIGDATA_HOME"
DUMMY_STANDBY_INSTANCE = 2
INSTANCE_ROLE_COODINATOR = 3
g_ignorepgHbaMiss = True
CONFIG_ITEM_TYPE = "ConfigInstance"
g_clusterInfo = None
#Gauss200 IP Hosts Mapping
GAUSS_HOSTS_MAPPING_FLAG = "#%s IP Hosts Mapping" % VersionInfo.PRODUCT_NAME
#write /etc/hosts kerberos flag
KERBEROS_HOSTS_MAPPING_FLAG = "#Kerberos IP Hosts Mapping"
VALUE_LIST = ["PGKRBSRVNAME", "KRBHOSTNAME", "MPPDB_KRB5_FILE_PATH",
              "KRB5RCACHETYPE"]
SERVER_ENV_LIST = ["KRB_HOME", "KRB5_CONFIG", "KRB5_KDC_PROFILE"]

g_logger = None
g_opts = None
g_sshTool = None


class CmdOptions():
    """
    install the cluster on local node
    """
    def __init__(self):
        self.action = ""
        self.user = ""
        self.mpprcFile = ""
        self.clusterInfo = None
        self.principal = ""
        self.keytab = ""
        self.dbNodeInfo = None
        self.krbHomePath = ""
        self.krbConfigPath = ""
        self.server = False
        self.client = False
        self.gausshome = ""
        self.gausshome_kerberso = ""


def initGlobals():
    """
    init global variables
    input : NA
    output: NA
    """
    global g_opts
    logFile = DefaultValue.getOMLogPath(DefaultValue.LOCAL_LOG_FILE,
                                        g_opts.user, "")

    # Init logger
    global g_logger
    g_logger = GaussLog(logFile, "KerberosUtility")

    global g_clusterInfo
    # init for clusterInfo
    g_clusterInfo = dbClusterInfo()
    g_clusterInfo.initFromStaticConfig(g_opts.user)
    g_logger.debug("Cluster information: \n%s." % str(g_clusterInfo))

    global g_sshTool
    nodenames = g_clusterInfo.getClusterNodeNames()
    g_sshTool = SshTool(nodenames)

    try:
        # init for __clusterInfo and __dbNodeInfo
        g_opts.clusterInfo = g_clusterInfo
        hostName = DefaultValue.GetHostIpOrName()
        g_opts.dbNodeInfo = g_clusterInfo.getDbNodeByName(hostName)

        #get env variable file
        g_opts.mpprcFile = DefaultValue.getMpprcFile()

        # create kerberso directory under GAUSSHOME
        gausshome = DefaultValue.getInstallDir(g_opts.user)
        if not gausshome:
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] % "GAUSSHOME")
        g_opts.gausshome = gausshome
        g_opts.gausshome_kerberso = os.path.join(gausshome, "kerberos")
        if not os.path.isdir(g_opts.gausshome_kerberso):
            dir_permission = 0o700
            os.makedirs(g_opts.gausshome_kerberso, mode=dir_permission)

        if g_opts.action == "install" and g_opts.server:
            g_logger.debug("%s the kerberos server.", g_opts.action)

        else:
            if g_opts.action == "uninstall":
                g_logger.debug("%s the kerberos tool.", g_opts.action)
            else:
                g_logger.debug("%s the kerberos client.", g_opts.action)
            tablespace = DefaultValue.getEnv("ELK_SYSTEM_TABLESPACE")
            if tablespace is not None and tablespace != "":
                xmlfile = os.path.join(os.path.dirname(g_opts.mpprcFile),
                                       DefaultValue.FI_ELK_KRB_XML)
            else:
                xmlfile = os.path.join(os.path.dirname(g_opts.mpprcFile),
                                       DefaultValue.FI_KRB_XML)
            xmlfile = os.path.realpath(xmlfile)
            if not os.path.isfile(xmlfile):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % xmlfile)

            rootNode = initParserXMLFile(xmlfile)
            elementArray = rootNode.findall("property")

            for element in elementArray:
                if (element.find('name').text == "mppdb.kerberos.principal"
                    or element.find('name').text == "elk.kerberos.principal"):
                    g_opts.principal = element.find('value').text
                if (element.find('name').text == "mppdb.kerberos.keytab"
                    or element.find('name').text == "elk.kerberos.keytab"):
                    g_opts.keytab = element.find('value').text
                if (element.find('name').text == 'KRB_HOME'):
                    g_opts.krbHomePath = element.find('value').text
                if (element.find('name').text == 'KRB_CONFIG'):
                    g_opts.krbConfigPath = element.find('value').text

            if(g_opts.principal == "" or g_opts.keytab == ""
                    or g_opts.krbHomePath == "" or g_opts.krbConfigPath == ""):
                raise Exception(ErrorCode.GAUSS_512["GAUSS_51200"] %
                                "mppdb.kerberos.principal or "
                                "mppdb.kerberos.keytab"
                                " or KRB_HOME or KRB_CONFIG"
                                + " The xml file is %s." % xmlfile)

    except Exception as e:
        g_logger.logExit(str(e))
    g_logger.debug("Instance information on local node:\n%s."
                   % str(g_opts.dbNodeInfo))


class Kerberos():
    def __init__(self):
        self.__dbNodeInfo = None
        self.__allIps = []
        self.__cooConfig = {}
        self.__dataConfig = {}
        self.__gtmConfig = {}
        self.__cmsConfig = {}
        self.__IpStringList = []
        self.__DNStringList = []

    def __rollback(self, isServer):
        g_logger.log("An error happened in executing the command, "
                     "begin rollback work...")
        if isServer:
            self.__rollbackServerInstall()
        else:
            self.__uninstall(True)
        g_logger.log("rollback work complete.")

    def __rollbackServerInstall(self):
        if os.path.isdir(g_opts.gausshome_kerberso):
            shutil.rmtree(g_opts.gausshome_kerberso)
        self.__clearEnvironmentVariableValue(True)
        self.__cleanAuthConfig()
        self.__cleanServer()


    def __triggerJob(self, isUninstall, isServer=False):
        '''
        function: triggerJob for call kinit
        '''
        if(not isUninstall):
            g_logger.log("start triggerJob.")
            if isServer:
                self.__initUser()
                self.__startServer()
                self.__distributeKeyAndSite()
                self.__setServiceCron()
            else:
                self.__executeJob()
            g_logger.log("successfully start triggerJob.")
        else:
            g_logger.log("stop triggerJob.")
            self.__cancelCron()
            g_logger.log("successfully stop triggerJob.")

    def __clearEnvironmentVariableValue(self, isServer=False):
        """
        function: clear kerberos EnvironmentVariable
        input: isServer
        output: NA
        """
        for value in VALUE_LIST:
            cmd = "sed -i -e '/^.*%s=/d' '%s'" % (value, g_opts.mpprcFile)
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + "Error:\n%s" % output)
        if isServer:
            for value in SERVER_ENV_LIST:
                cmd = "sed -i -e '/^.*%s=/d' '%s' && " \
                      "sed -i -e '/^.*PATH=\$%s/d' '%s' && " \
                      "sed -i -e '/^.*LD_LIBRARY_PATH=\$%s/d' '%s'" % \
                      (value, g_opts.mpprcFile, value, g_opts.mpprcFile,
                       value, g_opts.mpprcFile)
                (status, output) = subprocess.getstatusoutput(cmd)
                if(status != 0):
                    raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                    + "Error:\n%s" % output)

        g_logger.log("successfully clear kerberos env Variables.")

    def __setUserEnvVariable(self, isUninstall, isServer=False,
                             isRollBack=False):
        '''
        function: set user env Variable
        '''
        g_logger.log("start set user env Variable.")
        if(not isUninstall):
            try:
                if isServer:
                    self.__clearEnvironmentVariableValue(True)

                    # SET variable KRB_HOME
                    cmd = "echo \"export KRB_HOME=%s\" >> %s" % \
                          (g_opts.gausshome, g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"] %
                                        "KRB_HOME" + output
                                        + "\nThe cmd is %s " % cmd)
                    g_logger.log("Config environment variable KRB_HOME "
                                 "successfully.")

                    # SET variable KRB5_CONFIG
                    cmd = "echo \"export KRB5_CONFIG=%s/krb5.conf\" >> %s" % \
                          (g_opts.gausshome_kerberso, g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"] %
                                        "KRB5_CONFIG" + output
                                        + "\nThe cmd is %s " % cmd)
                    g_logger.log("Config environment variable KRB5_CONFIG "
                                 "successfully.")

                    # SET variable KRB5_KDC_PROFILE
                    cmd = "echo \"export KRB5_KDC_PROFILE=%s/kdc.conf\" " \
                          ">> %s" \
                          % (g_opts.gausshome_kerberso, g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"] %
                                        "KRB5_KDC_PROFILE" + output
                                        + "\nThe cmd is %s " % cmd)
                    g_logger.log("Config environment "
                                 "variable KRB5_KDC_PROFILE successfully.")

                    # SET variable PATH
                    cmd = "echo \"export PATH=\$KRB_HOME/bin:\$PATH\" " \
                          ">> %s" % (g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"] %
                                        "PATH" + output
                                        + "\nThe cmd is %s " % cmd)
                    g_logger.log("Config environment variable PATH "
                                 "successfully.")

                    # SET variable LD_LIBRARY_PATH
                    cmd = "echo \"export LD_LIBRARY_PATH=\$KRB_HOME/lib:" \
                          "\$LD_LIBRARY_PATH\" >> %s" % (g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"] %
                                        "LD_LIBRARY_PATH" + output
                                        + "\nThe cmd is %s " % cmd)
                    g_logger.log("Config environment variable LD_LIBRARY_PATH "
                                 "successfully.")

                else:
                    # get principal
                    principals = g_opts.principal.split("/")
                    if (len(g_opts.principal.split('/')) < 2):
                        raise Exception(ErrorCode.GAUSS_500["GAUSS_50009"]
                                        + "principal: %s" % g_opts.principal)
                    address = g_opts.principal.split('/')[1].split('@')[0]

                    self.__clearEnvironmentVariableValue()
                    # SET variable KRB5_CONFIG
                    cmd = "echo \"export MPPDB_KRB5_FILE_PATH=%s/krb5.conf\"" \
                          " >> %s" % (os.path.dirname(g_opts.mpprcFile),
                                     g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                        % cmd + "Error:\n%s." % output)
                    g_logger.log("Config environment variable KRB5_CONFIG "
                                 "successfully.")
                    # SET variable PGKRBSRVNAME

                    cmd = "echo \"export PGKRBSRVNAME=%s\" >>%s" % \
                          (principals[0], g_opts.mpprcFile)

                    (status, output) = subprocess.getstatusoutput(cmd)
                    if (status != 0):
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                        % cmd + "Error:\n%s." % output)
                    g_logger.log("Config environment variable PGKRBSRVNAME "
                                 "successfully.")
                    # SET variable KRBHOSTNAME

                    cmd = "echo \"export KRBHOSTNAME=%s\" >>%s" % \
                          (address, g_opts.mpprcFile)

                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                        % cmd + "Error:\n%s." % output)
                    g_logger.log("Config environment variable KRBHOSTNAME "
                                 "successfully.")
                    # SET variable KRB5RCACHETYPE
                    cmd = "echo \"export KRB5RCACHETYPE=none\" >>%s" % \
                          (g_opts.mpprcFile)
                    (status, output) = subprocess.getstatusoutput(cmd)
                    if status != 0:
                        raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"]
                                        % cmd + "Error:\n%s." % output)
                    g_logger.log("Config environment variable KRB5RCACHETYPE "
                                 "successfully.")
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_518["GAUSS_51804"]
                                % "" + "Error:%s." % str(e))
        else:
            if isRollBack:
                self.__clearEnvironmentVariableValue(False)
            else:
                self.__clearEnvironmentVariableValue(True)

    def __configPostgresql(self, isUninstall):
        '''
        function: set config postgresql file
        '''
        g_logger.log("start set config postgresql file")

        instanceList = []
        if(not isUninstall):
            self.__cooConfig["krb_server_keyfile"] = "'" + g_opts.keytab + "'"
            self.__dataConfig["krb_server_keyfile"] = "'" + g_opts.keytab + "'"
            self.__gtmConfig["gtm_authentication_type"] = "gss"
            self.__gtmConfig["gtm_krb_server_keyfile"] = "'" + g_opts.keytab \
                                                         + "'"
            self.__cmsConfig["cm_auth_method"] = "gss"
            self.__cmsConfig["cm_krb_server_keyfile"] = "'" + g_opts.keytab + \
                                                        "'"
        else:
            self.__cooConfig["krb_server_keyfile"] = ""
            self.__dataConfig["krb_server_keyfile"] = ""
            self.__gtmConfig["gtm_authentication_type"] = "trust"
            self.__gtmConfig["gtm_krb_server_keyfile"] = "''"
            self.__cmsConfig["cm_auth_method"] = "trust"
            self.__cmsConfig["cm_krb_server_keyfile"] = "''"

        # get coordinators instance
        for cooInst in g_opts.dbNodeInfo.coordinators:
            instanceList.append(cooInst)
        # get datanode instance
        for dnInst in g_opts.dbNodeInfo.datanodes:
            instanceList.append(dnInst)
        # get gtm instance
        for gtmInst in g_opts.dbNodeInfo.gtms:
            instanceList.append(gtmInst)
        # get cms instance
        for cmsInst in g_opts.dbNodeInfo.cmservers:
            instanceList.append(cmsInst)

        if(len(instanceList) == 0):
            return
        try:
            #config instance in paralle
            pool = ThreadPool(DefaultValue.getCpuSet())
            pool.map(self.__configInst, instanceList)
            pool.close()
            pool.join()
        except Exception as e:
            raise Exception(str(e))
        g_logger.log("successfully set config postgresql file")

    def __configPgHba(self, isUninstall):
        '''
        set pg_hba.conf file
        '''
        g_logger.log("start config pg_hba file")
        try:
            # get current node information
            hostName = DefaultValue.GetHostIpOrName()
            self.__dbNodeInfo = g_clusterInfo.getDbNodeByName(hostName)
            if (self.__dbNodeInfo is None):
                raise Exception(ErrorCode.GAUSS_516["GAUSS_51620"] % "local"
                                + " There is no host named %s." % hostName)
            #getall node names
            nodenames = g_clusterInfo.getClusterNodeNames()
            for nodename in nodenames:
                nodeinfo = g_clusterInfo.getDbNodeByName(nodename)
                self.__allIps += nodeinfo.backIps
                self.__allIps += nodeinfo.sshIps
                for inst in nodeinfo.cmservers:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.coordinators:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.datanodes:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
                for inst in nodeinfo.gtms:
                    self.__allIps += inst.haIps
                    self.__allIps += inst.listenIps
            # set local ip 127.0.0.1
            self.__allIps += ['127.0.0.1']
            # get all ips. Remove the duplicates ips
            self.__allIps = DefaultValue.Deduplication(self.__allIps)
            # build ip string list
            #set Kerberos ip
            principals = g_opts.principal.split("/")
            principals = principals[1].split("@")
            # Every 1000 records merged into one"
            ipstring = ""
            j = 0
            for ip in self.__allIps:
                j += 1
                if not isUninstall:
                    ipstring += " -h 'host    all             all       " \
                                "      %s/32        gss         " \
                                "include_realm=1        krb_realm=%s'" % \
                                (ip, principals[1])
                else:
                    ipstring += " -h 'host    all             all       " \
                                "      %s/32       %s'" % (ip, METHOD_TRUST)
            if ipstring != "":
                self.__IpStringList.append(ipstring)
            #write config hba
            self.__writeConfigHba()
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_530["GAUSS_53024"]
                            + "Error:%s." % str(e))

        g_logger.debug("Instance information about local node:\n%s." %
                       str(self.__dbNodeInfo))
        g_logger.log("successfully config pg_hba file")

    def __configDNPgHba(self, isUninstall):
        '''
        set DN pg_hba.conf file for replication channel
        '''
        g_logger.log("start config pg_hba file for database node replication "
                     "channel")
        try:
            principals = g_opts.principal.split("/")
            principals = principals[1].split("@")
            ipstring = ""
            if (not isUninstall):
                ipstring += " -h 'host    replication             %s        " \
                            "     ::1/128        gss         include_realm=1" \
                            "        krb_realm=%s'" % \
                            (g_opts.user, principals[1])
            else:
                ipstring += " -h 'host    replication             %s        " \
                            "     ::1/128        %s'" % \
                            (g_opts.user, METHOD_TRUST)
            if (ipstring != ""):
                self.__DNStringList.append(ipstring)
            self.__writeDNConfigHba()
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"]
                            % ("database node config for pg_hba.conf. %s"
                               % str(e)))
        g_logger.log("successfully config pg_hba file for database node "
                     "replication channel")

    def __configInst(self, dbInst):
        """
        function: Modify a parameter of postgresql.conf
        input : typename, datadir, configFile, parmeterDict
        output: NA
        """
        configFile = os.path.join(dbInst.datadir, "postgresql.conf")
        if (dbInst.instanceRole != DefaultValue.INSTANCE_ROLE_GTM and
            dbInst.instanceRole != DefaultValue.INSTANCE_ROLE_CMSERVER and
            not os.path.isfile(configFile)):
            return
        if dbInst.instanceRole == DefaultValue.INSTANCE_ROLE_COODINATOR:
            # modifying CN configuration.
            g_logger.log("Modify CN %s configuration." % dbInst.instanceId)
            configFile = os.path.join(dbInst.datadir, "postgresql.conf")
            # Set default value for each inst
            tempCommonDict =  self.__cooConfig
            self.__setConfigItem(DefaultValue.INSTANCE_ROLE_COODINATOR,
                                 dbInst.datadir, tempCommonDict)

        if dbInst.instanceRole == DefaultValue.INSTANCE_ROLE_DATANODE:
            # modifying database node configuration.
            g_logger.log("Modify database node %s configuration."
                         % dbInst.instanceId)
            # Set default value for each inst
            tempCommonDict = self.__dataConfig
            try:
                self.__setConfigItem(DefaultValue.INSTANCE_ROLE_DATANODE,
                                     dbInst.datadir, tempCommonDict)
            except Exception as e:
                raise Exception(str(e))

        if dbInst.instanceRole == DefaultValue.INSTANCE_ROLE_GTM:
            # modifying GTM configuration.
            g_logger.log("Modify GTM %s configuration." % dbInst.instanceId)
            # Set default value for each inst
            tempCommonDict = self.__gtmConfig
            try:
                self.__setConfigItem(DefaultValue.INSTANCE_ROLE_GTM,
                                     dbInst.datadir, tempCommonDict)
            except Exception as e:
                raise Exception(str(e))

        if dbInst.instanceRole == DefaultValue.INSTANCE_ROLE_CMSERVER:
            # modifying CMSERVER configuration.
            g_logger.log("Modify CMserver %s configuration."
                         % dbInst.instanceId)
            # Set default value for each inst
            tempCommonDict = self.__cmsConfig
            try:
                self.__setConfigItem(DefaultValue.INSTANCE_ROLE_CMSERVER,
                                     dbInst.datadir, tempCommonDict)
            except Exception as e:
                raise Exception(str(e))

    def __setConfigItem(self, typename, datadir, parmeterDict):
        """
        function: Modify a parameter
        input : typename, datadir, parmeterDict
        output: NA
        """
        # build GUC parameter string
        gucstr = ""
        for entry in list(parmeterDict.items()):
            if entry[1] == "":
                gucstr += " -c \"%s\"" % (entry[0])
            else:
                gucstr += " -c \"%s=%s\"" % (entry[0], entry[1])
        # check the GUC parameter string
        if gucstr == "":
            return
        if typename == DefaultValue.INSTANCE_ROLE_DATANODE or \
                typename == DefaultValue.INSTANCE_ROLE_COODINATOR:
            cmd = "source '%s'; gs_guc set -D %s %s" % \
                  (g_opts.mpprcFile, datadir, gucstr)
            DefaultValue.retry_gs_guc(cmd)
            if self.__gsdbStatus():
                cmd = "source '%s'; gs_guc reload -D %s %s" % \
                      (g_opts.mpprcFile, datadir, gucstr)
                try:
                    DefaultValue.retry_gs_guc(cmd)
                except Exception as e:
                    raise Exception(str(e))
        else:
            cmd = "source '%s'; gs_guc set -N all -I all %s" % \
                  (g_opts.mpprcFile, gucstr)
            DefaultValue.retry_gs_guc(cmd)

    def __gsdbStatus(self):
        """
        function: get gaussdb process
        input: NA
        output: True/False
        """
        cmd = "ps ux | grep -v '\<grep\>' | grep '%s/bin/gaussdb'" % \
              g_clusterInfo.appPath
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0 and output:
            raise Exception("Get gaussdb process failed." +
                            "The cmd is %s " % cmd)
        if output:
            return True
        return False

    def __writeConfigHba(self):
        """
        function: set hba config
        input : NA
        output: NA
        """
        instances = self.__dbNodeInfo.datanodes + \
                    self.__dbNodeInfo.coordinators
        #Determine whether this node containing CN, DN instance
        if(len(instances) == 0):
            g_logger.debug("The count number of coordinator and "
                           "datanode on local node is zero.")
            return
        try:
            pool = ThreadPool(DefaultValue.getCpuSet())
            pool.map(self.__configAnInstance, instances)
            pool.close()
            pool.join()
        except Exception as e:
            raise Exception(str(e))

    def __writeDNConfigHba(self):
        """
        function: set hba config for dn replication channel
        input : NA
        output: NA
        """
        instances = self.__dbNodeInfo.datanodes
        if (len(instances) == 0):
            g_logger.debug("The count number of datanode "
                           "on local node is zero.")
            return
        try:
            pool = ThreadPool(DefaultValue.getCpuSet())
            pool.map(self.__configAnInstanceHA, instances)
            pool.close()
            pool.join()
        except Exception as e:
            raise Exception(str(e))

    def __configAnInstance(self, instance):
        # check instance data directory
        if (instance.datadir == "" or not os.path.isdir(instance.datadir)):
            if(g_ignorepgHbaMiss):
                g_logger.debug("Failed to obtain data directory of "
                               "the instance[%s]." % str(instance))
                return
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                                ("data directory of the instance[%s]" %
                                 str(instance)))
        # check pg_hba.conf
        hbaFile = "%s/pg_hba.conf" % instance.datadir
        if(g_ignorepgHbaMiss and not os.path.isfile(hbaFile)):
            g_logger.debug("The %s does not exist." % hbaFile)
            return
        # do gs_guc to add host into pg_hba.conf
        self.__addHostToFile(instance.datadir)

    def __configAnInstanceHA(self, instance):
        instanceRole = "datanode"
        # check instance data directory
        if (instance.datadir == "" or not os.path.isdir(instance.datadir)):
            if(g_ignorepgHbaMiss):
                g_logger.debug("Failed to obtain data directory "
                               "of the instance[%s]." % str(instance))
                return
            else:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50219"] %
                                ("data directory of the instance[%s]"
                                 % str(instance)))
        # check pg_hba.conf
        hbaFile = "%s/pg_hba.conf" % instance.datadir
        if(g_ignorepgHbaMiss and not os.path.isfile(hbaFile)):
            g_logger.debug("The %s does not exist." % hbaFile)
            return
        # do gs_guc to add host into pg_hba.conf
        self.__addDNhostToFile(instanceRole, instance.datadir)

    def __addHostToFile(self, instanceDataPath):
        # do gs_guc to add host into pg_hba.conf
        for IpString in self.__IpStringList:
            cmd = "source '%s';gs_guc set -D %s %s" % (g_opts.mpprcFile,
                                                       instanceDataPath,
                                                       IpString)
            DefaultValue.retry_gs_guc(cmd)

    def __addDNhostToFile(self, dbInstanceRole, instanceDataPath):
        """
        function: set DN config postgresql file for replication channel
        input:dbInstanceRole, instanceDataPath
        output:NA
        """
        if (dbInstanceRole == "datanode"):
            for IpDNString in self.__DNStringList:
                cmd = "source '%s';gs_guc set -D %s %s" % \
                      (g_opts.mpprcFile, instanceDataPath, IpDNString)
                DefaultValue.retry_gs_guc(cmd)

    def __executeJob(self):
        """
        function:call TGT from kinit's tool
        input:NA
        output:NA
        """
        try:
            kinitPath = "%s/bin/kinit" % g_opts.krbHomePath
            kcmd = 'export LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH;' \
                   'export KRB5_CONFIG=$MPPDB_KRB5_FILE_PATH;%s -k -t %s %s' \
                   % (g_opts.krbHomePath, kinitPath,
                      g_opts.keytab, g_opts.principal)
            cmd = 'source %s; %s' % (g_opts.mpprcFile, kcmd)
            (status, output) = DefaultValue.retryGetstatusoutput(cmd)
            if(status != 0):
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd
                                + " Output: \n%s" % str(output))
            g_logger.debug("Get ticket successfully.")
            #set cron
            self.__setCron()
        except Exception as e:
            raise Exception("Call TGT from kinit's tool: %s." % cmd +
                            " Exception: \n%s" % str(e))

    def __setCron(self):
        """
        function: Set linux cron
        input : NA
        output: NA
        """
        g_logger.log("Set CRON.")
        cronFile = "%s/gauss_cron_%d" % \
                   (DefaultValue.getTmpDirFromEnv(g_opts.user), os.getpid())
        setCronCmd = "crontab -l > %s && " % cronFile
        setCronCmd += "sed -i '/^.*kinit.*$/d' '%s'; " % cronFile
        setCronCmd += '''echo '*/1 * * * * source '%s';''' \
                      '''export LD_LIBRARY_PATH='%s'/lib:$LD_LIBRARY_PATH;''' \
                      '''export KRB5_CONFIG=$MPPDB_KRB5_FILE_PATH ''' % \
                      (g_opts.mpprcFile, g_opts.krbHomePath)
        setCronCmd += '''klistcmd="'%s'/bin/klist";''' % (g_opts.krbHomePath)
        setCronCmd += '''kinitcmd="'%s'/bin/kinit -k -t %s %s ";''' % \
                      (g_opts.krbHomePath, g_opts.keytab, g_opts.principal)
        setCronCmd += '''klistresult=`$klistcmd>>/dev/null 2>&1;echo $?`;'''
        setCronCmd += '''if [ $klistresult -ne 0 ];then `$kinitcmd`;else ''' \
                      '''expiresTime=`$klistcmd|grep krbtgt|awk -F "  " ''' \
                      '''"{print \\\\\\$2}"`;startTime=`$klistcmd|grep ''' \
                      '''krbtgt|awk -F "  " "{print \\\\\\$1}"`;''' \
                      '''currentTime=`date +\%%s`;currentTime=''' \
                      '''`date +\%%s`;if [ $[`date -d "$expiresTime"''' \
                      ''' +\%%s`-$currentTime] -le 300 ] || [ $[`date -d''' \
                      ''' "$startTime" +\%%s`-$currentTime] -ge 0 ];''' \
                      '''then `$kinitcmd`;fi;fi;>>/dev/null 2>&1''' \
                      '''& ' >> %s ;''' % (cronFile)
        setCronCmd += "crontab %s&&" % cronFile
        setCronCmd += "rm -f '%s'" % cronFile

        g_logger.debug("Command for setting CRON: %s" % setCronCmd)
        (status, output) = subprocess.getstatusoutput(setCronCmd)
        if(status != 0):
            raise Exception(ErrorCode.GAUSS_508["GAUSS_50801"]
                            + " Error: \n%s." % str(output)
                            + "The cmd is %s " % setCronCmd)

        cmd = "source %s;export LD_LIBRARY_PATH=%s/lib:$LD_LIBRARY_PATH;" \
              "export KRB5_CONFIG=$MPPDB_KRB5_FILE_PATH;" % \
              (g_opts.mpprcFile, g_opts.krbHomePath)
        cmd += "klistcmd='%s/bin/klist';" % (g_opts.krbHomePath)
        cmd += "kinitcmd='%s/bin/kinit -k -t %s %s';" % \
               (g_opts.krbHomePath, g_opts.keytab, g_opts.principal)
        cmd += "klistresult=`$klistcmd>>/dev/null 2>&1;echo $?`;"
        cmd += "if [ $klistresult -ne 0 ];then `$kinitcmd`;" \
               "else expiresTime=`$klistcmd|grep krbtgt|" \
               "awk -F '  ' '{print \$2}'`;" \
               "startTime=`$klistcmd|grep krbtgt|awk -F '  ' '{print \$1}'`;" \
               "currentTime=`date +\%s`;currentTime=`date +\%s`;" \
               "if [ $[`date -d $expiresTime +\%s`-$currentTime] -le 300 ] " \
               "|| [ $[`date -d $startTime +\%s`-$currentTime] -ge 0 ];" \
               "then `$kinitcmd`;fi;fi;>>/dev/null 2>&1"
        (status, output) = subprocess.getstatusoutput(cmd)
        if(status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd  +
                            "Error:\n%s" % str(output))

        g_logger.log("Successfully Set CRON.")


    def __setServiceCron(self):
        g_logger.log("Set CRON.")
        cronFile = "%s/gauss_cron_%d" % \
                   (DefaultValue.getTmpDirFromEnv(g_opts.user), os.getpid())
        setCronCmd = "crontab -l > %s && " % cronFile
        setCronCmd += "sed -i '/^.*krb5kdc.*$/d' '%s'; " % cronFile
        setCronCmd += '''echo "*/1 * * * * source %s; ''' \
                      '''kdc_pid_list=\`ps ux | grep -E krb5kdc| ''' \
                      '''grep -v grep | awk '{print \\\\\\$2}'\` && ''' \
                      '''(if [ X\"\$kdc_pid_list\" == X\"\" ]; ''' \
                      '''then krb5kdc; fi) " >> %s; ''' % \
                      (g_opts.mpprcFile, cronFile)
        setCronCmd += "crontab %s && " % cronFile
        setCronCmd += "rm -f '%s'" % cronFile

        g_logger.debug("Command for setting CRON: %s" % setCronCmd)
        (status, output) = subprocess.getstatusoutput(setCronCmd)
        if(status != 0):
            raise Exception(ErrorCode.GAUSS_508["GAUSS_50801"]
                            + " Error: \n%s." % str(output)
                            + "The cmd is %s " % setCronCmd)

        g_logger.log("Successfully Set CRON.")

    def __cancelCron(self):
        """
        function: clean kerberos_monitor process and delete cron
        input : NA
        output: NA
        """
        g_logger.log("Deleting kerberos monitor.")
        try:
            # Remove cron
            crontabFile = "%s/gauss_crontab_file_%d" % \
                          (DefaultValue.getTmpDirFromEnv(g_opts.user),
                           os.getpid())
            cmd = "crontab -l > %s; " % crontabFile
            cmd += "sed -i '/^.*kinit.*$/d' '%s'; " % crontabFile
            cmd += "crontab '%s';" % crontabFile
            cmd += "rm -f '%s'" % crontabFile

            (status, output) = subprocess.getstatusoutput(cmd)
            if (status != 0):
                #no need raise error here, user can do it manually.
                g_logger.debug("Failed to delete regular tasks. Error: \n%s"
                               "  You can do it manually." % str(output))
                g_logger.debug("The cmd is %s " % cmd)
            cmd = "source '%s';export LD_LIBRARY_PATH=%s/lib:" \
                  "$LD_LIBRARY_PATH;export KRB5_CONFIG=" \
                  "$MPPDB_KRB5_FILE_PATH;%s/bin/kdestroy" % \
                  (g_opts.mpprcFile, g_opts.krbHomePath, g_opts.krbHomePath)
            (status, output) = DefaultValue.retryGetstatusoutput(cmd)
            if (status != 0):
                g_logger.debug("Failed to delete ticket. Error: \n%s" %
                               str(output))
                g_logger.debug("The cmd is %s " % cmd)
        except Exception as e:
            raise Exception(str(e))
        g_logger.log("Successfully deleted kerberos OMMonitor.")

    def __copyConf(self, src_dir, dest_dir, file_list):
        for config_file in file_list:
            src_path = os.path.realpath(os.path.join(src_dir, config_file))
            if (not os.path.isfile(src_path)):
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % src_path)
            dest_path = os.path.realpath(os.path.join(dest_dir, config_file))
            try:
                shutil.copy(src_path, dest_path)
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50214"] % src_path)
        g_logger.log("Copy server config files successfully.")

    def __initKadm5Conf(self, dest_dir):
        kadm5_file = os.path.realpath(os.path.join(dest_dir, "kadm5.acl"))
        cmd = "sed -i 's/#realms#/HUAWEI.COM/g' %s" % kadm5_file
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % kadm5_file
                            + output)
        g_logger.log("Initialize \"kadm5.acl\" successfully.")

    def __initKrb5Conf(self, dest_dir, dest_file='krb5.conf'):
        krb5_file = os.path.realpath(os.path.join(dest_dir, dest_file))
        kdc_ip = g_opts.dbNodeInfo.backIps[0]
        kdc_port = 21732
        krb_conf = g_opts.gausshome_kerberso
        gausslog = DefaultValue.getEnvironmentParameterValue("GAUSSLOG", "")
        if not gausslog:
            raise Exception(ErrorCode.GAUSS_518["GAUSS_51802"] % "GAUSSLOG")
        cmd = "sed -i 's/#kdc_ip#/%s/g' %s && \
               sed -i 's/#kdc_ports#/%d/g' %s && \
               sed -i 's;#krb_conf#;%s;g' %s && \
               sed -i 's;#GAUSSHOME#;%s;g' %s" % \
               (kdc_ip, krb5_file,
                kdc_port, krb5_file,
                g_opts.gausshome_kerberso, krb5_file,
                gausslog, krb5_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % krb5_file
                            + output)

        kerberoslog = os.path.join(gausslog, "kerberos")
        cmd = "if [ ! -d '%s' ]; then mkdir -p '%s' -m %s; fi" % (kerberoslog,
            kerberoslog, DefaultValue.KEY_DIRECTORY_MODE)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % krb5_file
                            + output)

        g_logger.log("Initialize \"krb5.conf\" successfully.")

    def __initKdcConf(self, dest_dir):
        self.__initKrb5Conf(dest_dir, "kdc.conf")

        kdc_file = os.path.realpath(os.path.join(dest_dir, "kdc.conf"))
        cmd = "sed -i 's;#KRB_HOME#;%s;g' %s" % (g_opts.gausshome, kdc_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] % kdc_file
                            + output)

        g_logger.log("Initialize \"kdc.conf\" successfully.")

    def __initMppdbSite(self, dest_dir):
        mppdb_site_file = os.path.realpath(os.path.join(dest_dir,
                                                        "mppdb-site.xml"))
        principal = "%s/huawei.huawei.com@HUAWEI.COM " % g_opts.user
        cmd = "sed -i 's;#mppdb.kerberos.principal#;%s;g' %s" % \
              (principal, mppdb_site_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            mppdb_site_file + output)

        cmd = "sed -i 's;#KRB_HOME#;%s;g' %s" % (g_opts.gausshome,
                                                 mppdb_site_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            mppdb_site_file + output)

        kdc_conf = os.path.realpath(os.path.join(g_opts.gausshome_kerberso,
                                                 "kdc.conf"))
        cmd = "sed -i 's;#KRB_CONFIG#;%s;g' %s" % (kdc_conf, mppdb_site_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            mppdb_site_file + output)

        keytab = os.path.realpath(os.path.join(g_opts.gausshome_kerberso,
                                               "%s.keytab" % g_opts.user))
        cmd = "sed -i 's;#mppdb.kerberos.keytab#;%s;g' %s" % (keytab,
                                                              mppdb_site_file)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50205"] %
                            mppdb_site_file + output)

        g_logger.log("Initialize \"mppdb-site.xml\" successfully.")

    def __configKrb5(self, isUninstall, isServer=False):
        """
        function: config specify krb5.conf
        input:  isUninstall, isServer
        output: NA
        """
        destfile = "%s/krb5.conf" % os.path.dirname(g_opts.mpprcFile)
        if not isUninstall:
            if isServer:
                # 1.copy conf files to GAUSSHOME/kerberos
                CONFIG_LIST = ["kadm5.acl", "kdc.conf", "krb5.conf"]
                src_path = os.path.realpath(os.path.join(g_opts.gausshome,
                                                         "etc", "kerberos"))
                self.__copyConf(src_path, g_opts.gausshome_kerberso,
                                CONFIG_LIST)
                # 2.initialize conf files
                self.__initKadm5Conf(g_opts.gausshome_kerberso)
                self.__initKrb5Conf(g_opts.gausshome_kerberso)
                self.__initKdcConf(g_opts.gausshome_kerberso)

            else:
                #1. copy "krb5.conf"
                if (os.path.isfile(g_opts.krbConfigPath)):
                    shutil.copy(g_opts.krbConfigPath, destfile)
                else:
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50228"] %
                                    g_opts.krbConfigPath)
                #2. change cache file path of kerberos
                if(not os.path.isdir("%s/auth_config" %
                                     os.path.dirname(g_opts.mpprcFile))):
                    raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] %
                                    ("%s/auth_config" %
                                     os.path.dirname(g_opts.mpprcFile)))
                cmd = "sed -i '/default_realm.*/i default_ccache_name = " \
                      "FILE:%s/auth_config/krb5cc_%s' '%s'" % \
                      (os.path.dirname(g_opts.mpprcFile),
                       pwd.getpwnam(g_opts.user).pw_uid, destfile)
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    g_logger.debug("The cmd is %s " % cmd)
                    raise Exception("Config 'krb5.conf' failed.cmd: %s" % cmd)
                g_logger.log("Client Config \"krb5.conf\" successfully.")

        else:
            if os.path.isfile(destfile):
                os.remove(destfile)

            g_logger.log("Clear \"krb5.conf\" successfully.")

    def __initUser(self):

        # create kerberos database
        kerberos_database_file = \
            os.path.realpath(os.path.join(g_opts.gausshome,
                                          "var", "krb5kdc", "principal"))
        if os.path.isfile(kerberos_database_file):
            g_logger.debug("kerberos database has existed.")
        else:
            dir_permission = 0o700
            os.makedirs(os.path.dirname(kerberos_database_file),
                        mode=dir_permission)
            with open("/dev/random", 'rb') as fp:
                srp = fp.read(16)
            passwd = int(srp.hex(), 16)
            cmd = "source %s && kdb5_util create -r HUAWEI.COM -s -P %s" % \
                  (g_opts.mpprcFile, str(passwd))
            (status, output) = subprocess.getstatusoutput(cmd)
            if status != 0:
                g_logger.debug("The cmd is %s " % cmd)
                raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] %
                                "kdb5_util")
            g_logger.debug("Create kerberos database successfully.")

        # create kerberos database user
        cmd = "source %s && kadmin.local -q \"addprinc  " \
              "-randkey %s/huawei.huawei.com\"" % \
              (g_opts.mpprcFile, g_opts.user)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + output)
        g_logger.debug("Create kerberos database user successfully.")

        # create kerberos keytab
        cmd = "source %s && kadmin.local -q \"ktadd -k %s/%s.keytab " \
              "%s/huawei.huawei.com@HUAWEI.COM\"" % \
                (g_opts.mpprcFile, g_opts.gausshome_kerberso,
                 g_opts.user, g_opts.user)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + output)
        g_logger.debug("Create kerberos keytab successfully.")

        g_logger.log("Initialize kerberos user successfully.")

    def __startServer(self):
        # start kdc
        cmd = "source %s && krb5kdc" % g_opts.mpprcFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + output)
        g_logger.debug("Start kerberos kdc successfully.")

        g_logger.log("Start kerberos server successfully.")

    def __distributeKeyAndSite(self):
        hostlist = []
        for hostName in g_sshTool.hostNames:
            if hostName != g_opts.dbNodeInfo.name:
                hostlist.append(hostName)
        g_logger.debug("Distribute nodes: %s" % ",".join(hostlist))
        # distribute keytab
        dest_kerberos_dir = os.path.dirname(g_opts.gausshome_kerberso) + '/'
        g_sshTool.scpFiles(g_opts.gausshome_kerberso,
                           dest_kerberos_dir, hostlist)

        # create auth_config
        mppdb_site_dir = os.path.join(os.path.dirname(g_opts.mpprcFile),
                                      "auth_config")
        cmd = "if [ ! -d '%s' ]; then mkdir %s; fi" % (mppdb_site_dir,
                                                       mppdb_site_dir)
        g_sshTool.executeCommand(cmd, "create auth_config directory",
                                 DefaultValue.SUCCESS, g_sshTool.hostNames,
                                 g_opts.mpprcFile)
        # copy mppdb-site.xml
        src_path = os.path.realpath(os.path.join(g_opts.gausshome, "etc",
                                                 "kerberos", "mppdb-site.xml"))
        if not os.path.isfile(src_path):
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50201"] % src_path)
        dest_path = os.path.realpath(os.path.join(mppdb_site_dir,
                                                  "mppdb-site.xml"))
        try:
            shutil.copy(src_path, dest_path)
        except Exception as e:
            raise Exception(ErrorCode.GAUSS_502["GAUSS_50214"] % src_path)
        # init mppdb-site.xml
        self.__initMppdbSite(mppdb_site_dir)
        # distribute mppdb-site.xml
        g_sshTool.scpFiles(dest_path, dest_path, hostlist)

    def __restartOMmonitor(self):
        """
        function: restart OM_monitor for new environment variable
        input:  NA
        output: NA
        """
        #1. find om_monitor process
        DefaultValue.KillAllProcess(g_opts.user, "om_monitor")
        g_logger.log("Kill om_monitor successfully.")
        cmd = "source /etc/profile;source '%s';%s/bin/om_monitor " \
              "-L %s/%s/cm/om_monitor >> /dev/null 2>&1 &" % \
              (g_opts.mpprcFile, g_clusterInfo.appPath,
               g_clusterInfo.logPath, g_opts.user)
        (status, output) = subprocess.getstatusoutput(cmd)
        if status != 0:
            g_logger.debug("The cmd is %s " % cmd)
            g_logger.debug("Start om_monitor process failed.")
            g_logger.debug("Error:%s\n" % output)

        g_logger.log("Restart om_monitor succeed.")

    def __cleanAuthConfig(self):
        auth_config = os.path.join(os.path.dirname(g_opts.mpprcFile),
                                   "auth_config")
        if os.path.isdir(auth_config):
            try:
                shutil.rmtree(auth_config)
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] %
                                auth_config)
        logPath = DefaultValue.getUserLogDirWithUser(g_opts.user)
        kerberosLog = "%s/kerberos" % logPath
        if os.path.exists(kerberosLog):
            g_file.removeDirectory(kerberosLog)
        g_logger.log("Clean auth config directory succeed.")

    def __cleanServer(self):
        if os.path.isdir(g_opts.gausshome_kerberso):
            try:
                shutil.rmtree(g_opts.gausshome_kerberso)
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] %
                                g_opts.gausshome_kerberso)

        krb_data = "%s/var/krb5kdc" % g_opts.gausshome
        if os.path.isdir(krb_data):
            try:
                shutil.rmtree(krb_data)
            except Exception as e:
                raise Exception(ErrorCode.GAUSS_502["GAUSS_50207"] % krb_data)

        # Remove cron
        crontabFile = "%s/gauss_crontab_file_%d" % \
                      (DefaultValue.getTmpDirFromEnv(g_opts.user), os.getpid())
        cmd = "crontab -l > %s; " % crontabFile
        cmd += "sed -i '/^.*krb5kdc.*$/d' '%s'; " % crontabFile
        cmd += "crontab '%s';" % crontabFile
        cmd += "rm -f '%s'" % crontabFile
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            #no need raise error here, user can do it manually.
            g_logger.debug("The cmd is %s " % cmd)
            g_logger.debug("Failed to delete regular tasks. Error: \n%s  "
                           "You can do it manually." % str(output))

        cmd = "source /etc/profile; source '%s' && \
               proc_pid_list=`ps ux | grep -E 'krb5kdc'|  \
               grep -v 'grep'|awk '{print \$2}'` && \
               (if [ X\"$proc_pid_list\" != X\"\" ];  \
               then echo \"$proc_pid_list\" | xargs -r -n 100 kill -9 ; fi)" \
                % (g_opts.mpprcFile)
        (status, output) = subprocess.getstatusoutput(cmd)
        if (status != 0):
            raise Exception(ErrorCode.GAUSS_514["GAUSS_51400"] % cmd + output)

        g_logger.log("Clean Server files and process succeed.")

    def __install(self):
        try:
            self.__configKrb5(False, g_opts.server)
            self.__setUserEnvVariable(False, g_opts.server)
            self.__triggerJob(False, g_opts.server)
            if not g_opts.server:
                self.__configPostgresql(False)
                self.__configPgHba(False)
                self.__configDNPgHba(False)
                self.__restartOMmonitor()
            g_logger.log("Successfully start Kerberos Authentication.")
        except Exception as e:
            self.__rollback(g_opts.server)
            raise e
        finally:
            pass


    def __uninstall(self, isRollBack=False):
        self.__configKrb5(True)
        self.__setUserEnvVariable(True, False, isRollBack)
        self.__triggerJob(True)
        self.__configPostgresql(True)
        self.__configPgHba(True)
        self.__configDNPgHba(True)
        if not isRollBack:
            self.__cleanAuthConfig()
            self.__cleanServer()
        g_logger.log("Successfully close Kerberos Authentication.")

    def run(self):
        '''
        function: call start or stop
        '''
        if(g_opts.action == "install"):
            self.__install()
        elif(g_opts.action  == "uninstall"):
            self.__uninstall(False)
        else:
            raise Exception(ErrorCode.GAUSS_500["GAUSS_50000"] % g_opts.action)


def parseCommandLine():
    """
    function: Check parameter from command line
    input : NA
    output: NA
    """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "m:U:",
                                   ["help", "krb-server", "krb-client"])
    except Exception as e:
        usage()
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] % str(e))

    if(len(args) > 0):
        GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50000"] %
                               str(args[0]))

    global g_opts
    g_opts = CmdOptions()

    for (key, value) in opts:
        if (key == "--help"):
            usage()
            sys.exit(0)
        elif(key == "-m"):
            g_opts.action = value
        elif (key == "-U"):
            g_opts.user = value
        elif (key == "--krb-server"):
            g_opts.server = True
        elif (key == "--krb-client"):
            g_opts.client = True

    if g_opts.action == 'install':
        if not g_opts.server and not g_opts.client:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50001"] %
                                   "-krb-server' or '--krb-client")
        if g_opts.server and g_opts.client:
            GaussLog.exitWithError(ErrorCode.GAUSS_500["GAUSS_50005"] %
                                   ("-krb-server", "-krb-client"))


def usage():
    '''
 python3 KerberosUtility.py is a utility to config a {0} cluster.
Usage:
    KerberosUtility.py -m install -U USER --krb-server
    KerberosUtility.py -m install -U USER --krb-client
    KerberosUtility.py -m uninstall -U USER
General options:
    -m          "install" will set kerberos config for {0} cluster,
                "uninstall" will cancel to set kerberos config for {0} cluster.
    -U          Cluster User for {0} cluster
Install options:
    --krb-server          Execute install for server.
                          This parameter only work for install
    --krb-client          Execute install for client.
                          This parameter only work for install
Notes:
    --krb-server and --krb-client can only chose one
    '''
    print(usage.__doc__)

if __name__ == '__main__':
    """
    main function
    """
    try:
        parseCommandLine()
        initGlobals()
    except Exception as e:
        GaussLog.exitWithError(str(e))

    try:
        kbs = Kerberos()
        kbs.run()
        sys.exit(0)
    except Exception as e:
        g_logger.logExit(str(e))
