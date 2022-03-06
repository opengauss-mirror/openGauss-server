#!/usr/bin/env python
#
# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# ---------------------------------------------------------------------------------------
#
# IDENTIFICATION
#        src/test/grayscale_upgrade/upgradeCheck.py
#
# ---------------------------------------------------------------------------------------

import getopt, sys, os
import shutil
import time
import string
import commands
import getpass
import datetime
from multiprocessing.dummy import Pool as ThreadPool

g_base_port = 25632
g_gtm_port = 25686
g_pooler_base_port = 25832
g_file_node_info = "cndn.cnf"
bin_path = ""
g_valgrind = ""
fastcheck_conf_file = "src/distribute/test/regress/make_fastcheck_postgresql.conf"
startTime = ""
endTime = ""
(INIT_STATUS_AND_EXEC_PRE_SCRIPT,
 EXEC_PRE_ROLLBACK_SCRIPT,
 RESERVED_VALUE,
 EXEC_POST_ROLLBACK_SCRIPT,
 EXEC_POST_SCRIPT
 ) = list(range(1, 6))

class Pterodb():
    def __init__(self, coordinator_num = 3, data_node_num = 12, data_dir = "src/distribute/test/regress/tmp_check/", base_port = 25632, part_number = 0, upgrade_from = 92, gsql_dir = "gsql"):
        self.coordinator_num = coordinator_num
        self.data_node_num = data_node_num
        self.base_port = base_port
        self.gtm_port = base_port + 2*coordinator_num + 4*data_node_num
        self.dname_prefix = "datanode"
        self.data_dir = data_dir
        self.new_version_number = 0
        self.base_versioni_number = 0
        self.part_number = part_number
        script_path = os.path.abspath(os.path.dirname(__file__))
        self.script_path = script_path
        self.upgrade_from = float(upgrade_from)/1000.0
        self.gsql_dir = gsql_dir
        self.run_type = 0
        global upgrade_catalog_maindb_path_check, upgrade_catalog_maindb_sql_check, \
            upgrade_catalog_otherdb_path_check, upgrade_catalog_otherdb_sql_check, \
            rollback_catalog_maindb_path_check, rollback_catalog_maindb_sql_check, \
            rollback_catalog_otherdb_path_check, rollback_catalog_otherdb_sql_check, \
            upgrade_post_catalog_maindb_sql_check, upgrade_post_catalog_otherdb_sql_check, \
            rollback_post_catalog_maindb_sql_check, rollback_post_catalog_otherdb_sql_check, \
            check_upgrade_path, check_upgrade_sql, \
            exec_sql_log, upgradeCheckLog, \
            maindb, otherdbs, private_dict, startTime, endTime, \
            INIT_STATUS_AND_EXEC_PRE_SCRIPT, EXEC_PRE_ROLLBACK_SCRIPT, RESERVED_VALUE, EXEC_POST_ROLLBACK_SCRIPT, EXEC_POST_SCRIPT

        # Upgrade Maindb Script Path
        private_upgrade_catalog_maindb_path_check = script_path + "/../../../../privategauss/include/catalog/upgrade_sql/upgrade_catalog_maindb/"
        upgrade_catalog_maindb_path_check = script_path + "/../../include/catalog/upgrade_sql/upgrade_catalog_maindb/"

        # Upgrade Maindb Exec Script Type
        upgrade_catalog_maindb_sql_check = script_path + "/sql/upgrade_catalog_maindb_tmp.sql"
        upgrade_post_catalog_maindb_sql_check = script_path + "/sql/upgrade-post_catalog_maindb_tmp.sql"

        # Upgrade Otherdb Script Path
        private_upgrade_catalog_otherdb_path_check = script_path + "/../../../../privategauss/include/catalog/upgrade_sql/upgrade_catalog_otherdb/"
        upgrade_catalog_otherdb_path_check = script_path + "/../../include/catalog/upgrade_sql/upgrade_catalog_otherdb/"

        # Upgrade Otherdb Exec Script Type
        upgrade_catalog_otherdb_sql_check = script_path + "/sql/upgrade_catalog_otherdb_tmp.sql"
        upgrade_post_catalog_otherdb_sql_check = script_path + "/sql/upgrade-post_catalog_otherdb_tmp.sql"

        # Rollback Maindb Script Path
        private_rollback_catalog_maindb_path_check = script_path + "/../../../../privategauss/include/catalog/upgrade_sql/rollback_catalog_maindb/"
        rollback_catalog_maindb_path_check = script_path + "/../../include/catalog/upgrade_sql/rollback_catalog_maindb/"

        # Rollback Maindb Exec Script Type
        rollback_catalog_maindb_sql_check = script_path + "/sql/rollback_catalog_maindb_tmp.sql"
        rollback_post_catalog_maindb_sql_check = script_path + "/sql/rollback-post_catalog_maindb_tmp.sql"

        # Rollback Otherdb Script Path
        private_rollback_catalog_otherdb_path_check = script_path + "/../../../../privategauss/include/catalog/upgrade_sql/rollback_catalog_otherdb/"
        rollback_catalog_otherdb_path_check = script_path + "/../../include/catalog/upgrade_sql/rollback_catalog_otherdb/"

        # Rollback Otherdb Exec Script Type
        rollback_catalog_otherdb_sql_check = script_path + "/sql/rollback_catalog_otherdb_tmp.sql"
        rollback_post_catalog_otherdb_sql_check = script_path + "/sql/rollback-post_catalog_otherdb_tmp.sql"

        # Check Script Path
        check_upgrade_path = script_path + "/../../include/catalog/upgrade_sql/check_upgrade/"

        # Check Exec Script Type
        check_upgrade_sql = script_path + "/sql/check_upgrade_tmp.sql"
        exec_sql_log = script_path +  "/sql/execSql.log"
        upgradeCheckLog = script_path +  "/upgradeCheck.log"
        maindb = "postgres"
        otherdbs = ['template0', 'template1', 'test_td']
        private_dict = {upgrade_catalog_maindb_path_check: private_upgrade_catalog_maindb_path_check,
                        upgrade_catalog_otherdb_path_check: private_upgrade_catalog_otherdb_path_check,
                        rollback_catalog_maindb_path_check: private_rollback_catalog_maindb_path_check,
                        rollback_catalog_otherdb_path_check: private_rollback_catalog_otherdb_path_check}
        startTime, endTime = datetime.datetime.now(), datetime.datetime.now()

    def writeLogFile(self, msg):
        """
        write log file durng upgrade check
        """
        try:
            fp = None
            fp = open(upgradeCheckLog, 'a')
            recordMsg = "[{0}]:{1}".format(datetime.datetime.now(), msg)
            fp.write(recordMsg + "\n")
            if(fp):
                fp.close()
        except Exception, e:
            if(fp):
                fp.close()
            raise Exception(str(e))

    def cleanup(self):
        """
        clean up temporary check files that may exist last time
        """
        cmd = "rm -rf %s/sql && rm -f %s && mkdir %s/sql" % (self.script_path, upgradeCheckLog, self.script_path)
        self.writeLogFile("Cmd for cleanup is: %s" % cmd)
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("Failed to clean up temproray check files from last time.\nCmd:%s\nOutput:%s" % (cmd, output))

    def prepareUpCheckSqlFile(self):
        """
        prepare 5 files: upgrade_catalog_maindb_tmp.sql,upgrade_catalog_otherdb_tmp.sql,check_upgrade_tmp.sql
                              rollback_catalog_maindb_tmp.sql,rollback_catalog_otherdb_tmp.sql
        """
        try:
            self.preparedSqlFile("upgrade", "maindb", upgrade_catalog_maindb_path_check, upgrade_catalog_maindb_sql_check)
            self.preparedSqlFile("upgrade-post", "maindb", upgrade_catalog_maindb_path_check, upgrade_post_catalog_maindb_sql_check)
            self.preparedSqlFile("upgrade", "otherdb", upgrade_catalog_otherdb_path_check, upgrade_catalog_otherdb_sql_check)
            self.preparedSqlFile("upgrade-post", "otherdb", upgrade_catalog_otherdb_path_check, upgrade_post_catalog_otherdb_sql_check)
            self.preparedSqlFile("rollback", "maindb", rollback_catalog_maindb_path_check, rollback_catalog_maindb_sql_check)
            self.preparedSqlFile("rollback-post", "maindb", rollback_catalog_maindb_path_check, rollback_post_catalog_maindb_sql_check)
            self.preparedSqlFile("rollback", "otherdb", rollback_catalog_otherdb_path_check, rollback_catalog_otherdb_sql_check)
            self.preparedSqlFile("rollback-post", "otherdb", rollback_catalog_otherdb_path_check, rollback_post_catalog_otherdb_sql_check)
            self.preparedSqlFile("check", "", check_upgrade_path, check_upgrade_sql)

            self.writeLogFile("Successfully prepared check upgrade sql files.")
        except Exception, e:
            raise Exception("Prepare upgrade check sql file failed.ERROR: %s" % str(e))

    def preparedSqlFile(self, scriptType, dbType, scriptDir, sqlAll):
        #filter all upgrade sql files and merge into the result file
        try:
            if "check" in scriptType:
                self.writeLogFile("Preparing check_upgrade_tmp.sql")
            else:
                self.writeLogFile("Preparing %s_catalog_%s_tmp.sql" % (scriptType, dbType))
            #filter all upgrade sql files
            validFileList = self.spliceSqlFile(scriptDir, scriptType)
            #sort the scripts
            if "rollback" in scriptType:
                validFileList.sort(reverse=True)
            else:
                validFileList.sort()
            #merge into one result file
            if "check" in scriptType:
                self.writeLogFile("check_upgrade_valid_fileList: %s" % validFileList)
            else:
                self.writeLogFile("%s_catalog_%s_valid_fileList: %s" % (scriptType, dbType, validFileList))
            self.writeSqlFile(sqlAll, validFileList, scriptDir)
        except Exception, e:
            raise Exception("Prepare sql file failed.ERROR: %s" % str(e))

    def getNewVersionNum(self):
        """
        Obtain the version number from the globals.cpp file.
        :return:
        """
        flagStr = "const uint32 GRAND_VERSION_NUM"
        globalsPath = os.path.abspath(os.path.dirname(__file__) + "/../../common/backend/utils/init/globals.cpp")
        if not os.path.isfile(globalsPath):
            errMsg = "The file {0} cannot be found".format(globalsPath)
            raise Exception(errMsg)
        allLines = []
        with open(globalsPath, 'r') as fp:
            allLines = fp.readlines()
        for line in allLines:
            if flagStr in line:
                result = line.split("=")[1].split(";")[0].strip(" ")
                return float(result)/1000.0
        errMsg = "The '{0}' cannot be found in the '{1}' file. " \
                 "Change the version number in '{0} = 92xxx;' format.".format(flagStr,globalsPath )
        raise Exception(errMsg)


    def spliceSqlFile(self, fileDir, scriptType="_"):
        try:
            NewVersionNum = self.getNewVersionNum()
            BaseVersionNum = self.upgrade_from
            fileAllList = os.listdir(fileDir)
            privateFileAllList = []
            keyElement = []
            if fileDir != check_upgrade_path:
                privateFileAllList = os.listdir(private_dict[fileDir])
                commonScriptList = list(set(fileAllList) & set(privateFileAllList))
                commonScriptList = [script for script in  commonScriptList if "407" not in script]
                commonScriptList = [script for script in  commonScriptList if "445" not in script]
                commonScriptList = [script for script in  commonScriptList if "467" not in script]
                keyElement = fileDir.split('/')[-2].split('_')
                if commonScriptList:
                    errMsg = "OpenGauss and privategauss contain scripts of the same version number. " \
                             "Please change the version number, " \
                             "the scripts {0} in openGauss and privateGauss cannot use the same version number.".format(commonScriptList)
                    self.writeLogFile(errMsg)
                    raise Exception("The script name does not meet the specifications. Error: {0}".format(errMsg))

            allList = fileAllList + privateFileAllList

            for name in allList:
                for key in keyElement:
                    if key not in name:
                        errMsg = "The script {0} name does not meet the specifications, it needs to contain {1}".format(name, key)
                        self.writeLogFile(errMsg)
                        raise Exception(errMsg)

            result = []
            if len(allList) != 0:
                for each_sql_file in allList:
                    if not os.path.isfile("%s/%s" % (fileDir, each_sql_file)) and \
                            not os.path.isfile("%s/%s" % (private_dict[fileDir], each_sql_file)):
                        errMsg = "can not file the file {0}".format(each_sql_file)
                        raise Exception(errMsg)
                    prefix = each_sql_file.split('.')[0]
                    resList = prefix.split('_')
                    if(len(resList) != 5) or scriptType not in resList:
                        continue
                    file_num = "%s.%s" % (resList[3], resList[4])
                    if float(file_num) <= 92:
                        continue
                    if BaseVersionNum < float(file_num) <= NewVersionNum:
                        result.append(each_sql_file)
        except Exception, e:
            raise Exception("Splice sql file failed.ERROR: %s" % str(e))
        return result

    def writeSqlFile(self, fileName, fileList, fileDir):
        file = open(fileName, 'w')
        file.write("START TRANSACTION;")
        file.write(os.linesep)
        file.write("SET IsInplaceUpgrade = on;")
        file.write(os.linesep)
        self.writeLogFile("fileDir is {0}, The list of files being written is {1}".format(fileDir, fileList))
        for each_file in fileList:
            if os.path.isfile("%s/%s" % (fileDir, each_file)):
                each_file_with_path = "%s/%s" % (fileDir, each_file)
            elif os.path.isfile("%s/%s" % (private_dict[fileDir], each_file)):
                each_file_with_path = "%s/%s" % (private_dict[fileDir], each_file)
            else:
                errMsg = "can not file the file {0}".format(each_file)
                raise Exception(errMsg)
            self.writeLogFile("handling file: %s" % each_file_with_path)
            for txt in open(each_file_with_path,'r'):
                file.write(txt)
            file.write(os.linesep)
        file.write("COMMIT;")
        file.write(os.linesep)
        file.close()
        self.writeLogFile("Complate file {0} with the list:{1}".format(fileName, fileList))

    def checkSqlResult(self, Type = "upgrade"):
        cmd = "grep ERROR " + exec_sql_log
        (status, output) = commands.getstatusoutput(cmd)
        if(output.find("ERROR") != -1):
            raise Exception("Failed to execute catalog %s" % Type)
        cmd = "grep PANIC " + exec_sql_log
        (status, output) = commands.getstatusoutput(cmd)
        if(output.find("PANIC") != -1):
            raise Exception("Failed to execute catalog %s" % Type)
        cmd = "grep FATAL " + exec_sql_log
        (status, output) = commands.getstatusoutput(cmd)
        if(output.find("FATAL") != -1):
            raise Exception("Failed to execute catalog %s" % Type)

    def upgrade_one_database(self, db_name):
        """
        """
        try:
            if db_name == "postgres":
                if self.run_type == EXEC_POST_SCRIPT:
                    upgrade_catalog_file = upgrade_post_catalog_maindb_sql_check
                else:
                    upgrade_catalog_file = upgrade_catalog_maindb_sql_check
            else:
                if self.run_type == EXEC_POST_SCRIPT:
                    upgrade_catalog_file = upgrade_post_catalog_otherdb_sql_check
                else:
                    upgrade_catalog_file = upgrade_catalog_otherdb_sql_check
            cmd = self.gsql_dir + " -X -q -a -d " + db_name + " -p " + str(self.base_port) + " -f " + upgrade_catalog_file + " >> " + exec_sql_log + " 2>&1 "
            (status, output) = commands.getstatusoutput(cmd)
            self.writeLogFile("Cmd is {0}, output is {1}".format(cmd, output))
            if(status != 0):
                raise Exception("Failed to upgrade catalogs!")
        except Exception, e:
            raise Exception(str(e))

    def rollback_one_database(self, db_name):
        """
        """
        try:
            if db_name == "postgres":
                if self.run_type >= EXEC_POST_ROLLBACK_SCRIPT:
                    rollback_catalog_file = rollback_post_catalog_maindb_sql_check
                else:
                    rollback_catalog_file = rollback_catalog_maindb_sql_check
            else:
                if self.run_type >= EXEC_POST_ROLLBACK_SCRIPT:
                    rollback_catalog_file = rollback_post_catalog_otherdb_sql_check
                else:
                    rollback_catalog_file = rollback_catalog_otherdb_sql_check
            cmd = self.gsql_dir + " -X -q -a -d " + db_name + " -p " + str(self.base_port) + " -f " + rollback_catalog_file + " >> " + exec_sql_log + " 2>&1 "
            (status, output) = commands.getstatusoutput(cmd)
            self.writeLogFile("Cmd is {0}, output is {1}".format(cmd, output))
            if(status != 0):
                raise Exception("Failed to rollback catalogs!")
        except Exception, e:
            raise Exception(str(e))

    def execSqlFile(self, scriptType):
        self.rollback_one_database(maindb)
        pool = ThreadPool(1)
        pool.map(self.rollback_one_database, otherdbs)
        pool.close()
        pool.join()
        self.checkSqlResult("rollback%s" % scriptType)

        if self.run_type in [EXEC_PRE_ROLLBACK_SCRIPT, EXEC_POST_ROLLBACK_SCRIPT]:
            return

        self.upgrade_one_database(maindb)
        pool = ThreadPool(1)
        pool.map(self.upgrade_one_database, otherdbs)
        pool.close()
        pool.join()
        self.checkSqlResult("upgrade%s" % scriptType)


    def executeSQL(self):
        try:
            cmd = ""
            if self.run_type < EXEC_POST_ROLLBACK_SCRIPT:
                testSql = self.gsql_dir + " -X -q -a -d postgres -p " + str(self.base_port) + " -c \"select datname from pg_database;\""
                (status, output) = commands.getstatusoutput(testSql)
                if(status != 0):
                    raise Exception("Failed to check test_td database!")
                if "test_td" not in output:
                    cmd =  cmd + self.gsql_dir + " -X -q -a -d postgres -p " + str(self.base_port) + " -c \"create database test_td DBCOMPATIBILITY 'C';\"" + " >> " + exec_sql_log + " 2>&1"
                    (status, output) = commands.getstatusoutput(cmd)
                    if(status != 0):
                        raise Exception("Failed to create database!")

            if self.run_type >= EXEC_POST_ROLLBACK_SCRIPT:
                self.execSqlFile("-post")
            else:
                self.execSqlFile("")

        except Exception, e:
            raise Exception("ERROR: %s\nPlease refer to %s for details" % (str(e), exec_sql_log))

    def executeSimpleTest(self):
        cmd = "gsql -X -q -a -d postgres -p " + str(self.base_port) + " -c \"create table upcheck_tmp_table(a int); drop table upcheck_tmp_table;\""+ " >> " + exec_sql_log + " 2>&1 "
        (status, output) = commands.getstatusoutput(cmd)
        if(status != 0):
            raise Exception("Simple test failed before make full-load fast check!")
        self.checkSqlResult("upgrade simple test")

    def run(self):
        try:
            if self.run_type == INIT_STATUS_AND_EXEC_PRE_SCRIPT:

                self.cleanup()

                self.prepareUpCheckSqlFile()

            self.executeSQL()

        except Exception, e:
            print str(e)
            exit(1)

def usage():
    print "------------------------------------------------------"
    print "python pgxc.py\n    -c coor_num -d datanode_num\n    -s means start\n    -o means stop"
    print "    -g means memcheck"
    print "    -D data directory\n"
    print "------------------------------------------------------"

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hD:c:d:ukovgp:n:f:s:r:p", ["help", "data_dir=", "rollback", "post"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        # usage()
        sys.exit(2)

    coordinator_num = 3
    datanode_num = 12
    base_port = 25632
    part_number = 0
    upgrade_from = 0
    data_dir = "src/distribute/test/regress/tmp_check/"
    gsql_dir = "gsql"
    global g_valgrind;
    global g_file_node_info;

    #1 start; 2 stop; 3 ; 4 again exec; 5 post script
    run_type = 0

    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-D", "data_dir"):
            data_dir = a
        elif o in ("-s", "--gsql-dir"):
            gsql_dir = a
        elif o in ("-c", "--coordinator"):
            coordinator_num = int(a)
        elif o in ("-d", "--datanode"):
            datanode_num = int(a)
        elif o in ("-p", "--port"):
            base_port = int(a)
        elif o in ("-f", "--from"):
            upgrade_from = int(a)
        elif o in ("-n", "--number"):
            part_number = int(a)
        elif o in ("-u", "--startup"):
            # The fastcheck invokes the upgradeCheck script for the first time. and execute the pre script.
            run_type = INIT_STATUS_AND_EXEC_PRE_SCRIPT
        elif o in ("-k", "--startcheck"):
            # Reserved Value
            run_type = RESERVED_VALUE
        elif o in ("-o", "--pre_rollback"):
            # Executing the Pre-Rollback Script
            run_type = EXEC_PRE_ROLLBACK_SCRIPT
        elif o in ("-r", "--rollback"):
            # Executing the Post-Rollback Script
            run_type = EXEC_POST_ROLLBACK_SCRIPT
        elif o in ("--post"):
            # Executing the Post Script
            run_type = EXEC_POST_SCRIPT
        elif o in ("-g", "--memcheck"):
            g_valgrind = "valgrind --tool=memcheck --leak-check=full  --log-file=memcheck.log "
            #g_valgrind = "valgrind --tool=massif --time-unit=B --detailed-freq=1 --massif-out-file=mass.out "
        else:
            assert False, "unhandled option"

    if((coordinator_num == 0 or datanode_num == 0) and run_type == 0):
        usage()
        sys.exit()

    g_file_node_info = data_dir + "/" + g_file_node_info
    ptdb = Pterodb(coordinator_num,datanode_num, data_dir, base_port, part_number, upgrade_from, gsql_dir)
    ptdb.run_type = run_type
    ptdb.run()
    endTime = datetime.datetime.now()
    if (run_type not in [EXEC_PRE_ROLLBACK_SCRIPT, EXEC_POST_ROLLBACK_SCRIPT]):
        print "Make upgrade check successfully. Total time {0}s\n".format((endTime-startTime).seconds)
    else:
        print "Make rollback upgrade check successfully. Total time {0}s\n".format((endTime - startTime).seconds)
    exit(0)


if __name__ == "__main__":
    main()
