'''
Created on 2014-3-1

@author: 
'''
import commands
import getopt
import sys
import os
import time
import socket

class node():
    def __init__(self, name = "", port = -1, sctp_port = -1):
        self.name = name
        if (port == -1):
            exitWithError("invalid port.")
        self.port = port    
        self.sctp_port = sctp_port         

class redis_tester():
    def __init__(self, src_path, bin_path, new_group_name, new_node_num):
        self.src_path = self.getAbsPath(src_path)
        self.bin_path = self.getAbsPath(bin_path)
        self.new_group_name = new_group_name
        self.new_node_num = new_node_num
        
        self.data_path = self.getAbsPath(self.src_path, "tmp_check")
        self.schema_file = self.getAbsPath(self.data_path, "schema_datanode.sql")
        self.envir_file = self.getAbsPath(self.data_path, "redis_envir_opt")
        self.new_node_list_file = self.getAbsPath(self.data_path, "new_node_list")
        self.logFile = self.getAbsPath(self.data_path, "redis_tester.log")
        self.initLogger()
        if (not os.path.exists(self.envir_file)):
            self.writeEnvirFile()
        
        self.old_datanodes = []
        self.old_coordinators = []
        self.new_node = []
        
    def initLogger(self):
        self.logger = open(self.logFile, "a")
        
    def init_log_prefix(self):
        try:
            import inspect
            cur_stack = inspect.stack()[2]
            return "[%s:%d]" % (cur_stack[3], cur_stack[2])
        except:
            return "[:]"
    
    def log(self, msg):
        msg = "%s %s" % (self.init_log_prefix(), msg)
        if (self.logger is not None):
            print >>self.logger, msg
            self.logger.flush()
        print msg
        
    def debug(self, msg):
        msg = "%s %s" % (self.init_log_prefix(), msg)
        if (self.logger is not None):
            print >>self.logger, msg
            self.logger.flush()
        
    def writeEnvirFile(self):
        envir_opt = "export PATH=%s:$PATH\n" % self.bin_path
        envir_opt += "export LD_LIBRARY_PATH=%s:$LD_LIBRARY_PATH\n" % self.getAbsPath(self.bin_path, "../lib")
        
        fp = open(self.envir_file, "w")
        fp.write(envir_opt)
        fp.flush()
        fp.close()
        
    def addPGPort(self, port):
        envir_opt = "export PGPORT=%d\n" % (port)
        fp = open(self.envir_file, "a")
        fp.write(envir_opt)
        fp.flush()
        fp.close()
        
    def addNewNodeList(self, nodename):
        fp = open(self.new_node_list_file, "a")
        fp.write("%s\n" % nodename)
        fp.flush()
        fp.close()
        
    def getAbsPath(self, dir, sub_dir = None):
        if (sub_dir is not None):
            return os.path.abspath(os.path.join(dir, sub_dir))
        else:
            return os.path.abspath(dir)
        
    def readOldNodePort(self, name):
        datadir = self.getAbsPath(self.data_path, name)
        pid_file = self.getAbsPath(datadir, "postmaster.pid")
        fp = open(pid_file, "r")
        lines = fp.readlines()
        port = int(lines[3])
        fp.close()
        return port
    
    def getNewNodes(self):
        if (self.new_node_num <= 0):
            exitWithError("error new_datanode_num: %d" % self.new_node_num)
        
        old_datanode_num = len(self.old_datanodes)
        last_port = -1
        for dbNode in self.old_coordinators + self.old_datanodes:
            if (dbNode.port > last_port):
                last_port = dbNode.port
        first_dn_port = self.old_datanodes[0].port
        for dbNode in self.old_datanodes:
            if (dbNode.port < first_dn_port):
                first_dn_port = dbNode.port

                
        if (last_port == -1):
            exitWithError("find last port failed.")
            
        new_start_port = last_port + 10 
        new_start_stcp_port = first_dn_port + old_datanode_num*256
        for i in range(old_datanode_num, old_datanode_num + self.new_node_num):
            new_datanode_name = "datanode%d" % (i + 1)
            new_datanode_port = new_start_port
            self.new_node.append(node(new_datanode_name, new_datanode_port, new_start_stcp_port))
            new_start_port += 4
            new_start_stcp_port += 256
            self.addNewNodeList(new_datanode_name)
        
    def getOldNodes(self):
        cmd = "ls -l %s | grep ^d | grep datanode | awk '{print $NF}' | sort" % self.data_path
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            exitWithError("get old datanodes failed. Error: %s" % (output))
        old_datanodes = output.split("\n")
        self.debug(old_datanodes)
        for nodename in old_datanodes:
            port = self.readOldNodePort(nodename)
            self.old_datanodes.append(node(nodename, port))
            
        cmd = "ls -l %s | grep ^d | grep coordinator | awk '{print $NF}' | sort" % self.data_path
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            exitWithError("get old coordinator failed. Error: %s" % (output))
        old_coordinators = output.split("\n")
        self.debug(old_coordinators)
        for nodename in old_coordinators: 
            port = self.readOldNodePort(nodename)
            self.old_coordinators.append(node(nodename, port))
            
        self.addPGPort(self.old_coordinators[0].port)
        
    def setConfig(self, node, config, file):
        cmd = 'echo "%s" >> %s' % (config, file)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            exitWithError("echo node opts[%s:%s] failed. Error: %s" % (node, config, output))
    
    def initdb(self):
        for dbNode in self.new_node:
            datadir = self.getAbsPath(self.data_path, dbNode.name)
            cmd = "source %s && gs_initdb -D %s --nodename %s -w gauss@123" % (self.envir_file, datadir, dbNode.name)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("initdb[%s] failed. Error: %s" % (dbNode.name, output))

            old_config = self.getAbsPath(self.getAbsPath(self.data_path, self.old_datanodes[0].name), "pg_hba.conf")
            new_config = self.getAbsPath(datadir, "pg_hba.conf")
            cmd = "cp %s %s" % (old_config, new_config)    
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("cp config[%s] failed. Error: %s" % (dbNode.name, output))

            old_config = self.getAbsPath(self.getAbsPath(self.data_path, self.old_datanodes[0].name), "postgresql.conf")
            new_config = self.getAbsPath(datadir, "postgresql.conf")
            cmd = "cp %s %s" % (old_config, new_config)    
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("cp config[%s] failed. Error: %s" % (dbNode.name, output))
			
            cmd = "sed -i \"/^pooler_port =/c\\pooler_port = %d\"   %s" % ((dbNode.port + 1), new_config)    
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("cp config[%s] failed. Error: %s" % (dbNode.name, output))
                
            self.setConfig(dbNode.name, "", new_config)
            self.setConfig(dbNode.name, "port = %d" % dbNode.port, new_config)
            self.setConfig(dbNode.name, "pgxc_node_name = \"%s\"" % dbNode.name, new_config)
            #self.setConfig(dbNode.name, "pooler_port = %d" % (dbNode.port + 1), new_config)
            
    def dumpall(self):
        cmd = "source %s && gs_dumpall -p %d -s -f %s --include-alter-table" % (self.envir_file, self.old_coordinators[0].port, self.schema_file)
        (status, output) = commands.getstatusoutput(cmd)
        if (status != 0):
            exitWithError("dumpall failed. Error: %s" % output)
            
    def restore_nodes(self):
        for dbNode in self.new_node:
            datadir = self.getAbsPath(self.data_path, dbNode.name)
            cmd = "source %s && gs_ctl start -Z restoremode -D %s -w -t 3600"  % (self.envir_file, datadir)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("start restore mode[%s] failed. Error: %s" % (dbNode.name, output))
                
            cmd = "source %s && gsql -d postgres -f %s -p %d > %s" % (self.envir_file, self.schema_file, dbNode.port, self.getAbsPath(datadir, "restore.log"))
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("restore node[%s] failed. Error: %s" % (dbNode.name, output))
                
            cmd = "source %s && gs_ctl stop -m i -D %s"  % (self.envir_file, datadir)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("stop restore mode[%s] failed. Error: %s" % (dbNode.name, output))
                
            cmd = "source %s && gs_ctl start -Z datanode -o \"-i\" -D %s -w -t 3600"  % (self.envir_file, datadir)
            (status, output) = commands.getstatusoutput(cmd)
            if (status != 0):
                exitWithError("start new node[%s] failed. Error: %s" % (dbNode.name, output))
            
    def create_new_node(self):
        create_new_node_sql = ""
        for dbNode in self.new_node:
            create_new_node_sql += "create node %s with (type='datanode',port=%d,host='%s',sctp_port=%d, control_port=%d);" % (dbNode.name, dbNode.port, 'localhost', dbNode.sctp_port, dbNode.port+2)
        create_new_node_sql += "select pgxc_pool_reload();"
             
        self.debug("old cn names: %s" % str([cnNode.name for cnNode in self.old_coordinators]))
        self.debug("old cn ports: %s" % str([cnNode.port for cnNode in self.old_coordinators]))
        self.debug("create_new_node_sql: %s" % create_new_node_sql)
        for dbNode in self.old_coordinators:
            cmd = "source %s && gsql -d postgres -p %d -c \"%s\"" % (self.envir_file, dbNode.port, create_new_node_sql)
            (status, output) = commands.getstatusoutput(cmd)
            self.debug(output)
            if (status != 0 or output.find("ERROR") >= 0):
                exitWithError("create new node[%s] failed. Error: %s" % (dbNode.name, output))
                
        allNodes = [dbNode.name for dbNode in self.old_datanodes + self.new_node]
        create_node_group_sql = "create node group %s with(%s);" % (self.new_group_name, ",".join(allNodes))
        reload_sql = "select pgxc_pool_reload();"
        self.debug("create_node_group_sql: %s" % (create_node_group_sql))
        self.debug("reload_sql: %s" % (reload_sql))
        for dbNode in self.old_coordinators:
            create_cmd = "source %s && gsql -d postgres -p %d -c \"%s\"" % (self.envir_file, dbNode.port, create_node_group_sql)
            retry = 5
            while (retry > 0):
                time.sleep(3)
                (status, output) = commands.getstatusoutput(create_cmd)
                self.debug("%s:%s" % (dbNode.name, output))
                if (status != 0 or output.find("ERROR") >= 0):
                    retry = retry - 1
                else:
                    reload_cmd = "source %s && gsql -d postgres -p %d -c \"%s\"" % (self.envir_file, dbNode.port, reload_sql)
                    (status, output) = commands.getstatusoutput(reload_cmd)
                    self.debug("%s:%s" % (dbNode.name, output))
                    if (status != 0 or output.find("ERROR") >= 0):
                        exitWithError("reload node group[%s] failed." % (dbNode.name))
                    break  
            if (retry == 0):
                exitWithError("create node group[%s] failed." % (dbNode.name))
        bucketcnts = [1024,2048,4096,8192]
        for i in bucketcnts:
            create_child_group_sql = "create node group %s_%s bucketcnt %s groupparent %s;" % (self.new_group_name, i, i, self.new_group_name)
            self.debug("create_child_group_sql: %s" % (create_child_group_sql))
            for dbNode in self.old_coordinators:
                cmd = "source %s && gsql -d postgres -p %d -c \"%s\"" % (self.envir_file, dbNode.port, create_child_group_sql)
                (status, output) = commands.getstatusoutput(cmd)
                self.debug(output)
                if (status != 0 or output.find("ERROR") >= 0):
                    exitWithError("create child group[%s] failed. Error: %s" %(i, output))

    def do_clean(self):
        fp = open(self.new_node_list_file, "r")
        lines = fp.read()
        allNodes = lines.split("\n")
        for dbNode in allNodes:
            dbNode = dbNode.strip()
            if (dbNode == ""):
                continue
            datadir = self.getAbsPath(self.data_path, dbNode)
            cmd = "source %s && gs_ctl stop -Z datanode -D %s -m i" % (self.envir_file, datadir)
            (status, output) = commands.getstatusoutput(cmd)
            self.debug(output)
            if (status != 0):
                exitWithError("do clean[%s] failed. Error: %s" % (dbNode, output))
    def update_installation_and_is_redistribution(self):
         sql_tmp = "start transaction;update pgxc_group set in_redistribution = 'y' where is_installation = true;commit;"
         for dbNode in self.old_coordinators:
             cmd = "source %s && gsql -d postgres -p %d -c \"%s\"" % (self.envir_file, dbNode.port, sql_tmp)
             (status, output) = commands.getstatusoutput(cmd)
             self.debug(output)
             if (status != 0 or output.find("ERROR") >= 0):
                 exitWithError("update_installation_and_is_redistribution failed. Error: %s" % (output))

    
def exitWithError(msg):
    print msg
    sys.exit(1)
    
def usage():
    pass    

def main():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], "t:n:b:s:g:h", ["help"])
    except Exception, e:
        usage()
        exitWithError(str(e))
    
    if(len(args) > 0):
        exitWithError("Parameter input error: %s" % str(args[0]))
    
    target = ""
    new_node_num = -1
    bin_path = None
    src_path = None
    new_group_name = ""
    for (key, value) in opts:
        if (key == "-h" or key == "--help"):
            usage()
            sys.exit(0)
        elif (key == "-t"):
            target = value.strip()
        elif (key == "-n"):
            new_node_num = int(value.strip())
        elif (key == "-b"):
            bin_path = value.strip()
        elif (key == "-s"):
            src_path = value.strip()
        elif (key == "-g"):
            new_group_name = value.strip()
        else:
            exitWithError("Parameter input error, unknown options %s." % key)
            
    if (target == "addnode"):
        tester = redis_tester(src_path, bin_path, new_group_name, new_node_num)
        tester.getOldNodes()    
        tester.getNewNodes()
        tester.initdb()
        tester.update_installation_and_is_redistribution();
        tester.dumpall()
        tester.restore_nodes()
        tester.create_new_node()
    elif (target == "clean"):
        tester = redis_tester(src_path, bin_path, "dummy_group_name", 1)
        tester.do_clean()
    else:
        exitWithError("error target: %s" % target)
        
    print "test redis success."

main()
