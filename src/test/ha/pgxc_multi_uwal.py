#!/usr/bin/python

import getopt, sys, os, getpass
import shutil
import time
import string

g_base_port = int(os.environ.get("g_base_port"))
g_pooler_base_port = int(os.environ.get("g_pooler_base_port"))
primary_port = int(os.environ.get("g_base_port")) + 3
install_path = os.environ.get("install_path")
g_data_path = os.environ.get("g_data_path")
g_local_ip = os.environ.get("g_local_ip")
g_file_node_info = "cndn.cnf"
g_valgrind = ""
g_username = getpass.getuser()
g_passwd = "Gauss@123"
g_trace_compress = False
g_codebase = os.environ.get("CODE_BASE")


class Pterodb():
    def __init__(self, standby_node_num, level, g_data_path):
        self.standby_node_num = standby_node_num
        self.level = level
        self.g_data_path = g_data_path
        self.dname_prefix = "datanode"
        self.__init_node_arr()

    def __init_node_arr(self):
        self.ha_port_arr = [0 for i in range(self.standby_node_num+1)]
        self.uwal_port_arr = [0 for i in range(self.standby_node_num+1)]
        self.uwal_id_arr = [0 for i in range(self.standby_node_num+1)]
        self.node_data_path_arr = [0 for i in range(self.standby_node_num+1)]

    def __kill_all_server(self):
        os.system("ps -ef | grep opengauss/bin/gaussdb | grep -v grep | awk '{print $2}' | xargs kill -9 ")


    def init_env(self):
        self.__kill_all_server()

        if(os.path.exists(self.g_data_path) == False):
            os.mkdir(self.g_data_path)
        else:
            shutil.rmtree(self.g_data_path)
            os.mkdir(self.g_data_path)
            print("rm dir ok")

        #generate port/uwal id/node path
        self.__generate_conf()

        # init primary and standby
        for i in range(0, self.standby_node_num + 1):
            datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.node_data_path_arr[i] + " --nodename=" + self.dname_prefix + str(i)  + " -U " + g_username + " -w " + g_passwd
            print(datanode_cmd_init)
            os.system(datanode_cmd_init)
            conf_file = self.node_data_path_arr[i] + "/postgresql.conf"
            self.__modify_conf_port(conf_file, self.ha_port_arr[i])
            self.__turn_on_pg_log(conf_file)
            self.__modify_conf_standby(conf_file, i)
            self.__modify_conf_application_name(conf_file, "dn_p" + str(i))
            self.__modify_conf_uwal(conf_file, i)
            self.__modify_conf_synchronous_standby_names(conf_file, self.level, i)

            uwal_path = self.node_data_path_arr[i] + "/uwal"
            uwal_path_create = "mkdir -p " + uwal_path
            print(uwal_path_create)
            os.system(uwal_path_create)

            uwal_log_path = self.node_data_path_arr[i] + "/uwal_log"
            uwal_log_path_create = "mkdir -p " + uwal_log_path
            print(uwal_log_path_create)
            os.system(uwal_log_path_create)

            os.system("chmod 0600 " + g_codebase + "/src/test/regress/sslcert/cacert.pem")
            datanode_pem_copy = "cp " + g_codebase + "/src/test/regress/sslcert/cacert.pem " + self.node_data_path_arr[i]
            print(datanode_pem_copy)
            os.system(datanode_pem_copy)

            os.system("chmod 0600 " + g_codebase + "/src/test/regress/sslcert/server.*")
            datanode_ssl_copy = "cp " + g_codebase + "/src/test/regress/sslcert/server.* " + self.node_data_path_arr[i]
            print(datanode_ssl_copy)
            os.system(datanode_ssl_copy)

            datanode_ssl_encrypt = install_path + "/bin/gs_guc encrypt -M server -K " + g_passwd + " -D " + self.node_data_path_arr[i]
            print(datanode_ssl_encrypt)
            os.system(datanode_ssl_encrypt)
            datanode_hba_modify = "sed -i " + " 's#host.*all.*all.*127.0.0.1/32.*#hostssl    all             all             127.0.0.1/32               sha256#g' "  + self.node_data_path_arr[i] +"/pg_hba.conf"
            print(datanode_hba_modify)
            os.system(datanode_hba_modify)
    
    def  __generate_conf(self):
        self.__generate_port()
        self.__generate_uwal_id()
        self.__generate_node_path()

    def __generate_port(self):
        port = primary_port
        for i in range(0, self.standby_node_num + 1):
            self.ha_port_arr[i] = port
            self.uwal_port_arr[i] = port + 1000
            port += 3

    def __generate_uwal_id(self):
        uwal_id = 0
        for i in range(0, self.standby_node_num+1):
            self.uwal_id_arr[i] = uwal_id + i

    def __generate_node_path(self):
        # primary
        self.node_data_path_arr[0] = self.g_data_path + "/" + self.dname_prefix + str(1)
        # standby
        for i in range(1, self.standby_node_num+1):
            self.node_data_path_arr[i] = self.g_data_path + "/" + self.dname_prefix + str(i) + "_standby"

    def __modify_conf_standby(self, conf_file, n):
        j = 1

        file_handler = open(conf_file,"a")
        string = "listen_addresses = '*'"+ "\n"
        file_handler.write(string)

        for i in range(0, self.standby_node_num + 1):
            if(i != n):
                #repl
                string = "replconninfo%d='localhost=%s localport=%d remotehost=%s remoteport=%d remotenodeid=%d remoteuwalhost=%s remoteuwalport=%d'\n" % \
                (j, g_local_ip, self.ha_port_arr[n], g_local_ip, self.ha_port_arr[i], self.uwal_id_arr[i], g_local_ip, self.uwal_port_arr[i])
                print(string)
                file_handler.write(string)
                j = j + 1

        file_handler.close()

    def __modify_conf_application_name(self, conf_file, name):
        file_handler = open(conf_file,"a")
        string = "application_name = '" + name + "'" + "\n"
        file_handler.write(string)
        file_handler.close()

    def __modify_conf_uwal(self, conf_file, index):
        file_handler = open(conf_file,"a")
        string = "password_encryption_type = 0" + "\n"
        file_handler.write(string)
        string = "remote_read_mode = 'non_authentication'" + "\n"
        file_handler.write(string)
        string = "enable_uwal = on" + "\n"
        file_handler.write(string)
        string = "uwal_disk_size = 8589934592" + "\n"
        file_handler.write(string)
        string = "uwal_config = '{\"uwal_nodeid\": %d, \"uwal_ip\": \"127.0.0.1\", \"uwal_port\": %d, \"uwal_protocol\": \"tcp\"}'\n" % \
            (index, self.uwal_port_arr[index])
        file_handler.write(string)
        string = "uwal_devices_path = '" + self.node_data_path_arr[index] + '/uwal' + "'\n"
        file_handler.write(string)
        string = "uwal_log_path = '" + self.node_data_path_arr[index] + '/uwal_log' + "'\n"
        file_handler.write(string)
        string = "uwal_rpc_compression_switch = false" + "\n"
        file_handler.write(string)
        string = "uwal_rpc_flowcontrol_switch = false" + "\n"
        file_handler.write(string)
        string = "uwal_rpc_flowcontrol_value = 128" + "\n"
        file_handler.write(string)
        string = "ssl = on" + "\n"
        file_handler.write(string)
        string = "ssl_ca_file = 'cacert.pem'" + "\n"
        file_handler.write(string)
        string = "ssl_cert_file = 'server.crt'" + "\n"
        file_handler.write(string)
        string = "ssl_key_file = 'server.key'" + "\n"
        file_handler.write(string)
        string = "synchronous_commit = on\n"
        file_handler.write(string)
        file_handler.close()

    def __modify_conf_port(self, conf_file, port):
        file_handler = open(conf_file, "a")
        string = "port = " + str(port) + "\n"
        file_handler.write(string)
        file_handler.close()

    def __modify_conf_synchronous_standby_names(self, conf_file, level, cur_node_id):
        if (self.standby_node_num == 0):
            return
        file_handler = open(conf_file, "a")
        standby_appname_arr = []
        for i in range(0, self.standby_node_num + 1):
            if i == cur_node_id:
                continue
            standby_appname_arr.append("dn_p" + str(i))
        standby_appname = ", ".join(standby_appname_arr)
        string = "synchronous_standby_names = 'ANY " + str(level) + " (" + standby_appname + ")'\n"
        file_handler.write(string)
        file_handler.close()

    def __turn_on_pg_log(self, conf_file):
        file_handler = open(conf_file,"a")
        pglog_conf =  "logging_collector = on   \n"
        pglog_conf = pglog_conf + "log_directory = 'pg_log' \n"
        pglog_conf = pglog_conf + "log_line_prefix = '%m %c %d %p %a %x %e ' \n"
        pglog_conf = pglog_conf + "enable_data_replicate = off \n"
        pglog_conf = pglog_conf + "replication_type = 1 \n"
        file_handler.write(pglog_conf)
        file_handler.close()

    def __rm_pid_file(self):
        cmd = "rm -rf "
        for i in range(0, self.standby_node_num + 1):
            rm_cmd = cmd + self.node_data_path_arr[i] + "/postmaster.pid"
            print(rm_cmd)
            os.system(rm_cmd)

    #save coor_num and datanode num
    def __save_nodes_info(self):
        file_nodes_info = open(g_file_node_info,"w")
        file_nodes_info.write(str(self.standby_node_num))
        file_nodes_info.write("\n")
        file_nodes_info.write(str(self.level))
        file_nodes_info.write("\n")
        file_nodes_info.write(str(self.g_data_path))
        file_nodes_info.write("\n")
        file_nodes_info.close()

    #read coor_num and datanode num
    def __read_nodes_info(self):
        file_nodes_info = open(g_file_node_info, "r")
        lines = file_nodes_info.readlines()
        self.standby_node_num = int(lines[0].strip())
        self.level = int(lines[1].strip())
        self.g_data_path = lines[2].strip()
        file_nodes_info.close()

        self.__init_node_arr()
        self.__generate_conf()

    def __start_primary(self):
        #start primary
        datanode_cmd = install_path + "/bin/gs_ctl start -M primary -D " + self.node_data_path_arr[0] 
            # + " > "  + self.node_data_path_arr[0] + "/logdn" + str(0) + ".log 2>&1 &"
        print(datanode_cmd)
        os.system(datanode_cmd)
        time.sleep(5)

    def __start_standby(self):
        #start standby
        for i in range(1, self.standby_node_num + 1):
            datanode_cmd = install_path + "/bin/gs_ctl start -M standby -D " + self.node_data_path_arr[i]  
                # + " > "  + self.node_data_path_arr[i] + "/logdn" + str(i) + ".log 2>&1 &"
            print(datanode_cmd)
            os.system(datanode_cmd)
            time.sleep(5)
        time.sleep(5)

    def __start_all(self):
        #clean evn
        self.__rm_pid_file()
        self.__start_primary()
        self.__start_standby()

    def __stop_all(self):
        for i in range(0, self.standby_node_num + 1):
            datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.node_data_path_arr[i]
            print(datanode_cmd)
            os.system(datanode_cmd)

    def __build_standby(self):
        for i in range(1, self.standby_node_num + 1):
            datanode_cmd = install_path + "/bin/gs_ctl build -D " + self.node_data_path_arr[i] + " -b full -M standby"
            print(datanode_cmd)
            os.system(datanode_cmd)

    def __primary_exec(self, db, sql):
        test_cmd = install_path + "/bin/gsql -U " + g_username + " -W " + g_passwd + " -d " + db + " -p " + str(self.ha_port_arr[0]) + " -r -c \"" + sql + "\";"
        print(test_cmd)
        os.system(test_cmd)
        time.sleep(3)

    def __standby_exec(self, db, sql):
        for i in range(1, self.standby_node_num + 1):
            test_cmd = install_path + "/bin/gsql -U " + g_username + " -W " + g_passwd + " -d " + db + " -p " + str(self.ha_port_arr[i]) + " -r -c \"" + sql + "\";"
            print(test_cmd)
            os.system(test_cmd)

    def __test_all_node(self):
        # on primary create database dbtest1
        self.__primary_exec("postgres", "CREATE DATABASE dbtest1")
        self.__primary_exec("dbtest1", "CREATE TABLE foo(id int); insert into foo (id) values (123); select * from foo")
        self.__standby_exec("dbtest1", "select * from foo")

    def run(self, run_type):
        #self.kill_process()
        if(run_type == "init"):
            self.init_env()
            self.__save_nodes_info()
            self.__read_nodes_info()
            self.__start_primary()
            self.__build_standby()
            self.__test_all_node()
            print("init, start, test ok")
        elif(run_type == "start"):
            self.__read_nodes_info()
            self.__start_all()
            print("start ok")
        elif(run_type == "stop"):
            self.__read_nodes_info()
            self.__stop_all()
            print("stop ok")

def usage():
    print("------------------------------------------------------")
    print("python pgxc.py\n")
    print("	-t trace compression log")
    print("	-s means start")
    print("	-o means stop")
    print("	-g means memcheck")
    print("	-D data directory")
    print("	-r create regression group sql")
    print("------------------------------------------------------")

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hrD:c:d:il:t:sovg", ["help", "data_dir=", "regress="])
    except getopt.GetoptError as err:
        # print help information and exit:
        print(str(err)) # will print something like "option -a not recognized"
        # usage()
        sys.exit(2)

    coordinator_num = 0
    datanode_num = 0
    global g_valgrind
    global g_file_node_info
    global g_trace_compress
    global g_data_path

    run_type = "init"
    level = 0

    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--init"):
            run_type = "init"
        elif o in ("-l", "--level"):
            level = a
        elif o in ("-D", "data_dir"):
            g_data_path = a
        elif o in ("-d", "--datanode"):
            datanode_num = int(a)
        elif o in ("-s", "--start"):
            run_type = "start"
        elif o in ("-o", "--stop"):
            run_type = "stop"
        else:
            assert False, "unhandled option"

    if((datanode_num == 0 or not level.isdigit()) and run_type == "init"):
        print("run type is %s, but datanode num is %d, level is %s" %(run_type, datanode_num, level))
        usage()
        sys.exit()

    g_file_node_info = g_data_path + "/" + g_file_node_info
    ptdb = Pterodb(datanode_num, level, g_data_path)
    ptdb.run(run_type)

if __name__ == "__main__":
    main()
