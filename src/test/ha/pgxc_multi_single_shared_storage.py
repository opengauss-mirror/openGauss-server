#!/usr/bin/python

import getopt, sys, os
import shutil
import time
import string

g_base_port = int(os.environ.get("g_base_port"))
g_pooler_base_port = int(os.environ.get("g_pooler_base_port"))
g_base_standby_port = int(os.environ.get("g_base_standby_port"))
install_path = os.environ.get("install_path")
g_data_path = os.environ.get("g_data_path")
g_local_ip = os.environ.get("g_local_ip")
g_file_node_info = "cndn.cnf"
g_valgrind = ""
g_passwd = "Gauss@123"
g_trace_compress = False


class Pterodb():
	def __init__(self, coordinator_num, data_node_num, data_dir):
		self.coordinator_num = coordinator_num
		self.data_node_num = data_node_num
		self.data_dir = data_dir
		self.dname_prefix = "datanode"
		print self.data_dir
		self.ha_port_arr = [0 for i in range(data_node_num+1)]
		self.service_port_arr = [0 for i in range(data_node_num+1)]
		self.heartbeat_port_arr = [0 for i in range(data_node_num+1)]

	def init_env(self):
		if(os.path.exists(self.data_dir) == False):
			os.mkdir(self.data_dir)
		else:
			shutil.rmtree(self.data_dir)
			os.mkdir(self.data_dir)
			print "rm dir ok"

		#generate port array
		self.__generate_port()

		for i in range(1,self.data_node_num + 2):
 			if(i == 1):
				 #primary
				datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd + " -g " + self.data_dir + "/shared_disk"
				print datanode_cmd_init
				os.system(datanode_cmd_init)
			
				#primary
				conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
				self.__modify_conf_port(conf_file,i+self.coordinator_num-1,1)
				self.__turn_on_pg_log(conf_file)
				self.__modify_conf_standby(conf_file,i,1,0)
				self.__modify_conf_application_name(conf_file, "dn_p" + str(i))
				self.__modify_remote_read_mode(conf_file)
                                self.__modify_conf_shared_storage(conf_file, 0)
                                self.__modify_conf_standby_shared_storage(conf_file,i,1,0)
			else:
				#standby
				datanode_cmd_standby = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i-1) + "_standby" + " --nodename=" + self.dname_prefix + str(1)  + " -w " + g_passwd
				print datanode_cmd_standby
				os.system(datanode_cmd_standby)
 				#standby
				conf_file_standby = self.data_dir + "/" + self.dname_prefix + str(i-1) + "_standby" + "/postgresql.conf"
				self.__modify_conf_port(conf_file_standby,i+self.coordinator_num-1,i)
				self.__turn_on_pg_log(conf_file_standby)
				self.__modify_conf_application_name(conf_file_standby, "dn_s" + str(i - 1))
				self.__modify_remote_read_mode(conf_file_standby)
                                if (i < self.data_node_num):
                                    self.__modify_conf_shared_storage(conf_file_standby, 0)
                                    self.__modify_conf_standby(conf_file_standby,i,i,0)
                                    self.__modify_conf_standby_shared_storage(conf_file_standby,i,i,0)
                                else:
                                    self.__modify_conf_shared_storage(conf_file_standby, 1)
                                    self.__modify_conf_standby(conf_file_standby,i,i,1)
                                    self.__modify_conf_standby_shared_storage(conf_file_standby,i,i,1)

	def __generate_port(self):
		port = g_base_standby_port
		for i in range(0,self.data_node_num+1):
			self.ha_port_arr[i] = port + 1;
			self.service_port_arr[i] = port + 2;
			self.heartbeat_port_arr[i] = port + 3;
			port = port + 3;

	def __modify_conf_standby(self, conf_file, n, flag, cluster_type):
		j = 1

		file_handler = open(conf_file,"a")
		string = "listen_addresses = '*'"+ "\n"
		file_handler.write(string)
                
                if cluster_type == 0:
                    st = 1
                    ed = self.data_node_num / 2 + 2
                else:
                    st = self.data_node_num / 2 + 2
                    ed = self.data_node_num + 2
                
		for i in range(st,ed):
			if(i != n):
				#repl
				string = "replconninfo%d = 'localhost=%s localport=%d localheartbeatport=%d localservice=%d remotehost=%s remoteport=%d remoteheartbeatport=%d remoteservice=%d'\n" % \
				(j, g_local_ip, self.ha_port_arr[n-1], self.heartbeat_port_arr[n-1], self.service_port_arr[n-1], g_local_ip, self.ha_port_arr[i-1], self.heartbeat_port_arr[i-1], self.service_port_arr[i-1])
				print string
				file_handler.write(string)
				j = j + 1

		file_handler.close()

        def __modify_conf_standby_shared_storage(self, conf_file, n, flag, cluster_type):
                j = 1

                file_handler = open(conf_file,"a")

                if cluster_type == 1:
                    st = 1
                    ed = self.data_node_num / 2 + 2
                else:
                    st = self.data_node_num / 2 + 2
                    ed = self.data_node_num + 2

                for i in range(st,ed):
                        if(i != n):
                                #repl
                                string = "cross_cluster_replconninfo%d = 'localhost=%s localport=%d localservice=%d remotehost=%s remoteport=%d remoteservice=%d'\n" % \
                                (j, g_local_ip, self.ha_port_arr[n-1], self.service_port_arr[n-1], g_local_ip, self.ha_port_arr[i-1], self.service_port_arr[i-1])
                                print string
                                file_handler.write(string)
                                j = j + 1

                file_handler.close()


	def __modify_conf_application_name(self, conf_file, name):
		file_handler = open(conf_file,"a")
		string = "application_name = '" + name + "'" + "\n"
		file_handler.write(string)
		file_handler.close()

        def __modify_remote_read_mode(self, conf_file):
                file_handler = open(conf_file,"a")
                string = "remote_read_mode = 'off'" + "\n"
                file_handler.write(string)
                file_handler.close()

	def __modify_conf_shared_storage(self, conf_file, cluster_type):
                file_handler = open(conf_file,"a")
                string = "xlog_file_path = '" + self.data_dir + "/shared_disk'\n"
                string = string + "xlog_file_size = 68719476736\n"
                if cluster_type == 0:
                    string = string + "cluster_run_mode = cluster_primary\n"
                else:
                    string = string + "cluster_run_mode = cluster_standby\n"                    
                file_handler.write(string)
                file_handler.close()

        def __modify_conf_port(self, conf_file, n, role_flag):
		file_handler = open(conf_file,"a")
		port = g_base_port + 3 * n

		string = "port = " + str(port) + "\n"
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

	def __switch_trace_cmpr(self):
		for i in range(1, self.data_node_num+1):
			conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
			file_handler = open(conf_file,"a")
			if g_trace_compress:
				pglog_conf = "log_line_prefix = '' \n"
				pglog_conf = pglog_conf + "log_min_messages = info \n"
				file_handler.write(pglog_conf)
			else:
				pglog_conf = "log_line_prefix = '%m %c %d %p %a %x %e ' \n"
				pglog_conf = pglog_conf + "log_min_messages = warning \n"
				file_handler.write(pglog_conf)
			file_handler.close()

	def __create_regress_group(self):
		create_regress_group = self.data_dir + "/create_regress_group.sql"
		#the default group is group1
                sql = "CREATE NODE GROUP group1 WITH ("
		for i in range(1,2):
			sql = sql + self.dname_prefix +str(i)+ ","
		sql = sql[:-1]
		sql = sql + ");"
		file_handler = open(create_regress_group, "w")
		file_handler.write(sql)
		file_handler.close()
		time.sleep(2)
		# execute create group sql
		port = g_base_port
		for i in range(0, self.coordinator_num):
			cmd = install_path + "/bin/gsql -p " + str(port + 3 * i) + " postgres < " + create_regress_group
			print cmd
			os.system(cmd)
		#return create_regress_group

	def __create_node(self):
		create_nodes_sql = self.data_dir + "/create_nodes.sql"
		Sql = "delete from pgxc_node;\n"
		for i in range(1,self.coordinator_num+1):
			port = g_base_port + 3* (i - 1)
			Sql = Sql + "CREATE NODE coordinator" + str(i) + " WITH (HOST = 'localhost', type = 'coordinator', PORT = " + str(port) + ");\n"

		port = g_base_port + 3 * self.coordinator_num
		for i in range(1,self.data_node_num+2):
			#Sql = Sql + "CREATE NODE " + self.dname_prefix +str(i)+ " WITH (type = 'datanode', HOST = 'localhost', PORT = " + str(port) + ",sctp_port = " + str(g_base_port+(i+self.coordinator_num-1)*128) + ",  control_port = " + str(port+1) + ", host1 = 'localhost', port1= "+str(port+2) + ", port2= " + str(port+4) + ", sctp_port = " + str(g_base_port+(i+self.coordinator_num-1)*128+64) + ", control_port = " + str(port+3) + ");\n"
			if(i == 1):
                            Sql = Sql + "CREATE NODE " + self.dname_prefix + str(1) + " WITH (type = 'datanode', RW = 'true', HOST = 'localhost', PORT = " + str(port + (i-1)*3) + ",sctp_port = " + str(g_base_port+(i+self.coordinator_num-1)*128) + ", control_port = " + str(port + (i-1)*3 + 2) + ");\n"
			else:
                            Sql = Sql + "CREATE NODE " + self.dname_prefix + str(1) + " WITH (type = 'datanode', RW = 'false', HOST = 'localhost', PORT = " + str(port +(i-1)*3) + ",sctp_port = " + str(g_base_port+(i+self.coordinator_num-1)*128) + ", control_port = " + str(port + (i-1)*3 + 2) + ");\n"
		file_handler = open(create_nodes_sql, "w")
		file_handler.write(Sql)
		file_handler.close()
		time.sleep(2)
		#execute create node sql
		for i in range(1,self.coordinator_num+1):
			port = g_base_port + 3* (i - 1)
			cmd = install_path + "/bin/gsql -p " + str(port) + " postgres < " + create_nodes_sql
			print cmd
			os.system(cmd)

	def __create_default_db(self):
		# connect to primary DN to create db
		cmd = install_path + "/bin/gsql -p " + str(g_base_port + 3) + " postgres -c 'create database test'"
		os.system(cmd)

	def __rm_pid_file(self):
		cmd = "rm -rf "
		 # dn
	        for i in range(1,self.data_node_num+2):
                        if(i == 1):
			    rm_cmd = cmd + self.data_dir + "/" + self.dname_prefix + str(i) + "/postmaster.pid"
			    print rm_cmd
			    os.system(rm_cmd)
                        else:
                            rm_cmd = cmd + self.data_dir + "/" + self.dname_prefix + str(i-1) + "_standby" +"/postmaster.pid"
                            print rm_cmd
                            os.system(rm_cmd)


	#save coor_num and datanode num
	def __save_nodes_info(self):
		file_nodes_info = open(g_file_node_info,"w")
		file_nodes_info.write(str(self.coordinator_num))
		file_nodes_info.write("\n")
		file_nodes_info.write(str(self.data_node_num))
		file_nodes_info.write("\n")
		file_nodes_info.write(str(self.data_dir))
		file_nodes_info.write("\n")
		file_nodes_info.close()

	#read coor_num and datanode num
	def __read_nodes_info(self):
		file_nodes_info = open(g_file_node_info,"r")
		lines = file_nodes_info.readlines()
		self.coordinator_num = int (lines[0].strip())
		self.data_node_num = int (lines[1].strip())
		self.data_dir = lines[2].strip()

		file_nodes_info.close()

	def __start_server(self):
		#clean evn
		self.__rm_pid_file()

		#start data_node
		for i in range(1,self.data_node_num+1):
			datanode_cmd = g_valgrind + install_path + "/bin/gaussdb --single_node" + " -M pending" + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			time.sleep(2)
                        #datanode_cmd = g_valgrind + install_path + "bin/gaussdb --datanode" + " -M primary" + " -p " +str(port) + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + #" &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)
			
			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + "notify -M primary" + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
                        break;

		time.sleep(5)

		#start data_node_standby1,2,3...7
		for i in range(1,self.data_node_num+1):
			datanode_cmd = g_valgrind + install_path + "/bin/gaussdb --single_node" + " -M pending "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)

			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl" + " notify -M standby "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)

                        if i <= self.data_node_num/2:
			    datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl" + " build "+ "-D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + " -Z single_node " + " > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
                        else:
                            datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl" + " build -b cross_cluster_full "+ "-D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + " -Z single_node " + " > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)

		time.sleep(5)

	def __stop_server(self):
		#stop data node
		for i in range(1,self.data_node_num+1):
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)
                        break
		#stop data node standby1,2,3...7
		for i in range(1,self.data_node_num+1):
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + "_standby" + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)

	def run(self, run_type):
		#self.kill_process()
		if(run_type == 0):
			self.init_env()
			#print "init_env ok"
			self.__save_nodes_info()
			#print "save_nodes_info ok"
			self.__read_nodes_info()
			#print "read_nodes_info ok"
			self.__start_server()
			#print "start_server ok"
			#self.__create_node()
			#print "create_node ok"
			#self.__create_regress_group()
			#print "create_regress_group ok"
			self.__create_default_db()
			#print "create_default_db ok"
			print "start ok"
		elif(run_type == 1):
			self.__read_nodes_info()
			self.__start_server()
			print "start ok"
		elif(run_type == 2):
			self.__read_nodes_info()
			self.__stop_server()
			print "stop ok"
		elif (run_type == 3):
			self.__read_nodes_info()
			self.__switch_trace_cmpr()
			print "compress trace changed"
		elif (run_type == 4):
			self.__read_nodes_info()
			#filepath = self.__create_regress_group()
			print filepath

def usage():
	print "------------------------------------------------------"
	print "python pgxc.py\n"
	print "	-c coor_num -d datanode_num, set and start up cn/dn"
	print "	-t trace compression log"
	print "	-s means start"
	print "	-o means stop"
	print "	-g means memcheck"
	print "	-D data directory"
	print "	-r create regression group sql"
	print "------------------------------------------------------"

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hrD:c:d:t:sovg", ["help", "data_dir=", "regress="])
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		# usage()
		sys.exit(2)

	coordinator_num = 0
	datanode_num = 0
	global g_valgrind;
	global g_file_node_info;
	global g_trace_compress;

	data_dir = g_data_path
	#1 start
	#2 stop
	run_type = 0
	
	for o, a in opts:
		if o == "-v":
			verbose = True
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-D", "data_dir"):
			data_dir = a
		elif o in ("-c", "--coordinator"):
			coordinator_num = int(a)
		elif o in ("-d", "--datanode"):
			datanode_num = int(a)
		elif o in ("-s", "--start"):
			run_type = 1
		elif o in ("-o", "--stop"):
			run_type = 2
		elif o in ("-g", "--memcheck"):
			g_valgrind = "valgrind --tool=memcheck --leak-check=full  --log-file=memcheck.log "
			#g_valgrind = "valgrind --tool=massif --time-unit=B --detailed-freq=1 --massif-out-file=mass.out "
		elif o in ("-t", "--trace"):
			if 'on' == a:
				g_trace_compress = True
			else:
				g_trace_compress = False
			run_type = 3
			print g_trace_compress
		elif o in ("-r", "--regress"):
			run_type = 4
		else:
			assert False, "unhandled option"

	if((datanode_num == 0) and run_type == 0):
		usage()
		sys.exit()

	g_file_node_info = data_dir + "/" + g_file_node_info;
	ptdb = Pterodb(coordinator_num,datanode_num, data_dir)
	ptdb.run(run_type)


if __name__ == "__main__":
	main()
