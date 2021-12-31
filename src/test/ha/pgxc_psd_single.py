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

	def init_env(self):
		if(os.path.exists(self.data_dir) == False):
			os.mkdir(self.data_dir)
		else:
			shutil.rmtree(self.data_dir)
			os.mkdir(self.data_dir)
			print "rm dir ok"

		for i in range(1,self.data_node_num+1):
 			#primary       
			datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd
			print datanode_cmd_init
			os.system(datanode_cmd_init)
			#standby
			#datanode_cmd_standby = "cp -r " + self.data_dir + "/" + self.dname_prefix + str(i) + " " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby"
			datanode_cmd_standby = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd
			print datanode_cmd_standby
			os.system(datanode_cmd_standby)
			#xlogreceiver
			#datanode_cmd_dummystandby = "cp -r " + self.data_dir + "/" + self.dname_prefix + str(i) + " " + self.data_dir + "/" + self.dname_prefix + str(i) + "_dummystandby"
			datanode_cmd_dummystandby = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_dummystandby" + " --nodename=" + self.dname_prefix + str(i)  + "_dummystandby" + " -w " + g_passwd
			print datanode_cmd_dummystandby
			os.system(datanode_cmd_dummystandby)
			
			
			#primary
			conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
			self.__modify_conf_port(conf_file,3 * (i-1) + self.coordinator_num,1)
			self.__turn_on_pg_log(conf_file)
			self.__modify_conf_standby(conf_file,i,1)
			self.__modify_remote_read_mode(conf_file)
			#standby
			conf_file_standby = self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "/postgresql.conf"
			self.__modify_conf_port(conf_file_standby,3 * (i-1) + 1 + self.coordinator_num,2)
			self.__turn_on_pg_log(conf_file_standby)
			self.__modify_conf_standby(conf_file_standby,i,2)
			self.__modify_remote_read_mode(conf_file_standby)
            #xlogreceiver
			conf_file_dummystandby = self.data_dir + "/" + self.dname_prefix + str(i) + "_dummystandby" + "/postgresql.conf"
			self.__modify_conf_port(conf_file_dummystandby,3 * (i-1) + 2 + self.coordinator_num,3)
			self.__turn_on_pg_log(conf_file_dummystandby)
			self.__modify_conf_standby(conf_file_dummystandby,i,3)
			self.__modify_remote_read_mode(conf_file_dummystandby)
			
            # datanode standby
		#for i in range(1,self.data_node_num+1):
			#datanode_cmd_init = "cp -r " + self.data_dir + "/" + self.dname_prefix + str(i) + " " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby"
			#print datanode_cmd_init
			#os.system(datanode_cmd_init)
			#conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "/postgresql.conf"
			#self.__modify_conf_standby(conf_file,i,0)

        def __modify_remote_read_mode(self, conf_file):
                file_handler = open(conf_file,"a")
                string = "remote_read_mode = 'off'" + "\n"
                file_handler.write(string)
                file_handler.close()

	def __modify_conf_standby(self, conf_file, n, flag):
		if(flag == 1):
			file_handler = open(conf_file,"a")
			#listen_address
			port = g_base_standby_port +  n
			string = "listen_addresses = '*'"+ "\n"
			file_handler.write(string)
			#repl
			string = "replconninfo1 = 'localhost=" + g_local_ip + " localport=" + str(port) + " localheartbeatport=" + str(26203) + " localservice=" + str(port + 100) +" remotehost=" + g_local_ip + " remoteport=" + str(port + 1000) + " remoteheartbeatport=" + str(26205) + " remoteservice=" + str(port + 1000 + 100) + "'" + "\n"
			file_handler.write(string)
			#rep2, for xlogreceiver
			#10 mean allow n from 1 to 9
			port = g_base_standby_port +  n + 10
			string = "replconninfo2 = 'localhost=" + g_local_ip + " localport=" + str(port) +" remotehost=" + g_local_ip + " remoteport=" + str(port + 1000 + 10) + "'" + "\n"
			file_handler.write(string)
			file_handler.close()
			return
		elif(flag == 2):
			file_handler = open(conf_file,"a")
			#listen_address
			port = g_base_standby_port +  n
			string = "listen_addresses = '*'"+ "\n"
			file_handler.write(string)
			#repl
			string = "replconninfo1 = 'localhost=" + g_local_ip + " localport=" + str(port + 1000) + " localheartbeatport=" + str(26205) + " localservice=" + str(port + 1000 + 100) +" remotehost=" + g_local_ip + " remoteport=" + str(port) + " remoteheartbeatport=" + str(26203) + " remoteservice=" + str(port + 100) + "'" + "\n"
			file_handler.write(string)
			#rep2  for xlogreceiver
			#10 mean allow n from 1 to 9
			port = g_base_standby_port +  n + 10
			string = "replconninfo2 = 'localhost=" + g_local_ip + " localport=" + str(port + 1000) +" remotehost=" + g_local_ip + " remoteport=" + str(port + 1000 + 20) + "'" + "\n"
			file_handler.write(string)
			file_handler.close()
		else:
			file_handler = open(conf_file,"a")
			#listen_address
			port = g_base_standby_port + n + 10
			string = "listen_addresses = '*'"+ "\n"
			file_handler.write(string)
			#repl for primary
			string = "replconninfo1 = 'localhost=" + g_local_ip + " localport=" + str(port + 1000 + 10) +" remotehost=" + g_local_ip + " remoteport=" + str(port) +"'"+ "\n"
			file_handler.write(string)
			#rep2 for standby
			#10 mean allow n from 1 to 9
			port = g_base_standby_port + n + 10
			string = "replconninfo2 = 'localhost=" + g_local_ip + " localport=" + str(port + 1000 + 20) +" remotehost=" + g_local_ip + " remoteport=" + str(port + 1000) + "'" + "\n"
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
		sql = "CREATE NODE GROUP def_regress_group WITH ("
		for i in range(1,self.data_node_num+1):
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
			port = g_base_port + 3 * (i - 1)
			Sql = Sql + "CREATE NODE coordinator" + str(i) + " WITH (HOST = '127.0.0.1', type = 'coordinator', PORT = " + str(port) + ");\n"
		for i in range(1,self.data_node_num+1):
			port = g_base_port + 3 * (3 * (i - 1) + self.coordinator_num)
			Sql = Sql + "CREATE NODE " + self.dname_prefix +str(i)+ " WITH (type = 'datanode', HOST = '127.0.0.1', PORT = " + str(port) + ",sctp_port = " + str(g_base_port+(3 * (i - 1) + self.coordinator_num)*128) + ",  control_port = " + str(port + 2) + ", host1 = '127.0.0.1', port1= "+ str (port + 3) + ", sctp_port1 = " + str(g_base_port+(3 * (i - 1) + 1 + self.coordinator_num)*128+64) + ", control_port1 = " + str(port + 5) + ");\n"
		file_handler = open(create_nodes_sql, "w")
		file_handler.write(Sql)
		file_handler.close()
		time.sleep(2)
		#execute create node sql
		for i in range(1,self.coordinator_num+1):
			port = g_base_port + 3 * (i - 1)
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
		for i in range(1,self.data_node_num+1):
			rm_cmd = cmd + self.data_dir + "/" + self.dname_prefix + str(i) + "/postmaster.pid"
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
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(10)
			
			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + "notify -M primary" + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			
			print datanode_cmd
			os.system(datanode_cmd)
		time.sleep(5)
		#start data_node_standby
		for i in range(1,self.data_node_num+1):
			datanode_cmd = g_valgrind + install_path + "/bin/gaussdb --single_node" + " -M pending " + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(10)

			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl" + " notify -M standby "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			
			time.sleep(3)

			datanode_cmd = g_valgrind + install_path + "/bin/gaussdb --single_node" + " -M standby -R " + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_dummystandby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_dummystandby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
		time.sleep(1)

	def __stop_server(self):
		#stop data node
		for i in range(1,self.data_node_num+1):
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)
		#stop data node standby
		for i in range(1,self.data_node_num+1):
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + "_standby" + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)
			
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + "_dummystandby" + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)

	def run(self, run_type):
		#self.kill_process()
		if(run_type == 0):
			self.init_env()
			self.__save_nodes_info()
			self.__read_nodes_info()
			self.__start_server()
			#self.__create_node()
			#self.__create_regress_group()
			self.__create_default_db()
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
