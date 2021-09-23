#!/usr/bin/python

import getopt, sys, os
import shutil
import time
import string
from os.path import dirname, abspath

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
	def __init__(self, coordinator_num, data_node_num, data_dir, dcf_port, new_node_id, new_node_port):
		self.coordinator_num = coordinator_num
		self.data_node_num = data_node_num
		self.data_dir = data_dir
		self.dname_prefix = "datanode"
		print self.data_dir
		self.ha_port_arr = [0 for i in range(data_node_num+1)]
		self.service_port_arr = [0 for i in range(data_node_num+1)]
		self.heartbeat_port_arr = [0 for i in range(data_node_num+1)]
		self.dcf_port = dcf_port
		self.new_node_id = new_node_id
		self.new_node_port = new_node_port
	def init_single_env(self):
		#generate port array
		self.__generate_port()
		i = 6
		#standby
		datanode_cmd_standby = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i-1) + \
		                       "_standby" + " --nodename=" + self.dname_prefix + str(1)  + " -w " + g_passwd + " -c"
		print datanode_cmd_standby
		os.system(datanode_cmd_standby)
		#standby
		conf_file_standby = self.data_dir + "/" + self.dname_prefix + str(i-1) + "_standby" + "/postgresql.conf"
		self.__modify_conf_port(conf_file_standby,i + self.coordinator_num - 1,i)
		self.__turn_on_pg_log(conf_file_standby)
		self.__modify_conf_standby(conf_file_standby,i,i)
		self.__modify_sinle_paxos(conf_file_standby)
		self.__modify_conf_application_name(conf_file_standby, "dn_s" + str(i - 1))
		self.__modify_remote_read_mode(conf_file_standby)

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
				datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + \
				                    " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd + " -c"
				print datanode_cmd_init
				os.system(datanode_cmd_init)
			
				#primary
				conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
				self.__modify_conf_port(conf_file,i+self.coordinator_num-1,1)
				self.__turn_on_pg_log(conf_file)
				self.__modify_conf_standby(conf_file,i,1)
				self.__modify_paxos(conf_file, i)
				self.__modify_conf_application_name(conf_file, "dn_p" + str(i))
				self.__modify_remote_read_mode(conf_file)
			else:
				#standby
				datanode_cmd_standby = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i-1) + \
				                       "_standby" + " --nodename=" + self.dname_prefix + str(1)  + " -w " + g_passwd + " --enable-dcf"
				print datanode_cmd_standby
				os.system(datanode_cmd_standby)
 				#standby
				conf_file_standby = self.data_dir + "/" + self.dname_prefix + str(i-1) + "_standby" + "/postgresql.conf"
				self.__modify_conf_port(conf_file_standby,i+self.coordinator_num-1,i)
				self.__turn_on_pg_log(conf_file_standby)
				self.__modify_conf_standby(conf_file_standby,i,i)
				self.__modify_paxos(conf_file_standby, i)
				self.__modify_conf_application_name(conf_file_standby, "dn_s" + str(i - 1))
				self.__modify_remote_read_mode(conf_file_standby)

	def __generate_port(self):
		port = g_base_standby_port
		for i in range(0,self.data_node_num+1):
			self.ha_port_arr[i] = port + 1
			self.service_port_arr[i] = port + 2
			self.heartbeat_port_arr[i] = port + 3
			port = port + 3

	def __modify_sinle_paxos(self, conf_file):
		file_handler = open(conf_file,"a")
		string = "dcf_log_level = 'RUN_ERR|RUN_WAR|RUN_INF|DEBUG_ERR|DEBUG_WAR|DEBUG_INF|TRACE|PROFILE|OPER'\n"
		string += "dcf_max_workers = 10\n"
		string = string + "dcf_data_path = '" + dirname(abspath(conf_file)) + "/dcf_data'\n"
		string = string + "dcf_log_path = '" + dirname(abspath(conf_file)) +  "/dcf_log'\n"
		string = string + "dcf_config = '[{\"stream_id\":1,\"node_id\":1,\"ip\":\"127.0.0.1\",\"port\":" + str(self.dcf_port) + ",\"role\":\"LEADER\"}"
		replaced = False
		for i in range(1, self.data_node_num):
			if (i + 1) == self.new_node_id:
				string = string + ", {\"stream_id\":1,\"node_id\":" + str(i + 1) + ",\"ip\":\"127.0.0.1\",\"port\":" + str(self.new_node_port) + ",\"role\":\"FOLLOWER\"}"
				replaced = True
			else:
				string = string + ", {\"stream_id\":1,\"node_id\":" + str(i + 1) + ",\"ip\":\"127.0.0.1\",\"port\":" + str(self.dcf_port + i) + ",\"role\":\"FOLLOWER\"}"
		if not replaced:
			string = string + ", {\"stream_id\":1,\"node_id\":" + str(self.new_node_id) + ",\"ip\":\"127.0.0.1\",\"port\":" + str(self.new_node_port) + ",\"role\":\"FOLLOWER\"}"

		string += "]'\n"
		string = string + "dcf_node_id = " + str(self.new_node_id) + "\n"
		file_handler.write(string)
		file_handler.close()

	def __modify_paxos(self, conf_file, n):
		file_handler = open(conf_file,"a")
		string = "dcf_log_level = 'RUN_ERR|RUN_WAR|RUN_INF|DEBUG_ERR|DEBUG_WAR|DEBUG_INF|TRACE|PROFILE|OPER'\n"
		string += "dcf_max_workers = 10\n"
		string = string + "dcf_data_path = '" + dirname(abspath(conf_file)) + "/dcf_data'\n"
		string = string + "dcf_log_path = '" + dirname(abspath(conf_file)) +  "/dcf_log'\n"
		string = string + "dcf_config = '[{\"stream_id\":1,\"node_id\":1,\"ip\":\"127.0.0.1\",\"port\":" + str(self.dcf_port) + ",\"role\":\"LEADER\"}"
		for i in range(1, self.data_node_num + 1):
			string = string + ", {\"stream_id\":1,\"node_id\":" + str(i + 1) + ",\"ip\":\"127.0.0.1\",\"port\":" + str(self.dcf_port + i) + ",\"role\":\"FOLLOWER\"}"

		string += "]'\n"
		string = string + "dcf_node_id = " + str(n) + "\n"
		file_handler.write(string)
		file_handler.close()

	def __modify_conf_standby(self, conf_file, n, flag):
		j = 1

		file_handler = open(conf_file,"a")
		string = "listen_addresses = '*'"+ "\n"
		file_handler.write(string)

		for i in range(1,self.data_node_num + 2):
			if(i != n):
				#repl
				string = "replconninfo%d = 'localhost=%s localport=%d localheartbeatport=%d localservice=%d remotehost=%s remoteport=%d remoteheartbeatport=%d remoteservice=%d'\n" % \
				(j, g_local_ip, self.ha_port_arr[n-1], self.heartbeat_port_arr[n-1], self.service_port_arr[n-1], g_local_ip, self.ha_port_arr[i-1], self.heartbeat_port_arr[i-1], self.service_port_arr[i-1])
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
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)
			
			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + "notify -M standby" + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(5)

			#datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + " start -M standby " + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			#print datanode_cmd
			#os.system(datanode_cmd)
			#time.sleep(5)
			break;

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

			#datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + " start -M standby "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			#print datanode_cmd
			#os.system(datanode_cmd)
			#time.sleep(5)

		#full build for standbys
		for i in range(1,self.data_node_num+1):
			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl " + " build -b full -Z single_node " + " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(10)
			break;

		for i in range(1,self.data_node_num+1):
			datanode_cmd = g_valgrind + install_path + "/bin/gs_ctl" + " build -b full -Z single_node "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "_standby" + " > "  + self.data_dir + "/" + self.dname_prefix + str(i) +"_standby"+ "/logdn" + str(i) + ".log 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
			time.sleep(10)

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
			print filepath
		elif (run_type == 5):
			self.init_single_env()
			print "init a sinle node ok"

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
	print " -a add a node to cluster"
	print "------------------------------------------------------"

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "ahrD:c:d:n:t:sop:q:vg", ["help", "data_dir=", "regress="])
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		# usage()
		sys.exit(2)

	coordinator_num = 0
	datanode_num = 0
	dcf_port=13001
	new_node_id = 6
	new_node_port = 13006
	global g_valgrind
	global g_file_node_info
	global g_trace_compress

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
		elif o in ("-p", "--dcf_port"):
			dcf_port = int(a)
		elif o in ("-q", "--new_node_port"):
			new_node_port = int(a)
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
		elif o in ("-a", "--add"):
			run_type = 5
		elif o in ("-n", "--new_node_id"):
			new_node_id = int(a)
		else:
			assert False, "unhandled option"

	if((datanode_num == 0) and run_type == 0):
		usage()
		sys.exit()

	g_file_node_info = data_dir + "/" + g_file_node_info
	ptdb = Pterodb(coordinator_num,datanode_num, data_dir, dcf_port, new_node_id, new_node_port)
	ptdb.run(run_type)


if __name__ == "__main__":
	main()
