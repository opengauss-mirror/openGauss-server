#!/usr/bin/python
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
#        src/test/ipv6/create_server.py
#
# ---------------------------------------------------------------------------------------

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
g_valgrind = ""
g_passwd = "Gauss@123"
g_trace_compress = False


class Pterodb():
	def __init__(self, data_node_num, data_dir):
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

		for i in range(1,self.data_node_num + 1):
			datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd
			print datanode_cmd_init
			os.system(datanode_cmd_init)
			
			conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
			self.__modify_conf_port(conf_file,i)
			self.__turn_on_pg_log(conf_file)
			if(self.data_node_num > 1):
				self.__modify_conf_standby(conf_file,i)

			self.__modify_conf_application_name(conf_file, "dn_p" + str(i))

	def __generate_port(self):
		port = g_base_standby_port
		for i in range(0,self.data_node_num):
			self.ha_port_arr[i] = port + 1;
			self.service_port_arr[i] = port + 2;
			self.heartbeat_port_arr[i] = port + 3;
			port = port + 3;

	def __modify_conf_standby(self, conf_file, n):
		j = 1

		file_handler = open(conf_file,"a")
		for i in range(1,self.data_node_num + 1):
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

	def __modify_conf_port(self, conf_file, n):
		file_handler = open(conf_file,"a")
		
		string = "listen_addresses = '*'"+ "\n"
		file_handler.write(string)
		
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

	def __create_default_db(self):
		# connect to primary DN to create db
		cmd = install_path + "/bin/gsql -p " + str(g_base_port + 3) + " postgres -c 'create database test'"
		os.system(cmd)

	def __rm_pid_file(self):
		cmd = "rm -rf "
		# dn
		for i in range(1,self.data_node_num+2):
			rm_cmd = cmd + self.data_dir + "/" + self.dname_prefix + str(i) + "/postmaster.pid"
			print rm_cmd
			os.system(rm_cmd)

	def run(self):
		self.init_env()
		print "create ok"

def usage():
	print "------------------------------------------------------"
	print "python create_server.py\n"
	print "	-t trace compression log"
	print "	-g means memcheck"
	print "	-D data directory"
	print "------------------------------------------------------"

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hrD:c:d:t:sovg", ["help", "data_dir=", "regress="])
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		# usage()
		sys.exit(2)

	datanode_num = 0
	global g_valgrind;
	global g_trace_compress;

	data_dir = g_data_path
	#1 start
	#2 stop
	
	for o, a in opts:
		if o == "-v":
			verbose = True
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-D", "data_dir"):
			data_dir = a
		elif o in ("-d", "--datanode"):
			datanode_num = int(a)
		elif o in ("-g", "--memcheck"):
			g_valgrind = "valgrind --tool=memcheck --leak-check=full  --log-file=memcheck.log "
		elif o in ("-t", "--trace"):
			if 'on' == a:
				g_trace_compress = True
			else:
				g_trace_compress = False
			run_type = 3
			print g_trace_compress
		else:
			assert False, "unhandled option"

	if(datanode_num == 0):
		usage()
		sys.exit()

	ptdb = Pterodb(datanode_num, data_dir)
	ptdb.init_env()


if __name__ == "__main__":
	main()
