#!/usr/bin/python

import getopt, sys, os
import shutil
import time
import string
from multiprocessing import Process

g_data_path = os.environ.get("g_data_path")
install_path = os.environ.get("install_path")
g_local_ip = os.environ.get("g_local_ip")
g_username = os.environ.get("g_username")
g_passwd = "Gauss@123"
dbcompatibility = os.environ.get("dbcompatibility")
pub_node1_port = int(os.environ.get("pub_node1_port"))
pub_node2_port = int(os.environ.get("pub_node2_port"))
pub_node3_port = int(os.environ.get("pub_node3_port"))
sub_node1_port = int(os.environ.get("sub_node1_port"))
sub_node2_port = int(os.environ.get("sub_node2_port"))
sub_node3_port = int(os.environ.get("sub_node3_port"))


class Pterodb():
	def __init__(self, data_node_num, port_arr, data_dir, dname_prefix):
		self.data_node_num = data_node_num
		self.data_dir = data_dir
		self.dname_prefix = dname_prefix

		self.ha_port_arr = port_arr
		self.service_port_arr = [port_arr[i] + 1 for i in range(data_node_num)]
		self.heartbeat_port_arr = [port_arr[i] + 2 for i in range(data_node_num)]

	def real_init_env(self, i):
		datanode_cmd_init = install_path + "/bin/gs_initdb -D " + self.data_dir + "/" + self.dname_prefix + str(i) + " --nodename=" + self.dname_prefix + str(i)  + " -w " + g_passwd + " --dbcompatibility=" + dbcompatibility
		print datanode_cmd_init
		os.system(datanode_cmd_init)

		conf_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/postgresql.conf"
		self.__modify_conf_port(conf_file,self.ha_port_arr[i-1])
		self.__turn_on_pg_log(conf_file)
		self.__modify_conf_standby(conf_file,i)
		self.__modify_conf_application_name(conf_file, "dn" + str(i))
		self.__modify_remote_read_mode(conf_file)

		hba_file = self.data_dir + "/" + self.dname_prefix + str(i) + "/pg_hba.conf"
		self.__config_replication_hba(hba_file)

	def init_env(self):
		if(os.path.exists(self.data_dir) == False):
			os.mkdir(self.data_dir)

		processes = []
		for i in range(1, self.data_node_num + 1):
			process = Process(target=self.real_init_env, args=(i,))
			process.daemon = True
			processes.append(process)
			process.start()
		
		for process in processes:
			if process.is_alive():
				process.join()

	def __modify_conf_standby(self, conf_file, n):
		j = 1

		file_handler = open(conf_file,"a")
		string = "listen_addresses = '*'"+ "\n"
		file_handler.write(string)

		for i in range(1, self.data_node_num + 1):
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

	def __modify_conf_port(self, conf_file, port):
		file_handler = open(conf_file,"a")

		string = "\n" + "port = " + str(port) + "\n"
		file_handler.write(string)

		file_handler.close()

	def __turn_on_pg_log(self, conf_file):
		file_handler = open(conf_file,"a")
		pglog_conf =  "logging_collector = on   \n"
		pglog_conf = pglog_conf + "log_directory = 'pg_log' \n"
		pglog_conf = pglog_conf + "log_line_prefix = '%m %c %d %p %a %x %e ' \n"
		pglog_conf = pglog_conf + "client_min_messages = ERROR \n"
		pglog_conf = pglog_conf + "enable_data_replicate = off \n"
		pglog_conf = pglog_conf + "replication_type = 1 \n"
		pglog_conf = pglog_conf + "wal_level = logical \n"
		pglog_conf = pglog_conf + "max_wal_senders = 8 \n"
		pglog_conf = pglog_conf + "enable_slot_log = on \n"
		file_handler.write(pglog_conf)
		file_handler.close()

	def __config_replication_hba(self, hba_file):
		file_handler = open(hba_file,"a")
		replication_line = "host replication " + str(g_username) + " " + str(g_local_ip) + "/32 trust"
		file_handler.write(replication_line)
		file_handler.close()

	def __create_default_db(self):
		# connect to primary DN to create db
		cmd = install_path + "/bin/gsql -p " + str(self.ha_port_arr[0]) + " postgres -c 'create database test'"
		os.system(cmd)

	def __rm_pid_file(self):
		cmd = "rm -rf "
		# dn
		for i in range(1,self.data_node_num+1):
			rm_cmd = cmd + self.data_dir + "/" + self.dname_prefix + str(i) + "/postmaster.pid"
			print rm_cmd
			os.system(rm_cmd)

	def __real_start_server(self, i):
		datanode_cmd = install_path + "/bin/gs_ctl" + " start -M standby "+ " -D " + self.data_dir + "/" + self.dname_prefix + str(i) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1"
		print datanode_cmd
		os.system(datanode_cmd)

		datanode_cmd = install_path + "/bin/gs_ctl" + " build "+ "-D " + self.data_dir + "/" + self.dname_prefix + str(i) + " -Z single_node " + " > "  + self.data_dir + "/" + self.dname_prefix + str(i) + "/logdn" + str(i) + ".log 2>&1"
		print datanode_cmd
		os.system(datanode_cmd)

	def __start_server(self):
		#clean evn
		self.__rm_pid_file()

		#start primary
		datanode_cmd = install_path + "/bin/gs_ctl " + "start -M primary" + " -D " + self.data_dir + "/" + self.dname_prefix + str(1) + "   > "  + self.data_dir + "/" + self.dname_prefix + str(1) + "/logdn" + str(1) + ".log 2>&1 &"
		print datanode_cmd
		os.system(datanode_cmd)

		time.sleep(5)

		processes = []
		#start data_node_standby1,2,3...7
		for i in range(2,self.data_node_num+1):
			process = Process(target=self.__real_start_server, args=(i,))
			process.daemon = True
			processes.append(process)
			process.start()

		for process in processes:
			if process.is_alive():
				process.join()

		time.sleep(5)

	def __stop_server(self):
		for i in range(1,self.data_node_num+1):
			datanode_cmd = install_path + "/bin/gs_ctl stop -D " + self.data_dir + "/" + self.dname_prefix  + str(i) + " -Z single_node"
			print datanode_cmd
			os.system(datanode_cmd)

	def run(self, run_type):
		if(run_type == 0):
			self.init_env()
			#print "init_env ok"
			self.__start_server()
			#print "start_server ok"
			self.__create_default_db()
			#print "create_default_db ok"
			print "start ok"
		elif(run_type == 1):
			self.__start_server()
			print "start ok"
		elif(run_type == 2):
			self.__stop_server()
			print "stop ok"

def usage():
	print "------------------------------------------------------"
	print "python pubsub.py\n"
	print "	-d datanode_num, set and start up dn"
	print "	-s means start"
	print "	-o means stop"
	print "	-D data directory"
	print "------------------------------------------------------"

def real_run(ptdb, run_type):
	ptdb.run(run_type)

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hD:d:sov", ["help", "data_dir="])
	except getopt.GetoptError, err:
		# print help information and exit:
		print str(err) # will print something like "option -a not recognized"
		# usage()
		sys.exit(2)

	datanode_num = 0
	data_dir = g_data_path
	run_type = 0
	
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
		elif o in ("-s", "--start"):
			run_type = 1
		elif o in ("-o", "--stop"):
			run_type = 2
		else:
			assert False, "unhandled option"

	if((datanode_num == 0) and run_type == 0):
		usage()
		sys.exit()

	create_key_cipher_cmd = install_path + "/bin/gs_guc generate -S " + g_passwd + " -D " + install_path + "/bin -o subscription"
	print create_key_cipher_cmd
	os.system(create_key_cipher_cmd)

	pub_port_arr = [pub_node1_port, pub_node2_port, pub_node3_port];
	pub_ptdb = Pterodb(datanode_num, pub_port_arr, data_dir, "pub_datanode");
	pub_process = Process(target=real_run, args=(pub_ptdb,run_type,))
	pub_process.start()

	if pub_process.is_alive():
		pub_process.join()

	sub_port_arr = [sub_node1_port, sub_node2_port, sub_node3_port];
	sub_ptdb = Pterodb(datanode_num, sub_port_arr, data_dir, "sub_datanode");
	sub_process = Process(target=real_run, args=(sub_ptdb,run_type,))
	sub_process.start()

	if sub_process.is_alive():
		sub_process.join()


if __name__ == "__main__":
	main()
