#!/usr/bin/python
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
# initdb.py
#        This script is used to build the database when the environment is 
#        reproduced using gs_plansimulator.
# 
# IDENTIFICATION
#        src/bin/gs_plan_simulator/dependences/initdb.py
# 
# ---------------------------------------------------------------------------------------
import getopt, sys, os
import shutil
import time
import random
import string
#This port is invalid. The user will specify the port when using gs_plansimulator.
base_port = 7500
g_gtm_port = 0
port_ = base_port
g_file_node_info = "cndn.cnf"
bin_path = os.getenv('GAUSSHOME') + '/'
g_data_path = "data"
g_valgrind = ""

coordnode_port=[]
coordnode_control_port=[]
coordnode_pooler_port=[]
coordnode_stream_ctl_port=[]
coordnode_sctp_port=[]

datanode_port=[]
datanode_control_port=[]
datanode_pooler_port=[]
datanode_stream_ctl_port=[]
datanode_sctp_port=[]


class Pterodb():
	def __init__(self, coordinator_num, data_node_num, data_dir):
		self.coordinator_num = coordinator_num
		self.data_node_num = data_node_num
		self.data_dir = data_dir

	def get_rand_string(self):
		with open("/dev/random", "rb") as fd:
			st = fd.read(13)
		return "%s%s" % ("GS@", st.encode("hex"))

	def init_env(self):
		if(os.path.exists(self.data_dir) == False):
			os.mkdir(self.data_dir)
		else:
			shutil.rmtree(self.data_dir)
			os.mkdir(self.data_dir)
			print "rm dir ok"
		# init GTM
		gtm_cmd_init = bin_path + "bin/gs_initgtm -D " + self.data_dir + "/gtm_data -Z gtm";
		print gtm_cmd_init
		os.system(gtm_cmd_init)
		global g_gtm_port
		g_gtm_port = base_port + self.coordinator_num * 8 + self.data_node_num * 8
		conf_file = self.data_dir + "/gtm_data/gtm.conf"
		global port_
		self.__modify_conf_port(conf_file, 100, 1)
		pwd = self.get_rand_string()
		for i in range(1, self.coordinator_num+1):
			coor_cmd_init = bin_path + "bin/gs_initdb -w " + pwd + " -D " + self.data_dir + "/coordinator" + str(i) + " --nodename=coordinator" + str(i)
			os.system(coor_cmd_init)
			conf_file = self.data_dir + "/coordinator"+ str(i) + "/postgresql.conf"
			self.__modify_conf_port(conf_file, i-1, -1)

		for i in range(1, self.data_node_num+1):
			datanode_cmd_init = bin_path + "bin/gs_initdb -w " + pwd + " -D " + self.data_dir + "/data_node" + str(i) + " --nodename=datanode" + str(i)
			os.system(datanode_cmd_init)
			conf_file = self.data_dir + "/data_node"+ str(i) + "/postgresql.conf"
			self.__modify_conf_port(conf_file, i, 0)

	def __modify_conf_port(self, conf_file, n, gtm_flag):
		global port_
		if(gtm_flag == 1):
			file_handler = open(conf_file)
			data = file_handler.read()
			file_handler.close()
			file_handler = open(conf_file, "w")
			data = data.replace("6666", str(g_gtm_port))
			file_handler.write(data)
			file_handler.close()
			return
		elif(gtm_flag==0):
			file_handler = open(conf_file, "a")
			#port
			datanode_port.append(port_)
			port = port_
			port_ += 2
			string = "port = " + str(port) + "\n"
			file_handler.write(string)

			#comm_tcp_mode
			string = "comm_debug_mode = off\n"
			file_handler.write(string)

			#comm_control_port
			datanode_control_port.append(port_)
			port = port_
			port_ += 2
			string = "comm_control_port = " + str(port) + "\n"
			file_handler.write(string)

			#comm_sctp_port
			datanode_sctp_port.append(port_)
			port = port_
			port_ += 2
			string = "comm_sctp_port  = " + str(port) + "\n"
			file_handler.write(string)

			#gtm port
			string = "gtm_port = " + str(g_gtm_port) + "\n"
			file_handler.write(string)
            
			#pooler port
			datanode_pooler_port.append(port_)
			port = port_
			port_ += 2
			string = "pooler_port = " + str(port) + "\n"
			file_handler.write(string)
		else:
			file_handler = open(conf_file, "a")
			#port
			coordnode_port.append(port_)
			port = port_
			port_ += 2
			string = "port = " + str(port) + "\n"
			file_handler.write(string)

			#comm_control_port
			coordnode_control_port.append(port_)
			port = port_
			port_ += 2
			string = "comm_control_port = " + str(port) + "\n"
			file_handler.write(string)

			#comm_sctp_port
			coordnode_sctp_port.append(port_)
			port = port_
			port_ += 2
			string = "comm_sctp_port  = " + str(port) + "\n"
			file_handler.write(string)

			#gtm port
			string = "gtm_port = " + str(g_gtm_port) + "\n"
			file_handler.write(string)
            
			#pooler port
			coordnode_pooler_port.append(port_)
			port = port_
			port_ += 2
			string = "pooler_port = " + str(port) + "\n"
			file_handler.write(string)
		file_handler.close()

	def __create_node(self):
		Sql = "delete from pgxc_node;\ndelete from pgxc_group;\n"

		nodegroup = 'CREATE NODE GROUP group1 WITH (';
		for i in range(1, self.data_node_num + 1):
			nodegroup = nodegroup + "datanode" + str(i)
			if i < self.data_node_num:
				nodegroup = nodegroup + ','
		nodegroup = nodegroup + ')'
		for i in range(1, self.coordinator_num+1):
			Sql = Sql + "CREATE NODE coordinator" + str(i) + " WITH (HOST = '127.0.0.1', type = 'coordinator', PORT = " + str(coordnode_port[i-1]) + ", SCTP_PORT = " + str(coordnode_sctp_port[i-1]) + ", CONTROL_PORT = " + str(coordnode_control_port[i-1]) + ");\n"
		for i in range(1, self.data_node_num+1):
			Sql = Sql + "CREATE NODE datanode" + str(i) + " WITH (HOST = '127.0.0.1', type = 'datanode', PORT = " + str(datanode_port[i-1]) + ", SCTP_PORT = " + str(datanode_sctp_port[i-1]) + ", CONTROL_PORT = " + str(datanode_control_port[i-1]) + ");\n"
			file_handler = open("create_nodes.sql", "w")
			file_handler.write(Sql)
			file_handler.close()
		time.sleep(5)
		#execute create node sql
		for i in range(1, self.coordinator_num+1):
			cmd = bin_path + "bin/gsql -p " + str(coordnode_port[i-1]) + " postgres -f create_nodes.sql"
			print cmd
			os.system(cmd)
		# create nodegroup
		port = base_port
		cmd = bin_path + "bin/gsql -p " + str(port) + " postgres -c '" + nodegroup + "'"
		print cmd
		os.system(cmd)

		# mark installation group
		for i in range(1, self.coordinator_num+1):
			port = coordnode_port[i-1]
			update_sql = "UPDATE pgxc_group SET is_installation = true WHERE group_name = 'group1';"
			cmd = bin_path + "bin/gsql -p " + str(port) + " postgres -c \"" + update_sql + "\""
			print "install group:" + cmd
			os.system(cmd)

	def __create_default_db(self):
		cmd = bin_path + "bin/gsql -p " + str(base_port) + " postgres -c 'create database test'"
		os.system(cmd)

	def __rm_pid_file(self):
		cmd = "rm -rf "
		# cn
		for i in range(1, self.coordinator_num+1):
			rm_cmd = cmd + self.data_dir + "/coordinator" + str(i) + "/postmaster.pid"
			print rm_cmd
			os.system(rm_cmd)
		# dn
		for i in range(1, self.data_node_num+1):
			rm_cmd = cmd + self.data_dir + "/data_node" + str(i) + "/postmaster.pid"
			print rm_cmd
			os.system(rm_cmd)
		#gtm
		rm_cmd = cmd + self.data_dir + "/gtm_data/gtm.pid"
		print rm_cmd
		os.system(rm_cmd)

	#save coor_num and datanode num
	def __save_nodes_info(self):
		file_nodes_info = open(g_file_node_info, "w")
		file_nodes_info.write(str(self.coordinator_num))
		file_nodes_info.write("\n")
		file_nodes_info.write(str(self.data_node_num))
		file_nodes_info.write("\n")
		file_nodes_info.write(str(self.data_dir))
		file_nodes_info.write("\n")
		file_nodes_info.close()

	#read coor_num and datanode num
	def __read_nodes_info(self):
		file_nodes_info = open(g_file_node_info, "r")
		lines = file_nodes_info.readlines()
    
		self.coordinator_num = int (lines[0].strip())
		self.data_node_num = int (lines[1].strip())
		self.data_dir = lines[2].strip()

		file_nodes_info.close()
        
	def __start_server(self):

		#clean evn
		self.__rm_pid_file()
		#start gtm
		gtm_cmd = bin_path + "bin/gs_gtm -D " + self.data_dir + "/gtm_data &"
		print gtm_cmd
		os.system(gtm_cmd)
		time.sleep(1)
		#start coor
		for i in range(1, self.coordinator_num+1):
			coor_cmd = bin_path + "bin/gaussdb --coordinator -D " + self.data_dir + "/coordinator" + str(i) + "   > logcn" + str(i) + " 2>&1 &"
			print coor_cmd
			os.system(coor_cmd)
			time.sleep(1)
		#start data_node
		for i in range(1, self.data_node_num+1):
			datanode_cmd = g_valgrind + bin_path + "bin/gaussdb --datanode  -D " + self.data_dir + "/data_node" + str(i) + "   > logdn" + str(i) + " 2>&1 &"
			print datanode_cmd
			os.system(datanode_cmd)
		time.sleep(1)

	def __stop_server(self):

		#stop coor
		for i in range(1, self.coordinator_num+1):
			coor_cmd = bin_path + "bin/gs_ctl stop -D " + self.data_dir + "/coordinator" + str(i) + " -Z coordinator"
			print coor_cmd
			os.system(coor_cmd)
		#stop data node
		for i in range(1, self.data_node_num+1):
			datanode_cmd = bin_path + "bin/gs_ctl stop -D "+ self.data_dir + "/data_node" + str(i) + " -Z datanode"
			print datanode_cmd
			os.system(datanode_cmd)
		#stop gtm
		gtm_cmd = bin_path + "bin/gtm_ctl stop -Z gtm -D " + self.data_dir + "/gtm_data"
		print gtm_cmd
		os.system(gtm_cmd)
	
	def run(self, run_type):
		if(run_type == 0):
			self.init_env()
			self.__save_nodes_info()
			self.__start_server()
			self.__create_node()
			print "start ok"
		elif(run_type == 1):
			self.__read_nodes_info()
			self.__start_server()
			print "start ok"
		elif(run_type == 2):
			self.__read_nodes_info()
			self.__stop_server()
			print "stop ok"

def usage():
	print "------------------------------------------------------"
	print "python pgxc.py\n	-c coor_num -d datanode_num\n	-s means start\n	-o means stop"
	print "	-g means memcheck"
	print "	-D data directory\n"
	print "------------------------------------------------------"

def main():
	try:
		opts, args = getopt.getopt(sys.argv[1:], "hD:c:d:sovg", ["help", "data_dir="])
	except getopt.GetoptError, err:
		print str(err) 
		sys.exit(2)

	coordinator_num = 0
	datanode_num = 0
	global g_valgrind;
	global g_file_node_info;

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
		else:
			print "unhandled option"
			sys.exit()

	if((coordinator_num == 0 or datanode_num == 0) and run_type == 0):
		usage()
		sys.exit()

	g_file_node_info = data_dir + "/" + g_file_node_info;
	ptdb = Pterodb(coordinator_num, datanode_num, data_dir)
	ptdb.run(run_type)


if __name__ == "__main__":
	main()
