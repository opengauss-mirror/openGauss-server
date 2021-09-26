# coding=utf-8
# !/usr/bin/python

from jenkinsapi.jenkins import Jenkins
import xlrd
import re
import sys
import time


def slaves_list(filename):
    # IP地址合法匹配
    pattern = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')

    try:
        wb = xlrd.open_workbook(filename)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)
    sheet = wb.sheets()[0]
    slaves = []

    for row in range(1, sheet.nrows):  # 跳过标题
        ipaddress = sheet.cell_value(row, 0).strip()
        label = sheet.cell_value(row, 1).strip()
        if pattern.match(ipaddress):
            slaves.append((ipaddress, label))
        else:
            print(" %s id invaild" % ipaddress)

    return slaves


class Jenkinsops():

    def __init__(self, jenkins_url, jenkins_username, jenkins_password):
        self.jenkins_url = jenkins_url
        self.jenkins_username = jenkins_username
        self.jenkins_password = jenkins_password
        self.__jenkins_server = Jenkins(self.jenkins_url, username=self.jenkins_username,
                                        password=self.jenkins_password)

    def get_jenkins_version(self):
        """
        返回Jenkins版本号
        """
        self.__jenkins_server.get
        return self.__jenkins_server.version

    def get_jenkins_slaves(self):
        """
        以字典的格式返回slave和labels
        """
        slaves = {}
        for name, node in self.__jenkins_server.get_nodes().items():
            if node.name == "master":
                continue
            slaves[node.name] = node.get_labels()
        return slaves

    def add_jenkins_slave(self, host, label, credential):
        """
        :param host: ip
        :param label: 标签
        :param credential: 认证
        :return:
        """
        params = {'num_executors': 1,
                  'node_description': label,
                  'remote_fs': '/usr1/gauss_jenkins',
                  'exclusive': True,
                  'labels': label,
                  'host': host,
                  'port': 22,
                  'jvm_options': None,
                  'java_path': None,
                  'prefix_start_slave_cmd': None,
                  'suffix_start_slave_cmd': None,
                  'credential_description': credential,
                  'max_num_retries': 0,
                  'retry_wait_time': 0,
                  'retention': 'Always'
                  }

        return self.__jenkins_server.nodes.create_node(host, params)

    def change_slave_label(self, slave, new_label):
        return self.__jenkins_server.get_node(slave).set_config_element("label", new_label)

    def get_slave(self, slave):

        return self.__jenkins_server.get_node(slave).node_attributes

    def delete_slave(self, slave):
        self.__jenkins_server.delete_node('')

    def get_job(self):
        self.__jenkins_server.get_jenkins_obj()

    def get_credentials(self):
        __credentials = {}
        for credential_id, credential_name in self.__jenkins_server.credentials.credentials.items():
            __credentials[str(credential_name)] = str(credential_id)
        return __credentials

    def get_queue(self):
        return self.__jenkins_server.get_queue()


jenkins_url = 'http://0.0.0.0:8080/jenkins'
username = 'Test'
password = 'TestPwd@123'
filename = "jenkinsslaves.xlsx"
openGauss = Jenkinsops(jenkins_url, username, password)


def print_slaves():
    print("----- slaves list ------")
    for host, label in openGauss.get_jenkins_slaves().items():
        print(host, label)


def add_slaves():
    credential_slave = 'TestPwd@123'
    filename = "jenkinsslaves.xlsx"
    new_slaves = slaves_list(filename)
    for slave in new_slaves:
        # print(slave[0], slave[1])
        res = openGauss.add_jenkins_slave(slave[0], slave[1], credential_slave)
        if res:
            print("添加:%s,标签：%s 成功" % (res, slave[1]))
        else:
            print("添加:%s,标签：%s 失败" % (slave[0], slave[1]))
        time.sleep(0.5)


def change_label():
    slaves = ['0.0.0.0']
    for slave in slaves:
        res = openGauss.change_slave_label(slave, 'backup')
        print(res)


add_slaves()
