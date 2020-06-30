"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

import re

from ssh import ExecutorFactory
from exceptions import ExecutionError


def check_validity(name):
    PATH_CHECK_LIST = [" ","|",";","&","$","<",">","`","\\","'","\"","{","}","(",")","[","]","~","*","?","!","\n"]
    if(name.strip() == ""):
        return
    for rac in PATH_CHECK_LIST:
        flag = name.find(rac)
        if flag >= 0:
            raise ExecutionError
        else:
            continue


class Knob(object):
    def __init__(self, name, knob):
        if not isinstance(knob, dict):
            raise AssertionError
        self.name = name
        self.type = knob.get('type')
        self.min = float(knob.get('min'))
        self.max = float(knob.get('max'))
        self.scale = self.max - self.min
        self.default = knob.get('default')
        self.reboot = knob.get('reboot', False)

        if self.type == 'bool':
            self.min = 0
            self.max = 1

    def to_string(self, val):
        rv = val * self.scale + float(self.min) if self.type in ['int', 'float'] else val
        if self.type == 'int':
            rv = str(int(round(rv)))
        elif self.type == 'bool':
            rv = 'on' if rv >= .5 else 'off'
        elif self.type == 'float':
            rv = str(round(rv, 2))
        else:
            raise ValueError('bad type: ' + self.type)

        return rv

    def to_numeric(self, val):
        if self.type in ['float', 'int']:
            rv = (float(val) - float(self.min)) / self.scale
        elif self.type == 'bool':
            rv = 0. if val == 'off' else 1.
        else:
            raise ValueError('bad type: ' + self.type)

        return rv


class DB_Agent(object):
    def __init__(self, host, host_user, host_user_pwd,
                 db_user, db_user_pwd, db_name, db_port, ssh_port=22):
        # set database authorization information:
        self.ssh = ExecutorFactory() \
            .set_host(host) \
            .set_user(host_user) \
            .set_pwd(host_user_pwd) \
            .set_port(ssh_port) \
            .get_executor()

        self.db_user = host_user if not db_user else db_user
        self.db_user_pwd = db_user_pwd
        self.db_name = db_name
        self.db_port = db_port

        # store database instance pid and data_path:
        _, self.data_path = self.exec_statement(
            'SELECT datapath FROM pg_node_env;'
        )

        # initialize knobs, firstly we initialize some variables:
        self.knobs = None
        self.orderly_knob_list = None

        # set connection session timeout
        self.set_knob_value("statement_timeout", 0)

    def set_tuning_knobs(self, knob_dict):
        if len(knob_dict) <= 0 or not isinstance(knob_dict, dict):
            raise AssertionError

        self.knobs = {}
        for k, v in knob_dict.items():
            self.knobs[k] = Knob(k, v)
        self.orderly_knob_list = sorted(self.knobs.keys())
        self._init_knobs()

    @staticmethod
    def generate_chosen_clause(knobs):
        seq = ['(']
        for i, knob in enumerate(knobs):
            check_validity(knob)
            if i > 0:
                seq.append(',')
            seq.append('\'')
            seq.append(knob)
            seq.append('\'')
        seq.append(')')
        return ''.join(seq)

    def _init_knobs(self):
        sql = 'SELECT name, boot_val, min_val, max_val FROM pg_settings WHERE name IN {}'.format(
            self.generate_chosen_clause(self.orderly_knob_list)
        )

        stdout = self.exec_statement(sql)
        stdout = stdout[4:]
        result = [[stdout[4 * i], stdout[4 * i + 1], stdout[4 * i + 2], stdout[4 * i + 3]] for i in
                  range(len(stdout) // 4)]
        for name, boot_val, min_val, max_val in result:
            knob = self.knobs[name]
            knob.min = min_val if not knob.min else knob.min
            knob.max = max_val if not knob.max else knob.max
            knob.default = boot_val if not knob.default else knob.default

    def exec_statement(self, sql):
        command = "gsql -p {db_port} -U {db_user} -d {db_name} -W {db_user_pwd} -c \"{sql}\";".format(
            db_port=self.db_port,
            db_user=self.db_user,
            db_name=self.db_name,
            db_user_pwd=self.db_user_pwd,
            sql=sql
        )
        stdout = self.exec_shell(command)
        result = re.sub(r'[-+]{2,}', r'', stdout)  # remove '----+----'
        result = re.sub(r'\|', r'', result)  # remove '|'
        result = re.sub(r'\(\d*[\s,]*row[s]*?\)', r'', result)  # remove '(1 row)'
        result = re.sub(r'\n', r' ', result)
        result = result.strip()
        result = re.split(r'\s+', result)
        return result

    def check_alive(self, timeout=1):
        cmd = "gsql -p {db_port} -U {db_user} -d {db_name} -W {db_user_pwd} -c \"{sql}\";".format(
            db_port=self.db_port,
            db_user=self.db_user,
            db_name=self.db_name,
            db_user_pwd=self.db_user_pwd,
            sql='select now();'
        )
        stdout, stderr = self.ssh.exec_command_sync(cmd, blocking_fd=1, timeout=timeout)
        return len(stderr) == 0 and len(stdout) > 0

    def exec_shell(self, cmd, timeout=None):
        stdout, stderr = self.ssh.exec_command_sync(cmd, blocking_fd=1, timeout=timeout)
        if len(stderr) > 0:
            raise ExecutionError(stderr)
        return stdout

    def get_knob_normalized_vector(self):
        nv = []
        for name in self.orderly_knob_list:
            val = self.get_knob_value(name)
            nv.append(self.knobs[name].to_numeric(val))
        return nv

    def set_knob_normalized_vector(self, nv):
        reboot = False
        for i, val in enumerate(nv):
            name = self.orderly_knob_list[i]
            knob = self.knobs[name]
            self.set_knob_value(name, knob.to_string(val))
            reboot = True if knob.reboot else reboot

        if reboot:
            self.reboot()

    def get_knob_value(self, name):
        check_validity(name)
        sql = "SELECT setting FROM pg_settings WHERE name = '{}';".format(name)
        _, value = self.exec_statement(sql)
        return value

    def set_knob_value(self, name, value):

        print("change knob: [%s=%s]" % (name, value))
        self.exec_shell("gs_guc reload -c \"%s=%s\" -D %s" %
                        (name, value, self.data_path))

    def reset_state(self):
        self.exec_shell(
            'gsql {database} -p {port} -c "select pg_stat_reset();"'
                .format(database=self.db_name,
                        port=self.db_port)
        )

    def get_used_mem(self):
        # we make total used memory as regular.
        # main mem: max_connections * (work_mem + temp_buffers) + shared_buffers + wal_buffers
        sql = "select " \
              "setting " \
              "from pg_settings " \
              "where name in ('max_connections', 'work_mem', 'temp_buffers', 'shared_buffers', 'wal_buffers') order by name;"
        res = self.exec_statement(sql)
        res.pop(0)
        res = map(int, res)
        max_conn, s_buff, t_buff, w_buff, work_mem = res
        total_mem = max_conn * (work_mem / 64 + t_buff / 128) + s_buff / 64 + w_buff / 4096  # unit: MB
        return total_mem

    def get_internal_state(self):
        # you could define used internal state here.
        # this is a demo, cache_hit_rate, we will use it while tuning shared_buffer.
        cache_hit_rate_sql = "select blks_hit / (blks_read + blks_hit + 0.001) " \
                             "from pg_stat_database " \
                             "where datname = '{}';".format(self.db_name)
        _, cache_hit_rate = self.exec_statement(cache_hit_rate_sql)
        cache_hit_rate = float(cache_hit_rate)

        self.reset_state()  # reset
        return [cache_hit_rate]

    def set_default_knob(self):
        reboot = False

        for knob in self.knobs:
            self.set_knob_value(knob.name, knob.default)
            reboot = True if knob.reboot else reboot

        self.reboot()

    def reboot(self):
        print("*" * 50)
        print("reboot database..")

        self.exec_shell('gs_ctl restart -D {data_path}'.format(data_path=self.data_path))

        if not self.check_alive(1):
            print('database reboot fail, exit..')
            exit(-1)
        else:
            print('database reboot successfully')

        print("*" * 50)
