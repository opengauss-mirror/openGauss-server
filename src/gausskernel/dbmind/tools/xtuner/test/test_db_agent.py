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
# -------------------------------------------------------------------------
#
# test_db_agent.py
#
# IDENTIFICATION
#    src/gausskernel/dbmind/xtuner/test/test_db_agent.py
#
# -------------------------------------------------------------------------


from db_agent import DB_Agent

db_agent = DB_Agent(host='',
                    host_user='',
                    host_user_pwd='',
                    db_user='',
                    db_user_pwd='',
                    db_name='',
                    db_port='')  # padding your information

# start test

if db_agent.pid <= 0:
    raise AssertionError
db_agent.reboot()
print(db_agent.pid)
print(db_agent.data_path)

print(db_agent.get_knob_value('statement_timeout'))

print(db_agent.get_used_mem())
print(db_agent.get_internal_state())
