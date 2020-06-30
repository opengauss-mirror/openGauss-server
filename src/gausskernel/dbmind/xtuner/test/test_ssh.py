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
# test_ssh.py
#
# IDENTIFICATION
#    src/gausskernel/dbmind/xtuner/test/test_ssh.py
#
# -------------------------------------------------------------------------


from ssh import ExecutorFactory


def test_remote():
    exe = ExecutorFactory().set_host('').set_user('').set_pwd('').get_executor()  # padding your information
    print(exe.exec_command_sync("cat /proc/cpuinfo | grep \"processor\" | wc -l"))
    print(exe.exec_command_sync("cat /proc/self/cmdline | xargs -0"))
    print(exe.exec_command_sync("echo -e 'hello \\n world'")[0].count('\n'))
    print(exe.exec_command_sync("echo -e 'hello \\n world'")[0])
    print(exe.exec_command_sync('echo $SHELL'))


def test_local():
    exe = ExecutorFactory().get_executor()
    print(exe.exec_command_sync("ping -h"))


if __name__ == "__main__":
    test_remote()
    test_local()
