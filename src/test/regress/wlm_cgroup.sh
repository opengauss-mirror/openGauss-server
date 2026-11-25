#!/usr/bin/expect
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.


set timeout 3

spawn su -c "
cgclear /dev/cgroup/cpu;
cgclear /dev/cgroup/blkio;
cgclear /dev/cgroup/cpuset;
cgclear /dev/cgroup/cpuacct;
cgclear /dev/cgroups;
rm -rf /dev/cgroup;
mkdir -p /dev/cgroup;
rm -rf /home/CodeTestCov/etc/*.cfg;
export LD_LIBRARY_PATH=/home/CodeTestCov/hutaf_llt/linux_avatar_64;
/home/CodeTestCov/code/PrivateBuildConfiguration/code/current/Gauss200_OLAP_V100R007C10/Code/src/bin/gs_cgroup/gs_cgroup -U CodeTestCov -H /home/CodeTestCov/ -c;
ls -l /home/CodeTestCov/etc/;"
exec sleep 1
send "Gauss_234\r"
#expect "#"
interact
