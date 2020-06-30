#!/usr/bin/expect

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
