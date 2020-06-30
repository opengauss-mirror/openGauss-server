#!/bin/bash
#Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
#openGauss is licensed under Mulan PSL v2.
#You can use this software according to the terms and conditions of the Mulan PSL v2.
#You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#-------------------------------------------------------------------------
#
# getOSInfo.sh
#
# IDENTIFICATION
#    src/manager/om/script/gspylib/inspection/lib/checkcollector/getOSInfo.sh
#
#-------------------------------------------------------------------------

file_pwd=$(cd $(dirname $0);pwd)
log_dir=$file_pwd/out
PORT=25308

#Base check
if [ $(whoami) == root ]; then
    echo "current user is root"
else
    echo "Please run the scripts as root user"
    exit 0
fi

if [ ! -f $file_pwd/hostfile ];then
    echo "Make sure the file \"$file_pwd/hostfile\" exists !!!"
    exit 0
fi

mkdir -p $log_dir

#check command
#get OS info
function os_info()
{
  echo $host
  #firewall info
  echo "Get firewall info" | tee $log_dir/${host}_osinfo.log
  if [ $(cat /etc/*release| grep SUSE|wc -l) -gt 0 ];then
    echo "OS is SUSE" | tee -a $log_dir/${host}_osinfo.log
    echo "filewall info" | tee -a $log_dir/${host}_osinfo.log
    ssh $host "/sbin/SuSEfirewall2 status" >> $log_dir/${host}_osinfo.log 2>&1
  elif [ $(cat /etc/*release| grep -E 'REDHAT|Red Hat'|wc -l) -gt 0 ];then
    echo "OS is REDHAT" | tee -a $log_dir/${host}_osinfo.log
    echo "filewall info" | tee -a $log_dir/${host}_osinfo.log
    ssh $host "service iptables status" >> $log_dir/${host}_osinfo.log 2>&1
  else
    echo "ERROR: NOT SUPPORT OS !!!"
  fi
}

for host in $(cat $file_pwd/hostfile)
do
  os_info
done
