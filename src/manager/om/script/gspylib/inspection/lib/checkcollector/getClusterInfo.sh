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
# getClusterInfo.sh
#
# IDENTIFICATION
#    src/manager/om/script/gspylib/inspection/lib/checkcollector/getClusterInfo.sh
#
#-------------------------------------------------------------------------

file_pwd=$(cd $(dirname $0);pwd)
log_dir=$file_pwd/out
PORT=''

#Base check
if [ $(whoami) == omm ]; then
    echo "current user is omm"
else
    echo "Please run the scripts as omm user"
    exit 0
fi

if [ ! -f $file_pwd/databaseinfo.sql ] || [ ! -f $file_pwd/hostfile ];then
    echo "Make sure the file \"databaseinfo.sql\" and  \"hostfile\" exists in the current path"
    exit 0
fi

mkdir -p $log_dir

function usage()
{
    echo "***********************************************************************"
    echo "*                         getClusterInfo.sh usage                            *"
    echo "* -p: coordinator port number                                         *"
    echo "* example:  ./getClusterInfo.sh -p 25308                                     *"
    echo "***********************************************************************"
}

function parse_para()
{
    while getopts "p:h" opt
    do
        case $opt in
            p)
                if [ -z $PORT ]; then
                    let PORT=$OPTARG
                else
                    echo "ERROR: duplicate port number"
                    usage
                    exit 1
                fi
                ;;
            h)
                usage
                exit 1
                ;;
            ?)
                echo "ERROR: unkonw argument"
                usage
                exit 1
                ;;
        esac
    done

    if [ -z $PORT ]; then
        echo "ERROR: must designate -p"
        usage
        exit 1
    fi
}



#Get os info
function os_info_for_remode_host()
{
  echo "CPU info" | tee $log_dir/sysinfo_$host
  ssh $host "cat /proc/cpuinfo" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Memory info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "/usr/bin/free -g" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Disk info" | tee -a $log_dir/sysinfo_$host
  ssh $host "/bin/df -lh" >> $log_dir/sysinfo_$host
  ssh $host "/bin/mount" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Network info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "/sbin/ifconfig" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Swap info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "/usr/bin/free | grep Swap" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "OS info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "/usr/bin/lsb_release -a" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Os parameter info" | tee -a $log_dir/sysinfo_$host
  ssh $host "/sbin/sysctl -a" >> $log_dir/sysinfo_$host 2>&1
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Cluster info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile;gs_om -t status --detail" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Directory of cluster info" | tee -a $log_dir/sysinfo_$host
  ssh $host "ps -ef |grep gaussdb|grep /|grep -v grep" >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "MPPDB info"  | tee -a $log_dir/sysinfo_$host
  ssh $host "source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile;gaussdb -V" >> $log_dir/sysinfo_$host
}

#Get os info
function os_info_for_local_host()
{
  echo "CPU info" | tee $log_dir/sysinfo_$host
  cat /proc/cpuinfo >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Memory info"  | tee -a $log_dir/sysinfo_$host
  /usr/bin/free -g >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Disk info" | tee -a $log_dir/sysinfo_$host
  /bin/df -lh >> $log_dir/sysinfo_$host
  /bin/mount >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Network info"  | tee -a $log_dir/sysinfo_$host
  /sbin/ifconfig >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Swap info"  | tee -a $log_dir/sysinfo_$host
  /usr/bin/free | grep Swap >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "OS info"  | tee -a $log_dir/sysinfo_$host
  /usr/bin/lsb_release -a >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Os parameter info" | tee -a $log_dir/sysinfo_$host
  /sbin/sysctl -a >> $log_dir/sysinfo_$host 2>&1
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Cluster info"  | tee -a $log_dir/sysinfo_$host
  source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile;gs_om -t status --detail >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "Directory of cluster info" | tee -a $log_dir/sysinfo_$host
  ps -ef |grep gaussdb|grep /|grep -v grep >> $log_dir/sysinfo_$host
  echo "========================================================================================" >> $log_dir/sysinfo_$host
  echo  | tee -a $log_dir/sysinfo_$host
  echo "MPPDB info"  | tee -a $log_dir/sysinfo_$host
  source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile;gaussdb -V >> $log_dir/sysinfo_$host
}

#Get the database info
function database_info()
{
  echo 
  echo "Database info"
  echo
    source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile
    for db in $(gsql -d postgres -p $PORT -c "select datname||'  DB' from pg_database where datname != 'template1' and datname != 'template0'" | grep  DB | awk '{print $1}')
    do
      echo "database name:$db" | tee $log_dir/db_$db.log
      gsql -d postgres -p $PORT -A -c "select 'database $db size:'||pg_size_pretty(pg_database_size(:db));" -v db="'$db'" | grep -v "column"| grep -v "row)" | tee -a $log_dir/db_$db.log
      gsql -d $db -p $PORT -A -f $file_pwd/databaseinfo.sql | grep -v point | grep -v "row)"| grep -v "rows)"  >> $log_dir/db_$db.log
      echo
    done
}    

#Get the cluster config file
function cluster_config_for_remote_host()
{
  source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile
  tmp_file=/tmp/conf$(date "+%H%M%S")
  echo ''>$tmp_file
  #collect the config file information
  for dirname in $(gs_om -t status --detail | grep "/" | awk '{ print $5}' | sort -u)
  do
    for filename in $(ssh $host "ls  $dirname/*.conf" 2>/dev/null)
    do
      echo $filename >> $tmp_file
    done
  done
  #copy the config file
  for conf_file in $(sort -u $tmp_file)
  do
    path=${conf_file%/*}
    instance=${path##*/}
    scp $host:$conf_file $log_dir/${host}_${instance}_${conf_file##*/}
  done
}

#Get the cluster config file
function cluster_config_for_local_host()
{
  source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile
  tmp_file=/tmp/conf$(date "+%H%M%S")
  echo ''>$tmp_file
  #collect the config file information
  for dirname in $(gs_om -t status --detail | grep "/" | awk '{ print $5}' | sort -u)
  do
    for filename in $(ls  $dirname/*.conf 2>/dev/null)
    do
      echo $filename >> $tmp_file
    done
  done
  #copy the config file
  for conf_file in $(sort -u $tmp_file)
  do
    path=${conf_file%/*}
    instance=${path##*/}
    cp $conf_file $log_dir/${host}_${instance}_${conf_file##*/}
  done
}

#obtain cn port
parse_para $*
#get information of all
for host in $(cat $file_pwd/hostfile)
do
  if [ $host == $(hostname) ];
  then
    os_info_for_local_host
    echo "get cluster config file"
    cluster_config_for_local_host
  else
    os_info_for_remode_host
    echo "get cluster config file"
    cluster_config_for_remote_host
  fi
done
  database_info
