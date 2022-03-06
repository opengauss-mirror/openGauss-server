#!/bin/sh
#some enviroment vars

export g_base_port=8888
export prefix=${GAUSSHOME}
export g_pooler_base_port=`expr $g_base_port \+ 410`
export g_base_standby_port=`expr $g_base_port \+ 400`
export install_path="$prefix"
export GAUSSHOME="$prefix"
export LD_LIBRARY_PATH=$prefix/lib:$prefix/lib/libobs:$LD_LIBRARY_PATH
export PATH="$prefix/bin":$PATH
export g_data_path="$install_path/hadata"

eth0ip=`/sbin/ifconfig eth0|sed -n 2p|awk  '{ print $2 }'`
eth1ip=`/sbin/ifconfig eth1|sed -n 2p|awk  '{ print $2 }'`
ethens=`/sbin/ifconfig ens4f0|sed -n 2p |awk  '{ print $2 }'`
enp2s0f0=`/sbin/ifconfig enp2s0f0|sed -n 2p |awk  '{ print $2 }'`
enp2s0f1=`/sbin/ifconfig enp2s0f1|sed -n 2p |awk  '{ print $2 }'`
enp125s0f0=`/sbin/ifconfig enp125s0f0|sed -n 2p |awk  '{ print $2 }'`

if [ -n "$eth0ip" ]; then
        export eth_local_ip=$eth0ip
elif [ -n "$eth1ip" ];then
        export eth_local_ip=$eth1ip
elif [ -n "$ethens" ];then
        export eth_local_ip=$eth1ip
elif [ -n "$enp2s0f0" ];then
        export eth_local_ip=$enp2s0f0
elif [ -n "$enp2s0f1" ];then
        export eth_local_ip=$enp2s0f1
elif [ -n "$enp125s0f0" ];then
        export eth_local_ip=$enp125s0f0
else
     echo "error eth0 and eth1 not configured,exit"
     exit 1
fi

export g_local_ip="127.0.0.1"

db=postgres
scripts_dir=`pwd`
username=`whoami`
data_dir=$g_data_path
install_dir=$install_path
bin_dir="$install_dir/bin"
rawdata_lines=657894
lessdata_lines=30
rawdata_size=67M
gsctl_wait_time=3600
passwd="Gauss@123"

dn1_primary_port=`expr $g_base_port \+ 3`
dn1_standby_port=`expr $g_base_port \+ 6`
standby2_port=`expr $g_base_port \+ 9`
standby3_port=`expr $g_base_port \+ 12`
standby4_port=`expr $g_base_port \+ 15`
dummystandby_port=`expr $g_base_port \+ 9`
dn_temp_port=`expr $g_base_port \+ 21`

primary_data_dir="$data_dir/datanode1"
standby_data_dir="$data_dir/datanode1_standby"
standby2_data_dir="$data_dir/datanode2_standby"
standby3_data_dir="$data_dir/datanode3_standby"
standby4_data_dir="$data_dir/datanode4_standby"
dummystandby_data_dir="$data_dir/datanode1_dummystandby"

failed_keyword="testcase_failed"
startup_keyword="Normal|repair"
startup_keyword1="starting"
setup_keyword="peer_role"
setup_keyword1="Secondary"
walkeepsegment_keyword="removed"
create_table_keyword="TABLE"
build_keyword="completed"
building_keyword="Building"
buildfailed_keyword="failed"

function query_primary()
{
echo query primary
gs_ctl query -D  $data_dir/datanode1
}

function query_multi_standby()
{
    #node_num = $1
    for((i=1; i<$node_num+1; i++));do
        node_dir=$data_dir/datanode$i_standby
        echo  $node_dir
        echo query standby
        gs_ctl query -D  $node_dir
    done
}
function query_dummy()
{

	echo query dummy
	gs_ctl query -D  $dummystandby_data_dir
}
function query_standby()
{
echo query standby
gs_ctl query -D  $data_dir/datanode1_standby
}

function query_standby2()
{
 echo query standby2
 gs_ctl query -D  $data_dir/datanode2_standby

}

function query_standby3()
{
 echo query standby2
 gs_ctl query -D  $data_dir/datanode3_standby

}

function check_primary_startup()
{
echo checking primary startup
for i in $(seq 1 30)
do
	if [ $(query_primary | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_primary | grep $startup_keyword1 | wc -l) -gt 0 ]; then
		sleep 2
	else
		sleep 5
		return 0
	fi
done
echo "$failed_keyword when check_primary_startup"
exit 1
}

function check_standby_startup()
{
echo checking standby startup
for i in $(seq 1 30)
do
	if [ $(query_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_standby | grep $startup_keyword1 | wc -l) -gt 0 ]; then
		sleep 2
	else
		sleep 5
		return 0
	fi
done
echo "$failed_keyword when check_standby_startup"
exit 1
}

function check_standby2_startup()
{
echo checking standby2 startup
for i in $(seq 1 30)
do
        if [ $(query_standby2 | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_standby2 | grep $startup_keyword1 | wc -l) -gt 0 ]; then
                sleep 2
        else
                sleep 5
                return 0
        fi
done
echo "$failed_keyword when check_standby2_startup"
exit 1
}

function check_multi_standby_startup()
{
    echo checking standby startup
    for i in $(seq 1 30)
    do
        if [ $(query_multi_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_multi_standby | grep $startup_keyword1 | wc -l) -gt 0 ]; then
            sleep 2
        else
            sleep 5
            return 0
        fi
    done
    echo "$failed_keyword when check_multi_standby_startup"
    exit 1
}

function check_primary_setup()
{
echo checking primary setup
for i in $(seq 1 30)
do
	if [ $(query_primary | grep -E $setup_keyword | wc -l) -eq 2 ]; then
		return 0
	else
		sleep 2
	fi
done

echo "$failed_keyword when check_primary_setup"
exit 1
}

function check_primary_setup_for_multi_standby()
{
echo checking primary setup for multi-standby mode
for i in $(seq 1 30)
do
	if [ $(query_primary | grep -E $setup_keyword | wc -l) -eq 4 ]; then
		return 0
	else
		sleep 2
	fi
done

echo "$failed_keyword when check_primary_setup_for_multi_standby"
exit 1
}

function check_replication_setup()
{
echo checking replication setup
for i in $(seq 1 30)
do
	if [ $(query_standby | grep -E $setup_keyword | wc -l) -eq 1 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_replication_setup"
exit 1
}

function check_standby_as_primary_setup()
{
echo checking replication setup
for i in $(seq 1 30)
do
	if [ $(query_standby | grep -E $setup_keyword | wc -l) -eq 2 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_standby_as_primary_setup"
exit 1
}

function check_replication_setup_for_primary()
{
echo checking replication setup
for i in $(seq 1 30)
do
  if [ $(query_primary | grep -E $setup_keyword | wc -l) -eq 1 ]; then
    return 0
  else
    sleep 2
  fi
done
echo "$failed_keyword when check_replication_setup_for_primary"
exit 1
}

function check_dummy_setup()
{
echo checking dummy setup
for i in $(seq 1 30)
do
	if [ $(query_primary | grep -E $setup_keyword1 | wc -l) -eq 1 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_dummy_setup"
exit 1
}

function check_dummy2_setup()
{
echo checking dummy setup
for i in $(seq 1 30)
do
        if [ $(query_standby | grep -E $setup_keyword1 | wc -l) -eq 1 ]; then
                return 0
        else
                sleep 2
        fi
done
echo "$failed_keyword when check_dummy2_setup"
exit 1
}

function check_standby_setup()
{
echo checking standby setup
for i in $(seq 1 30)
do
	if [ $(query_standby | grep -E $setup_keyword1 | wc -l) -eq 1 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_standby_setup"
exit 1
}

function check_standby2_setup()
{
echo checking standby2 setup
for i in $(seq 1 30)
do
	if [ $(query_standby2 | grep -E $setup_keyword1 | wc -l) -eq 1 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_standby2_setup"
exit 1
}

function check_walkeepsegment()
{
echo checking wal keep segment
for i in $(seq 1 30)
do
	if [ $(query_standby | grep -E $walkeepsegment_keyword | wc -l) -eq 1 ]; then
		return 0
	else
		sleep 2
	fi
done
echo "$failed_keyword when check_walkeepsegment"
exit 1
}

function wait_catchup_finish()
{
echo wait catchup finish
while [ $(gsql -d $db -p $dn1_primary_port -c "select * from pg_get_senders_catchup_time;" | grep "Catchup" | wc -l) -gt 0 ]
do
	sleep 1
done
}

function wait_primarycatchup_finish()
{
echo wait catchup finish
while [ $(gsql -d $db -p $dn1_standby_port -c "select * from pg_get_senders_catchup_time;" | grep "Catchup" | wc -l) -gt 0 ]
do
  sleep 1
done
}

function kill_primary()
{
echo "kill primary $primary_data_dir"
ps -ef | grep -w $primary_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 3
}

function kill_standby()
{
echo "kill standby $standby_data_dir"
ps -ef | grep -w $standby_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 3
}

function kill_multi_standy()
{
     node_num = $1;
     for((i=1; i<$node_num+1; i++));do
         standby_dir = $data_dir/datanode$i_standby
         echo "kill standby $standby_dir"
         ps -ef | grep -w $standby_dir | grep -v grep | awk '{print $2}' | xargs kill -9
         sleep 3
     done
}

function kill_dummystandby()
{
echo "kill dummystandby $dummystandby_data_dir"
ps -ef | grep $USER | grep -w $dummystandby_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 3
}

function kill_standby2()
{
  echo "kill standby2 $standby2_data_dir"
  ps -ef | grep -w $standby2_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 3
}

function kill_standby3()
{
  echo "kill standby3 $standby3_data_dir"
  ps -ef | grep -w $standby3_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 3
}

function kill_standby4()
{
  echo "kill standby4 $standby4_data_dir"
  ps -ef | grep -w $standby4_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 3
}

function stop_primary()
{
echo "stop primary $primary_data_dir"
$bin_dir/gs_ctl stop -D $primary_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}

function stop_multi_standby()
{
    node_num = $1;
    for((i=1; i<$node_num+1; i++));do
        standby_dir=$data_dir/datanode$i_standby
        echo "stop standby $standby_dir"
        $bin_dir/gs_ctl stop -D $standby_dir -m fast > ./results/gs_ctl.log 2>&1
        sleep 3
    done

}

function stop_standby()
{
echo "stop standby $standby_data_dir"
$bin_dir/gs_ctl stop -D $standby_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}

function stop_dummystandby()
{
echo "stop standby $dummystandby_data_dir"
$bin_dir/gs_ctl stop -D $dummystandby_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}

function stop_standby2()
{
echo "stop standby $standby2_data_dir"
$bin_dir/gs_ctl stop -D $standby2_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}

function stop_standby3()
{
echo "stop standby $standby3_data_dir"
$bin_dir/gs_ctl stop -D $standby3_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}

function stop_standby4()
{
echo "stop standby $standby4_data_dir"
$bin_dir/gs_ctl stop -D $standby4_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}
function start_primary()
{
echo "start primary $primary_data_dir"
start_primary_as_pending
notify_primary_as_primary
}

function start_multi_standby()
{
   
   for((i=1; i<$node_num+1; i++));do
      standby_dir=$data_dir/datanode$i_standby 
      port=$($g_base_port + expr (4 + ($i-1)*2))
      echo "start standby $standby_dir"
      $bin_dir/gaussdb --single_node -M standby -p $port -D $standby_dir > ./results/gaussdb.log 2>&1 &
      check_standby_startup
   done
}

function check_multi_standby_startup()
{ 
    echo checking standby startup
    while [ $(query_multi_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_multi_standby | grep $startup_keyword1 | wc -l) -gt 0 ]
    do
        sleep 3
    done

    sleep 5
}
function start_standby()
{
echo "start standby $standby_data_dir"
$bin_dir/gaussdb --single_node  -M standby -p $dn1_standby_port -D $standby_data_dir > ./results/gaussdb.log 2>&1 &
check_standby_startup
}

function start_dummystandby()
{
echo "start dummystandby $dummystandby_data_dir"
$bin_dir/gaussdb --single_node  -M standby -R -p $dummystandby_port -D $dummystandby_data_dir > ./results/gaussdb.log 2>&1 &
sleep 10
}

function start_standby2()
{
  echo "start standby2 $standby2_data_dir"
  $bin_dir/gaussdb --single_node -M standby -p $standby2_port -D $standby2_data_dir > ./results/gaussdb.log 2>&1 &
  sleep 10
}

function start_standby3()
{
  echo "start standby3 $standby3_data_dir"
  $bin_dir/gaussdb --single_node -M standby -p $standby3_port -D $standby3_data_dir > ./results/gaussdb.log 2>&1 &
  sleep 10
}

function start_standby4()
{
  echo "start standby4 $standby4_data_dir"
  $bin_dir/gaussdb --single_node -M standby -p $standby4_port -D $standby4_data_dir > ./results/gaussdb.log 2>&1 &
  sleep 10
}

function start_primary_as_primary()
{
echo "start primary $primary_data_dir as primary"
$bin_dir/gaussdb --single_node  -M primary -p $dn1_primary_port -D $primary_data_dir > ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_primary_as_standby()
{
echo "start primary $primary_data_dir as standby"
$bin_dir/gaussdb --single_node  -M standby -p $dn1_primary_port -D $primary_data_dir > ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_standby_as_primary()
{
echo "start standby $standby_data_dir as primary"
$bin_dir/gaussdb --single_node  -M primary -p $dn1_standby_port -D $standby_data_dir > ./results/gaussdb.log 2>&1 &
check_standby_startup
}

function start_standby2_as_primary()
{
echo "start standby $standby2_data_dir as primary"
$bin_dir/gaussdb --single_node  -M primary -p $standby2_port -D $standby2_data_dir > ./results/gaussdb.log 2>&1 &
check_standby2_startup
}

function start_primary_as_pending()
{
echo "start primary $primary_data_dir as pending"
$bin_dir/gaussdb --single_node  -M pending -p $dn1_primary_port -D $primary_data_dir > ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_standby_as_pending()
{
echo "start standby $standby_data_dir as pending"
$bin_dir/gaussdb --single_node  -M pending -p $dn1_standby_port -D $standby_data_dir > ./results/gaussdb.log 2>&1 &
check_standby_startup
}

function notify_primary_as_primary()
{
echo "notify primary as primary"
gs_ctl notify -M primary -D $primary_data_dir
}

function notify_primary_as_standby()
{
echo "notify primary as standby"
gs_ctl notify -M standby -D $primary_data_dir
}

function notify_standby_as_primary()
{
echo "notify standby as primary"
gs_ctl notify -M primary -D $standby_data_dir
}

function notify_standby_as_standby()
{
echo "notify standby as standby"
gs_ctl notify -M standby -D $standby_data_dir
}

# for primary-standby-dummystandby
function check_instance(){
sleep 2
date
#can grep datanode1 datanode1_standby datanode1_dummystandby gtm
ps -ef | grep $data_dir | grep -v grep

echo query datanode1
check_primary_setup

echo query datanode1_standby
check_replication_setup
}

# for multi-standby
function check_instance_multi_standby(){
sleep 2
date
#can grep datanode1 datanode1_standby datanode1_dummystandby gtm
ps -ef | grep $data_dir | grep -v grep

echo query datanode1
check_primary_setup_for_multi_standby

echo query datanode1_standby
check_replication_setup
}

#################################################################################
#cascade standby
#hacheck
#cascade_data_dir=$standby2_data_dir
function query_cascade_standby()
{
 echo query cascade standby
 gs_ctl query -D  $data_dir/datanode2_standby
}
function check_cascade_standby_startup()
{
echo checking cascade standby startup
for i in $(seq 1 30)
do
        if [ $(query_cascade_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_cascade_standby | grep $startup_keyword1 | wc -l) -gt 0 ]; then
                sleep 2
        else
                sleep 5
                return 0
        fi
done
echo "$failed_keyword when check_cascade_standby_startup"
exit 1
}
function check_cascade_primary_setup()
{
echo checking primary setup for cascade
for i in $(seq 1 30)
do
        if [ $(query_primary | grep -E $setup_keyword | wc -l) -eq 1 ]; then
                return 0
        else
                sleep 2
        fi
done
echo "$failed_keyword when check_cascade_primary_setup"
exit 1
}
function check_cascade_replication_setup()
{
echo checking replication setup for cascade
for i in $(seq 1 30)
do
        if [ $(query_standby | grep -E $setup_keyword | wc -l) -eq 2 ]; then
                return 0
        else
                sleep 2
        fi
done
echo "$failed_keyword when check_cascade_replication_setup"
exit 1
}
function kill_cascade_standby()
{
  echo "kill cascade standby $standby2_data_dir"
  ps -ef | grep -w $standby2_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 3
}
function stop_cascade_standby()
{
echo "stop cascade_standby $standby2_data_dir"
$bin_dir/gs_ctl stop -D $standby2_data_dir -m fast > ./results/gs_ctl.log 2>&1
sleep 3
}
function start_cascade_standby()
{
echo "start cascade standby $standby2_data_dir"
$bin_dir/gaussdb --single_node  -M cascade_standby -p $standby2_port -D $standby2_data_dir > ./results/gaussdb.log 2>&1 &
check_cascade_standby_startup
}
function start_primary_as_cascade_standby()
{
echo "start primary $primary_data_dir as cascade standby"
$bin_dir/gaussdb --single_node  -M cascade_standby -p $dn1_primary_port -D $primary_data_dir > ./results/gaussdb.log 2>&1 &
check_primary_startup
}
function check_instance_cascade_standby()
{
sleep 2
date
#can grep datanode1 datanode1_standby datanode1_dummystandby gtm
ps -ef | grep $data_dir | grep -v grep
echo query datanode1
check_cascade_primary_setup
echo query datanode1_standby
check_cascade_replication_setup
}
