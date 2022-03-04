#!/bin/sh
#some enviroment vars
export g_base_port=7777

export g_pooler_base_port=`expr $g_base_port \+ 410`
export g_base_standby_port=`expr $g_base_port \+ 400`
export install_path="$prefix"
export GAUSSHOME="$prefix"
export LD_LIBRARY_PATH=$prefix/lib:$LD_LIBRARY_PATH
export PATH="$prefix/bin":$PATH
export g_data_path="$install_path/ipv6_data"

export g_local_ip="::1"

root_dir=$(dirname $(pwd))
export jar_path=$root_dir/regress/jdbc_test/gsjdbc400.jar

db=postgres
scripts_dir=`pwd`
username=`whoami`
data_dir=$g_data_path
install_dir=$install_path
bin_dir="$install_dir/bin"
passwd="Gauss@123"

dn1_primary_port=`expr $g_base_port \+ 3`
dn1_standby_port=`expr $g_base_port \+ 6`
standby2_port=`expr $g_base_port \+ 9`
standby3_port=`expr $g_base_port \+ 12`
standby4_port=`expr $g_base_port \+ 15`
casecade_standby_port=`expr $g_base_port \+ 9`
dn_temp_port=`expr $g_base_port \+ 21`
dn1_normal_port=`expr $g_base_port \+ 3`

primary_data_dir="$data_dir/datanode1"
standby_data_dir="$data_dir/datanode2"
standby2_data_dir="$data_dir/datanode3"
standby3_data_dir="$data_dir/datanode4"
standby4_data_dir="$data_dir/datanode5"
casecade_standby_data_dir="$data_dir/datanode3"
normal_data_dir="$data_dir/datanode1"

failed_keyword="testcase_failed"
startup_keyword="Normal|repair"
startup_keyword1="starting"
setup_keyword="peer_role"
primary_setup_keyword="Standby"
standby_setup_keyword="Primary"
casecade_standby_setup_keyword="Cascade Standby"
walkeepsegment_keyword="removed"
create_table_keyword="TABLE"
build_keyword="completed"
building_keyword="Building"
buildfailed_keyword="failed"

function query_node()
{
node_num=$1
echo query node $node_num
if [[ $node_num -lt 1 ]] ; then
    echo "node_num must great than 0"
    return 0
fi

$bin_dir/gs_ctl query -D  $data_dir/datanode$node_num
}

function query_primary()
{
echo query primary
$bin_dir/gs_ctl query -D  $primary_data_dir
}

function query_normal()
{
echo query normal
$bin_dir/gs_ctl query -D  $primary_data_dir
}

function query_multi_standby()
{
    node_num=$1
    if [[ $node_num -lt 2 ]] ; then
        echo "node_num must great than 1"
        return 0
    fi
    echo query multi standby
    for((i=2; i<$node_num+1; i++));do
        node_dir=$data_dir/datanode$i
        $bin_dir/gs_ctl query -D  $node_dir
    done
}

function query_casecade_standby()
{
    echo query casecade standby
    $bin_dir/gs_ctl query -D  $casecade_standby_data_dir
}

function query_standby()
{
echo query standby
$bin_dir/gs_ctl query -D  $standby_data_dir
}

function query_standby2()
{
 echo query standby2
 $bin_dir/gs_ctl query -D  $standby2_data_dir

}

function query_standby3()
{
 echo query standby2
 $bin_dir/gs_ctl query -D  $standby2_data_dir

}

function check_primary_startup()
{
echo checking primary startup
for i in $(seq 1 30)
do
    if [ $(query_primary | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_primary | grep $startup_keyword1 | wc -l) -gt 0 ]; then
        sleep 2
    else
        return 0
    fi
done
echo "$failed_keyword when check_primary_startup"
return 1
}

function check_normal_startup()
{
echo checking normal startup
for i in $(seq 1 30)
do
    if [ $(query_normal | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_normal | grep $startup_keyword1 | wc -l) -gt 0 ]; then
        sleep 2
    else
        return 0
    fi
done
echo "$failed_keyword when check_normal_startup"
return 1
}

function check_standby_startup()
{
echo checking standby startup
for i in $(seq 1 30)
do
    if [ $(query_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_standby | grep $startup_keyword1 | wc -l) -gt 0 ]; then
        sleep 2
    else
        return 0
    fi
done
echo "$failed_keyword when check_standby_startup"
return 1
}

function check_casecade_standby_startup()
{
echo checking casecade standby startup
for i in $(seq 1 30)
do
    if [ $(query_casecade_standby | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_casecade_standby | grep $startup_keyword1 | wc -l) -gt 0 ]; then
        sleep 2
    else
        return 0
    fi
done
echo "$failed_keyword when check_casecade_standby_startup"
return 1
}

function check_multi_standby_startup()
{
    node_num=$1
    if [[ $node_num -lt 2 ]] ; then
        echo "node_num must great than 1"
        return 0
    fi
    echo checking standby startup
    for i in $(seq 1 30)
    do
        if [ $(query_multi_standby $node_num | grep -E $startup_keyword | wc -l) -eq 0 -o $(query_multi_standby $node_num | grep $startup_keyword1 | wc -l) -gt 0 ]; then
            sleep 2
        else
            return 0
        fi
    done
    echo "$failed_keyword when check_multi_standby_startup"
    return 1
}

function check_primary_setup()
{
echo checking primary setup
for i in $(seq 1 30)
do
    if [ $(query_primary | grep -E $setup_keyword | wc -l) -eq 1 ]; then
        return 0
    else
        sleep 2
    fi
done

echo "$failed_keyword when check_primary_setup"
return 1
}

function check_replication_setup_for_primary()
{
echo checking replication setup
for i in $(seq 1 30)
do
  if [ $(query_primary | grep -E $setup_keyword | wc -l) -ge 1 ]; then
    return 0
  else
    sleep 2
  fi
done
echo "$failed_keyword when check_replication_setup_for_primary"
return 1
}

function check_primary_setup_for_multi_standby()
{
echo checking primary setup for multi-standby mode
for i in $(seq 1 30)
do
    if [ $(query_primary | grep -E $setup_keyword | wc -l) -ge 2 ]; then
        return 0
    else
        sleep 2
    fi
done

echo "$failed_keyword when check_primary_setup_for_multi_standby"
return 1
}

function check_standby_replication_setup()
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
echo "$failed_keyword when check_standby_replication_setup"
return 1
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
return 1
}


function check_casecade_setup()
{
echo checking casecade standby setup
for i in $(seq 1 30)
do
    if [ $(query_casecade_standby | grep -E $setup_keyword | wc -l) -eq 1 ]; then
        return 0
    else
        sleep 2
    fi
done
echo "$failed_keyword when check_casecade_setup"
return 1
}

function check_standby_replication_setup_for_casecade()
{
echo checking replication setup for casecade
for i in $(seq 1 30)
do
  if [ $(query_standby | grep -E $setup_keyword | wc -l) -ge 2 ]; then
    return 0
  else
    sleep 2
  fi
done
echo "$failed_keyword when check_replication_setup_for_casecade"
return 1
}


function check_standby_setup()
{
echo checking standby setup
for i in $(seq 1 30)
do
    if [ $(query_standby | grep -E $standby_setup_keyword | wc -l) -eq 1 ]; then
        return 0
    else
        sleep 2
    fi
done
echo "$failed_keyword when check_standby_setup"
return 1
}

function check_standby2_setup()
{
echo checking standby2 setup
for i in $(seq 1 30)
do
    if [ $(query_standby2 | grep -E $standby_setup_keyword | wc -l) -eq 1 ]; then
        return 0
    else
        sleep 2
    fi
done
echo "$failed_keyword when check_standby2_setup"
return 1
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
return 1
}

function wait_catchup_finish()
{
echo wait catchup finish
while [ $($bin_dir/gsql -d $db -p $dn1_primary_port -c "select * from pg_get_senders_catchup_time;" | grep "Catchup" | wc -l) -gt 0 ]
do
    sleep 1
done
}

function wait_primarycatchup_finish()
{
echo wait catchup finish
while [ $($bin_dir/gsql -d $db -p $dn1_standby_port -c "select * from pg_get_senders_catchup_time;" | grep "Catchup" | wc -l) -gt 0 ]
do
  sleep 1
done
}

function kill_all()
{
     node_num=$1
     if [[ $node_num -lt 1 ]] ; then
        echo "node_num must great than 0"
        return 0
     fi
     echo "kill all"
     for((i=1; i<$node_num+1; i++));do
         node_dir=$data_dir/datanode$i
         ps -ef | grep -w $node_dir | grep -v grep | awk '{print $2}' | xargs kill -9  > /dev/null 2>&1
         sleep 1
     done
}

function kill_normal()
{
echo "kill primary"
ps -ef | grep $USER | grep -w $normal_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 1
}

function kill_primary()
{
echo "kill primary"
ps -ef | grep $USER | grep -w $primary_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 1
}

function kill_standby()
{
echo "kill standby"
ps -ef | grep $USER | grep -w $standby_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 1
}

function kill_multi_standy()
{
     node_num=$1
     if [[ $node_num -lt 2 ]] ; then
        echo "node_num must great than 1"
        return 0
     fi
     echo kill multi standy
     for((i=2; i<$node_num+1; i++));do
         standby_dir=$data_dir/datanode$i
         echo "kill standby $standby_dir"
         ps -ef | grep -w $standby_dir | grep -v grep | awk '{print $2}' | xargs kill -9
         sleep 1
     done
}

function kill_dummystandby()
{
echo "kill dummystandby"
ps -ef | grep $USER | grep -w $dummystandby_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
sleep 1
}

function kill_standby2()
{
  echo "kill standby2"
  ps -ef | grep -w $standby2_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 1
}

function kill_standby3()
{
  echo "kill standby3"
  ps -ef | grep -w $standby3_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 1
}

function kill_standby4()
{
  echo "kill standby4"
  ps -ef | grep -w $standby4_data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
  sleep 1
}

function stop_normal()
{
echo "stop normal"
$bin_dir/gs_ctl stop -D $normal_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}

function stop_primary()
{
echo "stop primary"
$bin_dir/gs_ctl stop -D $primary_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}

function stop_multi_standby()
{
    node_num=$1
    if [[ $node_num -lt 2 ]] ; then
        echo "node_num must great than 1"
        return 0
    fi

    echo "stop all standby"
    for((i=2; i<$node_num+1; i++));do
        standby_dir=$data_dir/datanode$i
        $bin_dir/gs_ctl stop -D $standby_dir -m fast >> ./results/gs_ctl.log 2>&1
        sleep 1
    done

}

function stop_standby()
{
echo "stop standby"
$bin_dir/gs_ctl stop -D $standby_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}


function stop_standby2()
{
echo "stop standby"
$bin_dir/gs_ctl stop -D $standby2_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}

function stop_standby3()
{
echo "stop standby"
$bin_dir/gs_ctl stop -D $standby3_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}

function stop_standby4()
{
echo "stop standby"
$bin_dir/gs_ctl stop -D $standby4_data_dir -m fast >> ./results/gs_ctl.log 2>&1
sleep 1
}

function start_normal()
{
echo "start normal"
$bin_dir/gs_guc set -D ${normal_data_dir} -c "most_available_sync = on" >> ./results/gaussdb.log 2>&1
$bin_dir/gaussdb --single_node  -M normal -p $dn1_normal_port -D $normal_data_dir >> ./results/gaussdb.log 2>&1 &
check_normal_startup
}

function start_primary()
{
echo "start primary"
$bin_dir/gaussdb --single_node  -M primary -p $dn1_primary_port -D $primary_data_dir >> ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_multi_standby()
{
   node_num=$1
   if [[ $node_num -lt 2 ]] ; then
        echo "node_num must great than 1"
        return 0
   fi

   echo "start all standby"
   for((i=2; i<$node_num+1; i++));do
      standby_dir=$data_dir/datanode$i
      echo $standby_dir
      port=$(($g_base_port + i*3))
      echo $port
      $bin_dir/gaussdb --single_node -M standby -p $port -D $standby_dir >> ./results/gaussdb.log 2>&1 &
   done
   check_multi_standby_startup $node_num
}

function start_casecade_standby()
{
echo "start standby"
$bin_dir/gaussdb --single_node  -M cascade_standby -p $casecade_standby_port -D $casecade_standby_data_dir >> ./results/gaussdb.log 2>&1 &
check_casecade_standby_startup
}

function start_standby()
{
echo "start standby"
$bin_dir/gaussdb --single_node  -M standby -p $dn1_standby_port -D $standby_data_dir >> ./results/gaussdb.log 2>&1 &
check_standby_startup
}


function start_standby2()
{
  echo "start standby2"
  $bin_dir/gaussdb --single_node -M standby -p $standby2_port -D $standby2_data_dir >> ./results/gaussdb.log 2>&1 &
  sleep 2
}

function start_standby3()
{
  echo "start standby3"
  $bin_dir/gaussdb --single_node -M standby -p $standby3_port -D $standby3_data_dir >> ./results/gaussdb.log 2>&1 &
  sleep 2
}

function start_standby4()
{
  echo "start standby4"
  $bin_dir/gaussdb --single_node -M standby -p $standby4_port -D $standby4_data_dir >> ./results/gaussdb.log 2>&1 &
  sleep 2
}

function start_primary_as_standby()
{
echo "start primary as standby"
$bin_dir/gaussdb --single_node  -M standby -p $dn1_primary_port -D $primary_data_dir >> ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_standby_as_primary()
{
echo "start standby as primary"
$bin_dir/gaussdb --single_node  -M primary -p $dn1_standby_port -D $standby_data_dir >> ./results/gaussdb.log 2>&1 &
check_standby_startup
}

function start_primary_as_pending()
{
echo "start primary as pending"
$bin_dir/gaussdb --single_node  -M pending -p $dn1_primary_port -D $primary_data_dir >> ./results/gaussdb.log 2>&1 &
check_primary_startup
}

function start_standby_as_pending()
{
echo "start standby as pending"
$bin_dir/gaussdb --single_node  -M pending -p $dn1_standby_port -D $standby_data_dir >> ./results/gaussdb.log 2>&1 &
check_standby_startup
}

function notify_primary_as_primary()
{
echo "notify primary as primary"
$bin_dir/gs_ctl notify -M primary -D $primary_data_dir
}

function notify_primary_as_standby()
{
echo "notify primary as standby"
$bin_dir/gs_ctl notify -M standby -D $primary_data_dir
}

function notify_standby_as_primary()
{
echo "notify standby as primary"
$bin_dir/gs_ctl notify -M primary -D $standby_data_dir
}

function notify_standby_as_standby()
{
echo "notify standby as standby"
$bin_dir/gs_ctl notify -M standby -D $standby_data_dir
}

# for primary-standby
function check_instance_primary_standby(){
echo query datanode1
check_primary_setup

echo query datanode1_standby
check_standby_replication_setup
}

# for multi-standby
function check_instance_multi_standby(){
echo query datanode1
check_primary_setup_for_multi_standby

echo query datanode_standby
check_standby_setup
check_standby2_setup

}

# for casecade-standby
function check_instance_casecade_standby(){
echo query datanode1
check_replication_setup_for_primary

echo query datanode1_standby
check_casecade_setup
check_standby_replication_setup_for_casecade
}

function gsql_test(){
echo gsql ipv6 connect
if [ $($bin_dir/gsql -d $db -p $dn1_primary_port -h ${g_local_ip} -U ipv6_tmp -W ${passwd} -c "create table ipv6_test(i int);" | grep "CREATE TABLE" | wc -l) -gt 0 ]
then
    return 0
else
    echo "$failed_keyword when gsql_test"
fi
}

function create_test_user(){
echo create test user
if [ $($bin_dir/gsql -d $db -p $dn1_primary_port -c "create user ipv6_tmp with login sysadmin password '"$passwd"';" | grep "CREATE ROLE" | wc -l) -gt 0 ]
then
    return 0
else
    echo "$failed_keyword when create_test_user"
fi
}

function gsql_standby_test(){
echo gsql ipv6 standby connect
if [ $($bin_dir/gsql -d $db -p $dn1_standby_port -h ${g_local_ip} -U ipv6_tmp -W ${passwd} -c "\dt;" | grep "ipv6_test" | wc -l) -gt 0 ]
then
    return 0
else
    echo "$failed_keyword when gsql_standby_test"
fi
}

function jdbc_test(){
echo jdbc ipv6 connect
javac -cp $CLASSPATH:$jar_path Ipv6Test.java
if [ $(java  -cp $CLASSPATH:$jar_path Ipv6Test $dn1_primary_port $g_local_ip | grep "Connection succeed!"  | wc -l) -gt 0 ]
then
    return 0
else
    echo "$failed_keyword when jdbc_test"
fi
}

function set_listen_address(){
echo set node listen address
node_num=$1
if [[ $node_num -le 0 ]] ; then
    echo "node_num must great than 0"
    return 0
fi

dir=$data_dir/datanode$node_num
if [ $($bin_dir/gs_guc set -D ${dir} -c "listen_addresses='"$g_local_ip"'" | grep "Success to perform gs_guc!"  | wc -l) -gt 0 ]
then
    return 0
else
    echo "$failed_keyword when set_listen_address"
fi
}

function set_node_conf(){
echo set node listen address
node_num=$1
if [[ $node_num -le 0 ]] ; then
    echo "node_num must great than 0"
    return 0
fi

dir=$data_dir/datanode$node_num
if [ $($bin_dir/gs_guc set -D ${dir} -c "listen_addresses='"$g_local_ip"'" | grep "Success to perform gs_guc!"  | wc -l) -lt 1 ]
then
    echo "$failed_keyword when set_listen_addresses"
fi

if [ $($bin_dir/gs_guc set -D ${dir} -c "enable_thread_pool=on" | grep "Success to perform gs_guc!"  | wc -l) -lt 1 ]
then
    echo "$failed_keyword when set_enable_thread_pool"
fi
}

function setup_hba()
{
    node_num=$1
    if [[ $node_num -le 0 ]] ; then
        echo "node_num must great than 0"
        return 0
    fi

    if [[ "$g_local_ip" = "127.0.0.1" ]]; then
        return 0
    elif [[ "$g_local_ip" = "::1" ]]; then
        return 0
    fi

    result=$(echo $g_local_ip | grep "::")    
    if [[ "$result" != "" ]]
    then
        mask="128"
    else
        mask="32"
    fi

    for((i=1; i<$node_num+1; i++))
    do
        dir=$data_dir/datanode$i
        hba_line="host    all    all    "${g_local_ip}/${mask}"    trust"
        $bin_dir/gs_guc set -Z datanode -D ${dir} -h "${hba_line}" >> ./results/gaussdb.log 2>&1
        # set up hba for test user
        hba_line="host    all    ipv6_tmp    "${g_local_ip}/${mask}"    sha256"
        $bin_dir/gs_guc set -Z datanode -D ${dir} -h "${hba_line}" >> ./results/gaussdb.log 2>&1
    done
}
