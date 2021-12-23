#!/bin/sh
# to make sure that standby will use inc build after down and reconnect
source ./util.sh

function test_1()
{
  kill_cluster
  start_cluster

  echo "start cluter success!"
  auto_build_failed_pattern="inc build failed"
  auto_build_pattern="server started"
  kill_standby3
  echo "standy killed"
  build_result=`gs_ctl build -Z single_node -D ${standby3_data_dir}`
  echo $build_result
  sudo iptables -I INPUT -p tcp --dport $dn1_primary_port -s $g_local_ip -j DROP
  sleep 1
  sudo iptables -D INPUT -p tcp --dport $dn1_primary_port -s $g_local_ip -j DROP 
  if [[ $build_result =~ $auto_build_failed_pattern ]] && [[ $build_result =~ $auto_build_pattern ]]
  then
    echo "inc build success"
  else
    echo "inc build $failed_keyword"
  fi
}

function tear_down() {
  sleep 5
}

test_1
tear_down

