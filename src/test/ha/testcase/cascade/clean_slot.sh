# This use case is used to test whether the primary can properly clean up the replication slots corresponding to the cascade standby.

#!/bin/sh
source ./util.sh

standby_mounted_cascade_datadir=""
standby_mounted_cascade_port=""
ports=($dn1_primary_port $standby1_port $standby2_port $standby3_port $standby4_port)
datadirs=($primary_data_dir $standby1_data_dir $standby2_data_dir $standby3_data_dir $standby4_data_dir)

function query_standby_mounted_cascade()
{
  standby_mounted_cascade_datadir=""
  standby_mounted_cascade_port=""
  for (( i=0; i<5; i++ )); do
    local result=`gsql -tA -d $db -p ${ports[i]} -c "select count(*) from pg_get_replication_slots() where slot_name='dn_s4' and active='t';"`
    if [[ X"${result}" = X"1" ]]; then
      standby_mounted_cascade_datadir=${datadirs[i]}
      standby_mounted_cascade_port=${ports[i]}
      break
    fi
  done
}

function check_clean_cascade_slot_result()
{
  local port=$1
  local result=`gsql -tA -d $db -p $port -c "select count(*) from pg_get_replication_slots() where slot_name='dn_s4' and active='f';"`
  if [[ "${result}" = "0" ]]; then
    echo "primary cleaned cascade slot successfully."
  else
    echo "cascade slot still exists in primary! "
    echo "clean cascade slot test failed $failed_keyword with [$result]."
    echo "cascade slot info:"
    gsql -d $db -p $port -c "select * from pg_get_replication_slots() where slot_name='dn_s4' and active='f';"
    exit 1
  fi
}

function test_cascade_standby_clean_slot_func()
{
  set_default

  # The initial state of the cluster is 1 primary and 4 standbies.

  # start standby4 as cascade
  echo "base"
  gs_ctl build -D $standby4_data_dir -M cascade_standby
  sleep 1

  query_primary
  query_multi_standby

  # case 1
  check_clean_cascade_slot_result $dn1_primary_port
  echo "case 1 passed"

  # case 2    https://gitee.com/opengaussorg/dashboard?issue_id=I9JWCD
  query_standby_mounted_cascade
  gs_ctl switchover -D $standby_mounted_cascade_datadir
  sleep 3
  check_clean_cascade_slot_result $standby_mounted_cascade_port
  sleep 1
  gs_ctl switchover -D $primary_data_dir
  sleep 3
  check_clean_cascade_slot_result $dn1_primary_port
  echo "case 2 passed"

  # case 3    https://gitee.com/opengaussorg/dashboard?issue_id=I9VA9H&from=project-issue
  query_primary
  query_multi_standby
  query_standby_mounted_cascade
  gs_ctl stop -D $standby_mounted_cascade_datadir
  sleep 1
  query_standby4
  gs_ctl start -D $standby_mounted_cascade_datadir -M standby
  sleep 3
  local result=`gsql -tA -d $db -p $standby_mounted_cascade_port -c "select count(*) from pg_get_replication_slots() where slot_name='dn_s4' and active='f';"`
  if [[ "$result" != "1" ]]; then
    echo "$failed_keyword with [$result]"
    exit 1
  fi
  gs_ctl switchover -D $standby_mounted_cascade_datadir
  sleep 3
  check_clean_cascade_slot_result $standby_mounted_cascade_port
  echo "case 3 passed"
}

function tear_down() {
  set_default
}

test_cascade_standby_clean_slot_func
tear_down