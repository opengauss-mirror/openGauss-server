#!/bin/sh
# common function

BIN_PATH=${GAUSSHOME}/bin
LIB_PATH=${GAUSSHOME}/lib

PGPORT=(6600 6700)
SS_DATA=${HOME}/ss_hatest
nodedata_cfg="0:127.0.0.1:6611,1:127.0.0.1:6711"
export CM_CONFIG_PATH=${CURPATH}/cm_config.ini


STANDBY_PGPORT=(9600 9700)
SS_DATA_STANDBY=${HOME}/ss_hatest1
standby_nodedata_cfg="0:127.0.0.1:9611,1:127.0.0.1:9711"

master_cluster_dn=("$SS_DATA/dn0" "$SS_DATA/dn1")
standby_cluster_dn=("$SS_DATA_STANDBY/dn0" "$SS_DATA_STANDBY/dn1")

function kill_dn()
{
    data_dir=$1
    echo "kill $data_dir"
    ps -ef | grep -w $data_dir | grep -v grep | awk '{print $2}' | xargs kill -9
    sleep 1
}

function kill_primary_cluster() {
    primary_dn=$1
    standby_dn=$2
    kill_dn $primary_dn
    kill_dn $standby_dn
}

function kill_standby_cluster() {
    main_standby_dn=$1
    standby_dn=$2
    kill_dn $main_standby_dn
    kill_dn $standby_dn
}

function check_dn_startup()
{
    echo"checking $data_dir startup"
    for i in $(seq 1 30)
    do
      if [ $(ps -ef | grep -w $data_dir | grep -v grep | awk '{print $2}' | wc -l) -gt 0 ]; then
        sleep 2
      else
        sleep 5
        return 0
      fi
    done
    echo "failed when check $data_dir startup"
    exit 1
}

function start_dn()
{
    data_dir=$1
    run_mode=$2
    echo "start $data_dir, exec gaussdb -D $data_dir $run_mode"
    nohup $BIN_PATH/gaussdb -D $data_dir $run_mode & 2>&1 &
    sleep 10
}

function start_primary_cluster() {
  primary_dn=$1
  standby_dn=$2
  ss_data=$3
  run_mode="-z cluster_primary"
  for node in $@
  do
    if [ ${node} == ${ss_data} ]; then
        continue
    fi
    run_mode="-z cluster_primary"
  done
  export DSS_HOME=${ss_data}/dss_home0
  start_dn $primary_dn "$run_mode"
  export DSS_HOME=${ss_data}/dss_home1
  start_dn $standby_dn "$run_mode"
}

function start_standby_cluster() {
  main_standby_dn=$1
  standby_dn=$2
  ss_data=$3
  run_mode="-z cluster_standby"
  for node in $@
  do
    if [ ${node} == ${ss_data} ]; then
        continue
    fi
    run_mode="-z cluster_standby"
  done
  start_dn $main_standby_dn "$run_mode"
  start_dn $standby_dn "$run_mode"
}

function assign_dorado_master_parameter()
{
    for id in $@
    do
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "xlog_file_path = '${SS_DATA}/dorado_shared_disk'"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "xlog_lock_file_path = '${SS_DATA}/shared_lock_primary'"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "application_name = 'master_${id}'"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "cross_cluster_replconninfo1='localhost=127.0.0.1 localport=${PGPORT[id]} remotehost=127.0.0.1 remoteport=${STANDBY_PGPORT[0]}'"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "cross_cluster_replconninfo2='localhost=127.0.0.1 localport=${PGPORT[id]} remotehost=127.0.0.1 remoteport=${STANDBY_PGPORT[1]}'"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "ha_module_debug = off"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "ss_log_level = 255"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "ss_log_backup_file_count = 100"
        gs_guc set -Z datanode -D ${SS_DATA}/dn${id} -c "ss_log_max_file_size = 1GB"
    done
}

function assign_dorado_standby_parameter()
{
    for id in $@
    do
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "xlog_file_path = '${SS_DATA}/dorado_shared_disk'"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "xlog_lock_file_path = '${SS_DATA_STANDBY}/shared_lock_standby'"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "application_name = 'standby_${id}'"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "cross_cluster_replconninfo1='localhost=127.0.0.1 localport=${STANDBY_PGPORT[id]} remotehost=127.0.0.1 remoteport=${PGPORT[0]}'"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "cross_cluster_replconninfo2='localhost=127.0.0.1 localport=${STANDBY_PGPORT[id]} remotehost=127.0.0.1 remoteport=${PGPORT[1]}'"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "ha_module_debug = off"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "ss_log_level = 255"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "ss_log_backup_file_count = 100"
        gs_guc set -Z datanode -D ${SS_DATA_STANDBY}/dn${id} -c "ss_log_max_file_size = 1GB"
    done
}