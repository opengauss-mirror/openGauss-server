#!/bin/sh
# full build

CURPATH=$(dirname $(readlink -f $0))
source ./common_function.sh

function test_1()
{
  echo ${SS_DATA}
  kill_primary_cluster ${SS_DATA}/dn0 ${SS_DATA}/dn1

  assign_dorado_master_parameter 0 1

  export CM_CONFIG_PATH=${CURPATH}/../cm_config.ini
  export DSS_HOME=${SS_DATA}/dss_home0
  start_primary_cluster ${SS_DATA}/dn0 ${SS_DATA}/dn1 ${SS_DATA}
  echo "start primary cluster success!"

  kill_standby_cluster ${SS_DATA_STANDBY}/dn0 ${SS_DATA_STANDBY}/dn1
  assign_dorado_standby_parameter 0 1
  echo "start standby cluster full build, cmd is: gs_ctl build -D ${SS_DATA_STANDBY}/dn0 -b cross_cluster_full -g 0 --vgname=+data --enable-dss --socketpath=\'${SS_DATA_STANDBY}/dss_home0/.dss_unix_d_socket\'"
  export DSS_HOME=${SS_DATA_STANDBY}/dss_home0
  dss_home=${SS_DATA_STANDBY}/dss_home0
  gs_ctl build -D ${SS_DATA_STANDBY}/dn0 -b cross_cluster_full -g 0 --vgname=+data --enable-dss --socketpath="UDS:${dss_home}/.dss_unix_d_socket" -q
  echo "start standby cluster full build success!"

  export CM_CONFIG_PATH=${CURPATH}/../cm_config_standby.ini
  start_standby_cluster ${SS_DATA_STANDBY}/dn0 ${SS_DATA_STANDBY}/dn1 ${SS_DATA_STANDBY}
  echo "start standby cluster success!"
}

function tear_down() {
  stop_primary_cluster
  stop_standby_cluster
  sleep 5
}

test_1
#tear_down