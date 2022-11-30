#!/bin/bash
CUR_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
echo "CUR_DIR : $CUR_DIR"

source $CUR_DIR/../ss/ss_database_build_env.sh

deploy_two_inst()
{
   # clean env & init conf
    kill_gaussdb
    clean_database_env

    sh ./../ss/conf_start_dss_inst.sh 2 ${SS_DATA} ${SS_DATA}/dss_disk
    init_gaussdb 0 ${SS_DATA}/dss_home0
    init_gaussdb 1 ${SS_DATA}/dss_home1

    set_gausdb_port ${SS_DATA}/dn0 ${PGPORT0}
    set_gausdb_port ${SS_DATA}/dn1 ${PGPORT1}

    #sh ./../ss/copy_xlog_to_private_vg.sh ${GAUSSHOME}/bin/dsscmd ${SS_DATA}/dss_home0 2

    assign_dms_parameter ${SS_DATA}/dn0 ${SS_DATA}/dn1

    export DSS_HOME=${SS_DATA}/dss_home0
    start_gaussdb ${SS_DATA}/dn0
    export DSS_HOME=${SS_DATA}/dss_home1
    start_gaussdb ${SS_DATA}/dn1
}

main()
{
    deploy_two_inst
}

main $@

