#!/bin/sh

echo "check env var"
if [ ${GAUSSHOME} ] && [ -d ${GAUSSHOME}/bin ];then
    echo "GAUSSHOME: ${GAUSSHOME}"
else
    echo "GAUSSHOME NOT EXIST"
    exit 1;
fi

PGPORT0=2000
PGPORT1=3000
SUPER_PASSWORD=Gauss_234
TPCC_USER=tpcc
TPCC_PASSWORD=Hello@123
dms_url="0:127.0.0.1:1611,1:127.0.0.1:1711"
SS_DATA=${HOME}/ss_data

clear_shm()
{
    ipcs -m | grep $USER | awk '{print $2}' | while read shm; do
        if [ -n ${shm} ]; then
            ipcrm -m ${shm}
        fi
    done
    ipcs -s | grep $USER | awk '{print $2}' | while read shm; do
        if [ -n ${shm} ]; then
            ipcrm -s ${shm}
        fi
    done
}

kill_gaussdb()
{
    ps ux | grep gaussdb | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
    ps ux | grep gsql | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
    ps ux | grep dssserver | grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1
    clear_shm
    sleep 2
}

init_ss_data()
{
    rm -rf ${SS_DATA}
    mkdir -p ${SS_DATA}
}

alter_dms_open()
{
    for node in $@
    do
        echo -e "\nautovacuum=false" >> ${node}/postgresql.conf
        echo -e "\nss_enable_reform = off" >> ${node}/postgresql.conf
        echo "${node}:"
        cat ${node}/postgresql.conf | grep ss_enable_dms
    done
}

init_gaussdb()
{
    inst_id=$1
    dss_home=$2
    echo "${GAUSSHOME}/bin/gs_initdb -D ${SS_DATA}/dn${inst_id} --nodename=single_node -w ${SUPER_PASSWORD} --vgname=\"+data,+log${inst_id}\" --enable-dss --dms_url=\"${dms_url}\" -I ${inst_id} --socketpath=\"UDS:${dss_home}/.dss_unix_d_socket\""

    ${GAUSSHOME}/bin/gs_initdb -D ${SS_DATA}/dn${inst_id} --nodename=single_node -w ${SUPER_PASSWORD} --vgname="+data,+log${inst_id}" --enable-dss --dms_url="${dms_url}" -I ${inst_id} --socketpath="UDS:${dss_home}/.dss_unix_d_socket"
}

start_gaussdb()
{
    data_node=$1
    pg_port=$2
    echo "" >> ${data_node}/postgresql.conf
    echo "port = ${pg_port}" >> ${data_node}/postgresql.conf
    echo "> starting ${data_node}" && ${GAUSSHOME}/bin/gs_ctl start -D ${data_node}
    sleep 3
}

create_tpcc_user()
{
    pg_port=$1
    user_name=$2
    user_password=$3
    ${GAUSSHOME}/bin/gsql -h 127.0.0.1 -d postgres -p ${pg_port} -U${USER} -W${SUPER_PASSWORD} -c "create user ${user_name} with password \"${user_password}\";grant all privileges to ${user_name};"
}

main()
{
    # clean env & init conf
    kill_gaussdb
    init_ss_data

    sh ./conf_start_dss_inst.sh 2 ${SS_DATA} ${SS_DATA}/dss_disk
    init_gaussdb 0 ${SS_DATA}/dss_home0
    init_gaussdb 1 ${SS_DATA}/dss_home1

#    sh ./copy_xlog_to_private_vg.sh ${GAUSSHOME}/bin/dsscmd ${SS_DATA}/dss_home0 2

    alter_dms_open ${SS_DATA}/dn0 ${SS_DATA}/dn1

    export DSS_HOME=${SS_DATA}/dss_home0
    start_gaussdb ${SS_DATA}/dn0 ${PGPORT0}
    export DSS_HOME=${SS_DATA}/dss_home1
    start_gaussdb ${SS_DATA}/dn1 ${PGPORT1}

    create_tpcc_user ${PGPORT0} ${TPCC_USER} ${TPCC_PASSWORD}
}

main $@