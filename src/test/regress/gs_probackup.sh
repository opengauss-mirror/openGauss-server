#!/bin/sh
cur_path=$(cd $(dirname $0); pwd)
bin_path=${GAUSSHOME}/bin/
SS_DATA=${HOME}/ss_data
echo "cur_path=${cur_path}"
echo "bin_path=${bin_path}"

build_datebase()
{
    cd  ${cur_path}/../ss/
    sh build_ss_database.sh
}

gs_probackup_pre()
{
    cd ${bin_path}
    if [ -d ${HOME}/gauss_bck ];then
        rm -rf ${HOME}/gauss_bck
    fi
    ${bin_path}/gs_probackup init -B ${HOME}/gauss_bck
    ${bin_path}/gs_probackup add-instance -B ${HOME}/gauss_bck -D ${SS_DATA}/dn0 --instance backup1 --enable-dss --instance-id 0 --vgname +data --socketpath=UDS:${SS_DATA}/dss_home0/.dss_unix_d_socket
    ${bin_path}/gs_probackup backup -B ${HOME}/gauss_bck/ --instance backup1 -b full -d postgres -p 2000
    ${bin_path}/gs_probackup show -B ${HOME}/gauss_bck/
    ${bin_path}/gs_ctl stop -D ${SS_DATA}/dn0
    ${bin_path}/gs_ctl stop -D ${SS_DATA}/dn1
    ps ux | grep dssserver | grep -v grep | grep "${SS_DATA}"| awk '{print $2}' | xargs kill -9
    rm -rf ${SS_DATA}/dn0/*
    rm -rf ${SS_DATA}/dss_disk/dss_data.dmp
}

gs_probackup_start()
{
    truncate -s 10G ${SS_DATA}/dss_disk/dss_data.dmp
    chmod 777 ${SS_DATA}/dss_disk/dss_data.dmp
    ${bin_path}/dsscmd cv -g data -v ${SS_DATA}/dss_disk/dss_data.dmp -s 2048 -D ${SS_DATA}/dss_home0/
    ${bin_path}/dssserver -D ${SS_DATA}/dss_home0/ &    
    ${bin_path}/gs_probackup restore -B ${HOME}/gauss_bck/ --instance backup1    
    ${bin_path}/gs_ctl start -D ${SS_DATA}/dn0
    pid=`ps ux | grep gaussdb |grep -v grep |awk '{print $2}'`
    if [ ! -n "${pid}" ];then
        echo "gs probackup failed"
        exit 1
    fi    
    ${bin_path}/gs_ctl stop -D ${SS_DATA}/dn0
}

clean_env()
{
    ps ux | grep dssserver | grep -v grep | grep "${SS_DATA}"| awk '{print $2}' | xargs kill -9  >/dev/null 2>&1
    rm -rf ${HOME}/gauss_bck
    echo "gs_probackup ok"
}


build_datebase
gs_probackup_pre
gs_probackup_start
clean_env


