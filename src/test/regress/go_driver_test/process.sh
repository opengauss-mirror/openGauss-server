#!/bin/bash

export initu_p="Gauss@234"
work_dir=`dirname $0`
export work_dir=`realpath $work_dir`
echo "work_dir : " $work_dir

echo "######## download go test code ######"
sh ${work_dir}/download_godriver.sh

##set go test connection info
port=5432
user=gauss
passwd=Gauss_234
database=gauss
hostip=127.0.0.1

echo "######## parsing go test args begin ######" $@
for arg in $@
do 
    val=${arg#*=}
    key=${arg%=*}
    echo "key="$key" val="$val" vallen="${#val}
    if [ x"${#val}" != x"0" ]; then
        case ${key} in
            i) hostip=$val
            ;;
            p) port=$val
            ;;
            w) passwd=$val
            ;;
            d) database=$val
            ;;
            u) user=$val
            ;;
            *) echo "not support this arg: " $arg
            ;;
        esac
   fi
done

echo "host ip = "$hostip
echo "port = "$port
echo "go running user = "$user
echo "go running user password = "$passwd
echo "go running user database = "$database
echo "######## parsing go test args end ######"

path_netstat=`which netstat`
if [ -x $path_netstat ]; then
    port_check=`netstat -t | awk '{print $4}' | grep $port | wc -l`
    if [ ! x"$port_check" == x"0" ]; then
        echo "your input port=$port is being used, please try another port."
        exit 1
    else
        echo "port $port is OK."
    fi
fi

echo "######## modify go config begin ######"
go_config_filtpath=$work_dir/src/github.com/GaussKernel/property.ini
rm -rf ${go_config_filtpath}.bak
cp $go_config_filtpath ${go_config_filtpath}.bak

sed -i "s/^host=.*/host=${hostip}/g" $go_config_filtpath
sed -i "s/^port=.*/port=${port}/g" $go_config_filtpath
sed -i "s/^password=.*/password=${passwd}/g" $go_config_filtpath
sed -i "s/^dbname=.*/dbname=${database}/g" $go_config_filtpath
sed -i "s/^user=.*/user=${user}/g" $go_config_filtpath

# echo "port=$port" >> $go_config_filtpath
# echo "password=$passwd" >> $go_config_filtpath
# echo "dbname=$database" >> $go_config_filtpath
# echo "user=$user" >> $go_config_filtpath
# echo "host=$hostip" >> $go_config_filtpath
echo "######## modify go config end ######"

echo "######## installing gaussdb begin ######"
command_found=`echo $PATH | grep gaussdb | wc -l`
if [ x$command_found == x"0" ]; then
    exp PATH=${PREFIX_HOME}/bin:$portATH
fi

#set install db info
export datapath=$work_dir/data
export initpasswd=Gauss_234
echo "install db path = "$datapath
export ssl_file_dir=$work_dir/src/github.com/GaussKernel/certs

function check_res()
{
    if [ $1 -ne 0 ]; then
        echo "process error. please check"
        exit 1
    fi
}

function stop_db_running()
{
    is_running=`gs_ctl status -D $database | grep "no server running" | wc -l`
    if [ x"$is_running" != x"1" ]; then
        gs_ctl stop -D $datapath > /dev/null 2>&1
    fi
    for pid in `ps -ux | grep $datapath | grep -v grep | awk '{print $2}'`
    do
        kill -9 $pid
    done
}

function prepare_ssl()
{
    cp ${ssl_file_dir}/server.crt $datapath
    cp ${ssl_file_dir}/server.key $datapath
    cp ${ssl_file_dir}/root.crt $datapath

    chmod 0600 $datapath/server.crt
    chmod 0600 $datapath/server.key
    chmod 0600 $datapath/root.crt

    rm -rf ${HOME}/.postgresql
    cp -r $ssl_file_dir ${HOME}
    mv ${HOME}/certs ${HOME}/.postgresql
    export PGSSLKEY=${HOME}/.postgresql/postgresql.key
    export PGSSLCERT=${HOME}/.postgresql/postgresql.crt
	export PGSSLROOTCERT=${HOME}/.postgresql/root.crt
    chmod 0600 $PGSSLKEY
    chmod 0600 $PGSSLCERT
    chmod 0600 $PGSSLROOTCERT
}

function modify_param(){
    gs_guc set -Z datanode -D $datapath -c "ssl = on"
    gs_guc set -Z datanode -D $datapath -c "ssl_cert_file = 'server.crt'"
    gs_guc set -Z datanode -D $datapath -c "ssl_key_file = 'server.key'"
    gs_guc set -Z datanode -D $datapath -c "ssl_ca_file = 'root.crt'"
    gs_guc set -Z datanode -D $datapath -c "logging_collector = 'on'"
    gs_guc set -Z datanode -D $datapath -c "log_connections = 'on'"
    gs_guc set -Z datanode -D $datapath -c "log_disconnections = 'on'"
    gs_guc set -Z datanode -D $datapath -c "log_statement = 'ddl'"
    gs_guc set -Z datanode -D $datapath -c "log_rotation_age = 30d"
    gs_guc set -Z datanode -D $datapath -c "log_rotation_size = 100MB"
    gs_guc set -Z datanode -D $datapath -c "log_duration = 'on'"
    gs_guc set -Z datanode -D $datapath -c "log_lock_waits = 'on'"
    gs_guc set -Z datanode -D $datapath -c "listen_addresses = '*'"
}

function modify_hba()
{
    echo "local   all all trust" >> $datapath/pg_hba.conf
    echo "host   all all 0.0.0.0/0 sha256" >> $datapath/pg_hba.conf
    echo "hostssl   all all 0.0.0.0/0 sha256" >> $datapath/pg_hba.conf
}

stop_db_running
rm -rf $datapath
gs_initdb -D $datapath --nodename=datanode -w $initpasswd -A trust
check_res $?
echo "port=$port" >>$datapath/postgresql.conf
gs_ctl start -D $datapath -Z single_node -l logfile
check_res $?
prepare_ssl
modify_param
modify_hba
gs_ctl restart -D $datapath -Z single_node -l logfile
check_res $?
echo "######## installing gaussdb end ######"

echo "######## create db user begin ######"
echo "
CREATE user ${user} WITH PASSWORD '${passwd}';
CREATE database ${database} OWNER ${user};
GRANT ALL PRIVILEGES ON database ${database} to ${user};
" | gsql -d postgres -p ${port}

echo "
create schema ${user};
create table people(id int,name varchar2(200));
insert into people values(10,20);
" | gsql -d ${database} -p ${port} -U ${user} -W ${passwd} -h ${hostip}
echo "######## create db user end (go run scipt with this user)######"


echo "######## running go test begin ######"

function cal_interval_time
{
    local t=$(awk 'BEGIN{print '$2' - '$1'}')
    local z=${t%.*}
    printf "%.2d:%.2d:%.2d" $((z/60/60)) $((z/60%60)) $((z%60))
}

export GO111MODULE=on
export GONOSUMDB="*"
export GOPATH=$work_dir
export GOPROXY=http://cmc.centralrepo.rnd.huawei.com/go,http://mirrors.tools.huawei.com/goproxy

gotest_base=$work_dir/src/github.com/GaussKernel
gotest_base_out=${gotest_base}/go_test_out
gotest_base_expect=${gotest_base}/go_test_expect
gotest_base_diff=${gotest_base}/go_test_diff

if [ ! -d ${gotest_base_out} ];then
    mkdir -p ${gotest_base_out}
fi
if [ ! -d ${gotest_base_expect} ];then
    mkdir -p ${gotest_base_expect}
fi
if [ ! -d ${gotest_base_diff} ];then
    mkdir -p ${gotest_base_diff}
fi

rm -rf ${gotest_base_out}/*

cd ${gotest_base}/go_test
declare -i go_test_files=0
declare -i failed_counts=0
for file in `ls`
do
    if [ -f ${file} ] && [ x"${file:0-7}" == x"test.go" ]; then
        ((go_test_files++))
        start_time=$(date +%s.%N)
        go test ${file} -c -o ${file}.exec
        if [ $? != 0 ]; then
            echo "compile file ${file} error."
            continue
        fi
        chmod +x ${file}.exec
        ./${file}.exec > ${gotest_base_out}/${file}.out 2>&1
        diff -w -B -u ${gotest_base_expect}/${file}.out ${gotest_base_out}/${file}.out > ${gotest_base_diff}/${file}.diff
        end_time=$(date +%s.%N)
        time_cost="$(cal_interval_time ${start_time} ${end_time})"
        is_fail=`cat ${gotest_base_diff}/${file}.diff | wc -l`
        if [ x"$is_fail" != x"0" ]; then
            ((failed_counts++))
            printf "%-40s : FAIL            %s\r\n" ${file} ${time_cost}
        else
            printf "%-40s : SUCCESS         %s\r\n" ${file} ${time_cost}
        fi
    fi
done
success_counts=`expr $go_test_files - $failed_counts`
echo "Execute go test $go_test_files examples, there are $success_counts success, and $failed_counts failed."
echo "######## running go test end ######"

echo "######## stop db and clean ######"
stop_db_running
echo "######## stop db end ######"

if [ x"$failed_counts" != x"0" ]; then
exit 1
fi