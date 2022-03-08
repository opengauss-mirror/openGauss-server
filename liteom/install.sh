#!/bin/bash
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : install.sh
# Version      : V1.0.0
# Date         : 2021-04-17
# Description  : the script used to install the single cluster on one machine
#########################################
function check_passwd()
{
	passwd=$1
	local -i num=0
	if [ -n "$(echo $passwd | grep -E --color '^(.*[a-z]+).*$')" ]
	then
	  num=$[ ${num} + 1 ]
	fi
	if [ -n "$(echo $passwd | grep -E --color '^(.*[A-Z]).*$')" ]
	then
	  num=$[ ${num} + 1 ]
	fi
	password_sepcical_char=('~' '!' '@' '#' '$' '%' '^' '&' '(' ')' '_' '-' '*'
	 '+' '=' '{' '[' ']' '}' '|' '\' ':' ';' '"' ',' "'" '.' '<' '.' '?' '/')
	for element in ${password_sepcical_char[@]}
	do
	    compare_str="${element}"
        if [[ "${passwd}" =~ "${compare_str}" ]]
        then
          num=$[ ${num} + 1 ]
          break
        fi
    done

	if [ -n "$(echo $passwd | grep -E --color '^(.*[0-9]).*$')" ]
	then
	    num=$[ ${num} + 1 ]
	fi
	if [ ${#passwd} -lt 8 ] || [ $num -lt 3 ] || [ ${#passwd} -gt 32 ]
	then
		return 1
	fi
	return 0
}

declare password=""
read -t 1 password

if [ "${password}" != "" ]
then
    check_passwd "${password}"
    if [ $? -ne 0 ]
    then
        echo -e "\033[31mthe password can contain only 8 to 32 characters
         and at least three types of the following characters: uppercase letters, lowercase letters, digits, and special characters..\033[0m"
        exit 1
    fi
fi

declare user=$(whoami)
if [ X"$user" = X"root" ]; then
	echo "error: can not install gauss with root"
	exit 1
fi

declare root_path=$(cd $(dirname $0);pwd)
if [ -e "${root_path}/dependency" ]
then
    export LD_LIBRARY_PATH="${root_path}"/dependency:$LD_LIBRARY_PATH
fi

declare data_path=""
declare app_path=""
declare env_file=~/.bashrc
declare mode_type="single"
declare -a localips
declare -a localhosts
declare -a remoteips
declare -i port=5432
declare ssl="off"
declare init_parameters=""
declare init_show_parameters=""
declare config_parameters=""
declare password_check=""
declare nodename=""
declare action="start"
declare mode="-Z single_node"
declare -i replconninfo_flag=1
declare -i install_path_full_flag=1
declare -i data_full_flag=1
declare -i start=1
declare -i ulimit_flag=1
declare -i interactive_passwd_flag=1
declare log_path=""
declare guc_file=""
declare log_file="${root_path}/install.log"
declare cert_path=""
declare client_ip=""


if [ -e "${log_file}" ]
then
    cat /dev/null >  "${log_file}"
else
    touch "${log_file}"
fi

if [ $? -ne 0 ]
then
    echo "error: failed to create the log file."
	exit 1
fi

# obtain all IP addresses of the local host.
declare -i ipindex=0
for localip in $(/sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v ::1|awk '{print $2}'| awk -F "addr:" '{print $NF}')
do
    localips[ipindex]=${localip}
    ipindex=$[ ${ipindex} + 1 ]
done
if [ ${#localips[*]} -eq 0 ]
then
    die "the ip address of the machine is not detected."
fi

function usage()
{
    echo "
Usage: $0 [OPTION]
Arguments:
   -D|--data-path              data path
   -R|--app-path               app directory
   -P|--gsinit-parameter       initializing database parameters
   -C|--dn-guc                 database configuration parameters
   -m|--mode                   database installation mode, only three modes are supported: single、primary、and standby, default single
   -l|--log-path               global log configuration path
   -f|--guc-file               global GUC configuration file
   -n|--nodename               node name (the default value is same with mode)
   -h|--help                   show this help, then exit
   --env-sep-file              detach environment variable files
   --start                     indicates whether to start the cluster.
   --ulimit                    indicates whether to set the maximum number of open files
   --cert-path                 SSL certificate path, if this parameter is used, SSL will be on.
   --ssl-client-ip             client ip address, the parameter modification takes effect only when the SSL switch is turned on.
   "
}

function info()
{
    echo "$1" >> "${log_file}"
    echo -e "\033[32minfo:\033[0m$1"
}

function log()
{
    echo "$1" >> "${log_file}"
    echo "$1"
}

function die()
{
    echo "$1" >> "${log_file}"
    echo -e "\033[31m$1\033[0m"
	exit 1
}

function warn()
{
    echo "$1" >> "${log_file}"
    echo -e "\033[33m$1\033[0m"
}

function security_check()
{
    if [[ "$1" == *[\(\)\{\}\[\]\<\>\`\\\*\!\|\;\&\$\~\?]* ]];then
      die "$1 contain illegal characters."
    fi
}

function path_security_check()
{
    if [[ "$1" == *[\(\)\{\}\[\]\<\>\`\\\ \*\!\|\;\&\$\~\?]* ]];then
      die "$1 contain illegal characters."
    fi
}

function hide_password()
{
    tmppassword=""
    stty -echo
    if [ $1 -eq 0 ]
    then
        read -p "please input database password:" password
    else
        read -p "please retry input database password:" password_check
    fi
    stty echo
}

# check if it has read/write/execute permission.
function check_red_permission()
{
    if [ ! -r "$1" ]
	then
	    die "the user does not have the read permission on the directory/file $1."
	fi
}
function check_write_permission()
{
	if [ ! -w "$1" ]
	then
	    die "the user does not have the write permission on the directory/file $1."
	fi
}
function check_execute_permission()
{
	if [ ! -x "$1" ]
	then
	    die "the user does not have the execute permission on the directory/file $1."
	fi
}

# check whether the path exists.
function check_path()
{
    if echo "$1"|grep -Eq "^/{1,}$"; then
      die "path cannot be / "
    fi
    path_security_check "$1"
	if [ -e "$1" ]
	then
		if [ -f "$1" ]
		then
			die "the path $1 already exists and it is a file."
		fi
		if [[ -n $(ls -A "$1") && "$2" = "install" ]]
		then
			install_path_full_flag=0
		elif [[ -n $(ls -A "$1") && "$2" = "data" ]]
		then
			data_full_flag=0
		fi
	else
		mkdir -p "$1"
		chmod 700 "$1"
	fi
	check_red_permission "$1"
	check_write_permission "$1"
	check_execute_permission "$1"
}

# check whether the IPV4 address is valid.
function is_valid_ipv4()
{
    local ip=$1
    local ret=1
    if [[ "$ip" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]
    then
        # use dots (.) to convert the data into an array to facilitate the following judgment.
        ip=(${ip//\./ })
        [[ ${ip[0]} -le 255 && ${ip[1]} -le 255 && ${ip[2]} -le 255 && ${ip[3]} -le 255 ]]
        ret=$?
    fi
    return $ret
}

# check whether the IPV6 address is valid.
function is_valid_ipv6()
{
    parameter_ip=$1
    if [[ "$1" =~ fe80.* ]]
    then
        if [ -z $(echo "$1" | grep %) ]
        then
            die "the local ipv6 address needs to be added with % and the network adapter name(such as fe80*%eth0)."
        fi
        parameter_ip=${parameter_ip%\%*}
    fi
    local ip=${parameter_ip}
    local ret=1
    if [[ "$ip" =~ ([a-f0-9]{1,4}(:[a-f0-9]{1,4}){7}|[a-f0-9]{1,4}(:[a-f0-9]{1,4}){0,7}::[a-f0-9]{0,4}(:[a-f0-9]{1,4}){0,7}) ]]
    then
        ret=0
    fi
    return $ret
}

# check whether the local host port is occupied
# check whether the entered ip address is the same as the ip address of the host
# identify the ip address of the remote host and add it to the configuration file later.
function check_conf_paramters()
{
    replconninfo=$(echo $1 | grep "replconninfo")
    port_flag=$(echo $1 | grep "port")
    if [ "${replconninfo}" != "" ]
    then
        replconninfo_flag=0
        remote_port_flag=$(echo "$1" | grep "remoteport")
        if [ -z "${remote_port_flag}" ]
        then
            die "replconninfo need parameter of 'remoteport'."
        fi
        # prevent repeated port identification and remove remoteport.
        parameter=$(echo "$1" | sed 's/remoteport//g' | sed "s/remoteport//g" | sed "s#'##g" | sed 's#"##g')
        localhost_flag=$(echo "${parameter}" | grep "localhost")
        if [ "${localhost_flag}" = "" ]
        then
            die "replconninfo need parameter of 'localhost.'"
        fi
        remotehost_flag=$(echo "${parameter}" | grep "remotehost")
        if [ "${remotehost_flag}" = "" ]
        then
            die "replconninfo need parameter of 'remotehost'."
        fi
        localport_flag=$(echo "${parameter}" | grep "localport")
        if [ "${localport_flag}" = "" ]
        then
            die "replconninfo need parameter of 'localport'."
        fi

        # init parameters
        parameter=$(echo "$1" | sed "s/remoteport//g" | sed "s#'##g" | sed 's#"##g')
        parameter_array=(${parameter//=/ })
        len=${#parameter_array[*]}
        if [[ "${localhost_flag}" != "" || "${localport_flag}" != "" || "${remote_port_flag}" != "" || "${remotehost_flag}" != "" ]]
        then
            index=0
            while [ ${index} -lt $[ ${len} + 1 ] ]
            do
                key=${parameter_array[${index}]}
                if [ "${key}" = "localhost" ]
                then
                    index=$[ ${index} + 1 ]
                    value=${parameter_array[${index}]}
                    check_value=${value}
                    is_valid_ipv4 "$value"
                    if [ $? -ne 0 ]
                    then
                        is_valid_ipv6 "$value"
                        if [ $? -ne 0 ]
                        then
                            is_valid_ipv6 "$value"
                            die "the local ip address ${value} is not standard."
                        fi
                        if [[ "$value" =~ fe80.* ]]
                        then
                            check_value=${check_value%\%*}
                        fi
                    fi
                    res=$(echo "${localips[@]}" | grep -wq "${check_value}" &&  echo "yes" || echo "no")
                    if [ "${res}" = "no" ]
                    then
                        die "the ip address of the network adapter is inconsistent with that of the host ${value}"
                    else
                        res=$(echo "${localhosts[@]}" | grep -wq "${check_value}" &&  echo "yes" || echo "no")
                        if [ "${res}" = "no" ]
                        then
                            localhostscount=${#localhosts[*]}
                            localhosts[localhostscount]=${value}
                        fi
                    fi
                elif [ "${key}" = "remotehost" ]
                then
                    index=$[ ${index} + 1 ]
                    value=${parameter_array[${index}]}
                    is_valid_ipv4 "$value"
                    if [ $? -ne 0 ]
                    then
                        is_valid_ipv6 "$value"
                        if [ $? -ne 0 ]
                        then
                            die "the remote ip address ${value} is not standard."
                        fi
                    fi
                    res=$(echo "${remoteips[@]}" | grep -wq "${value}" &&  echo "yes" || echo "no")
                    if [ "${res}" = "no" ]
                    then
                        remoteipcount=${#remoteips[*]}
                        remoteips[remoteipcount]=${value}
                    fi
                elif [ "${key}" = "localport" ]
                then
                    index=$[ ${index} + 1 ]
                    value=${parameter_array[${index}]}
                    if [[ ${value} -lt 1024 || ${value} -gt 65535 ]]
                    then
                        die "the port number must be between 1024 and 65535."
                    fi
                    netstat | grep "${value}"
                    if [ $? -eq 0 ]
                    then
                        die "the port ${value} is already in use"
                    fi
                elif [ "${key}" = "remoteport" ]
                then
                    index=$[ ${index} + 1 ]
                    value=${parameter_array[${index}]}
                    if [[ ${value} -lt 1024 || ${value} -gt 65535 ]]
                    then
                        die "the port number must be between 1024 and 65535."
                    fi
                    netstat | grep "${value}"
                    if [ $? -eq 0 ]
                    then
                        die "the port ${value} is already in use"
                    fi
                fi
                index=$[ ${index} + 1 ]
            done
        fi
    elif [ "${port_flag}" != "" ]
    then
        parameter=$1
        parameter_array=(${parameter//=/ })

        if [ ${#parameter_array[*]} -eq 2 ]
        then
            value=${parameter_array[1]}
            if [[ ${value} -lt 1024 || ${value} -gt 65535 ]]
            then
                die "the port number must be between 1024 and 65535."
            fi
            netstat | grep "${value}"
            if [ $? -eq 0 ]
            then
                die "the port ${value} is already in use"
            fi
            port=${value}
        fi
    fi
	config_parameters="${config_parameters} -c \"$1\""
}

# check whether the database password is normal and empty.
function check_init_paramters()
{
	parameter=$(echo "$1" | sed "s#'##g" | sed 's#"##g')
	interactive_passwd=$(echo "${parameter}" | grep "\-W")
	if [ "${interactive_passwd}" != "" ]
	then
	    interactive_passwd_flag=0
	fi
	passwd_flag=$(echo "${parameter}" | grep -E "\-w\s+|\-\-pwpasswd=")
	if [ "${passwd_flag}" != "" ]
	then
	    pwpasswd_flag=$(echo "${parameter}" | grep -E "\-\-pwpasswd=")
	    if [ "${pwpasswd_flag}" != "" ]
	    then
	        parameter=$(echo "${parameter}" | sed "s/--pwpasswd=/--pwpasswd /g")
	    fi
		parameter_array=(${parameter// / })
		len=${#parameter_array[*]}
		index=0
		while [ ${index} -lt $[ ${len} + 1 ] ]
		do
			key=${parameter_array[${index}]}
			if [[ "${key}" = "-w" || "${key}" = "--pwpasswd" ]]
			then
				index=$[ ${index} + 1 ]
				password=${parameter_array[${index}]}
				check_passwd $password
				if [ $? -ne 0 ]
				then
					die "the password can contain only 8 to 32 characters
					 and at least three types of the following characters: uppercase letters, lowercase letters, digits, and special characters."
				fi
			fi
			index=$[ ${index} + 1 ]
		done
		init_show_parameters="${init_show_parameters} ""-w ******"
	else
	    init_show_parameters="${init_show_parameters} "$1
	fi
	init_parameters="${init_parameters} "$1
}

function check_os()
{
    if [ ${ulimit_flag} -eq 0 ]
    then
        ulimit -SHn 1000000
        if [ $? -ne 0 ]
        then
            echo "error: failed to set the maximum number of open files, an exception occurs when the ulimit -SHn 1000000 command is executed.."
            exit 1
        fi
        sed -i "/.*ulimit\\s*-SHn/d" ${env_file}
        echo "ulimit -SHn 1000000" >> ${env_file}
    fi
    # check shm
    local shared_buffers=1073741824  # 1G
    local shmmax=$(cat /proc/sys/kernel/shmmax)
    env test ${shared_buffers} -gt ${shmmax} && echo "shared_buffers(1073741824) must be less than shmmax($shmmax), Please check it (vim /proc/sys/kernel/shmmax)." && exit 1
    local shmall=$(cat /proc/sys/kernel/shmall)
    local pagesize=$(getconf PAGESIZE)

    if [ $((${shmall}/1024/1024/1024*${pagesize})) -ne 0 ]; then
      if [ $((${shared_buffers}/1024/1024/1024-${shmall}/1024/1024/1024*${pagesize})) -gt 0 ]; then
        die "the usage of the device [Shared_buffers] space(1073741824) cannot be greater than shmall*PAGESIZE($shmall*$pagesize), Please check it (vim /proc/sys/kernel/shmmall)."
      fi
    fi
    # check sem
    local -a sem
    local -i index=0
    local max_connection=5000
    local conn_floor
    for line in $(cat /proc/sys/kernel/sem)
    do
        sem[index]=${line}
	    index=$[ ${index} + 1 ]
    done
    if [ ${sem[0]} -lt 17 ]
    then
        die "the maximum number of SEMMSL is not correct, please change it (/proc/sys/kernel/sem)), ensure that the value of first sem is greater than 17."
    fi
    let conn_floor=(${max_connection}+150)/16
    if [ ${sem[3]} -lt ${conn_floor} ]
    then
        die "the maximum number of SEMMNI is not correct, please change it (/proc/sys/kernel/sem)), ensure that the value of fourth sem is greater than 320."
    fi

    let conn_floor=${conn_floor}*17
    if [ ${sem[1]} -lt ${conn_floor} ]
    then
        die "the maximum number of SEMMNS is not correct, please change it (/proc/sys/kernel/sem)), ensure that the value of second sem is greater than 5500."
    fi
    info "[check install env and os setting success.]"
}

function check()
{
    if [[ "${mode_type}" != "single" && ""${replconninfo_flag} -ne 0 ]]
    then
        die "the parameter -C|--dn-guc["replconninfo='value'"] can not be empty."
    fi
}

function decompress()
{
    cd $root_path
    # get OS distributed version.
    kernel=""
    if [ -f "/etc/euleros-release" ]
    then
        kernel=$(cat /etc/euleros-release | awk -F ' ' '{print $1}' | tr a-z A-Z)
        if [ "${kernel}" = "EULEROS" ]
        then
            kernel="EULER"
        fi
    elif [ -f "/etc/openEuler-release" ]
    then
        kernel=$(cat /etc/openEuler-release | awk -F ' ' '{print $1}')
    elif [ -f "/etc/centos-release" ]
    then
        kernel=$(cat /etc/centos-release | awk -F ' ' '{print $1}')
    else
        kernel=$(lsb_release -d | awk -F ' ' '{print $2}')
    fi
    log "kernel: ${kernel}"

    # detect platform information.
    platform=32
    bit=$(getconf LONG_BIT)
    if [ "$bit" -eq 64 ]
    then
        platform=64
    fi
    platform_arch=$(uname -p)
    bin_name="openGauss-Lite.*-${kernel}-${platform_arch}"
	bin_res=$(ls -a | grep -E "${bin_name}.bin")
	if [ "${bin_res}" = "" ]
	then
		die "can not find suitable bin file, expected bin file is ${bin_name}.bin"
	fi
	log "bin file: ${bin_res}"

	bin_name=$(echo "$bin_res" | sed 's/.bin//g')
	sha_res=$(ls -a | grep -E "${bin_name}.sha256")
	if [ "${sha_res}" = "" ]
	then
		die "can not find suitable verification file, expected bin file is ${bin_name}.sha256"
	fi
	log "verification file: ${sha_res}"
	sha256sum "${bin_name}.bin" | awk -F" " '{print $1}' > "${bin_name}-check.sha256"
	if [ $? -ne 0 ]
	then
	    die "check integrality of bin file failed"
	fi
	sha_res=$(sha256sum -t ${bin_name}.sha256 | awk -F" " '{print $1}')
	check_res=$(sha256sum -t ${bin_name}-check.sha256 | awk -F" " '{print $1}')
	if [ "${sha_res}" != "${check_res}" ]
	then
	    die "computed checksum did not match"
	fi
	rm -rf "${bin_name}-check.sha256"
	cp ${bin_res} ${app_path} && cp version.cfg ${app_path}
	if [ $? -ne 0 ]
	then
		die "copy binary files *.bin and version.cfg to install path error"
	fi
	cd ${app_path}
	tar -zxf ${bin_name}.bin
	if [ $? -ne 0 ]
	then
		die "decompress binary files (*.bin) error"
	fi
	rm -rf ./*.bin
}

function set_environment()
{
    if [ "$2" = "app" ]
    then
        # set GAUSSHOME
        sed -i "/.*export\\s*GAUSSHOME=/d" ${env_file}
        echo "export GAUSSHOME=${app_path}" >> ${env_file}
        # set PATH and LD_LIBRARY_PATH
        sed -i "/.*export\\s*PATH=/d" ${env_file}
        echo "export PATH=${app_path}/bin:"'$PATH' >> ${env_file}
        log "export PATH=${app_path}/bin:$PATH >> ${env_file}"
        sed -i "/.*export\\s*LD_LIBRARY_PATH=/d" ${env_file}
        echo "export LD_LIBRARY_PATH=${app_path}/lib:"'$LD_LIBRARY_PATH' >> ${env_file}
        log "export LD_LIBRARY_PATH=${app_path}/lib:$LD_LIBRARY_PATH >> ${env_file}"
        info "[set GAUSSHOME environment variables success.]"
    elif [ "$2" = "data" ]
    then
        # set GAUSSDATA
        sed -i "/.*export\\s*GAUSSDATA=/d" ${env_file}
        echo "export GAUSSDATA=${data_path}" >> ${env_file}
        info "[set GAUSSDATA environment variables success.]"
    elif [ "$2" = "log" ]
    then
        # set GAUSSLOG
        sed -i "/.*export\\s*GAUSSLOG=/d" ${env_file}
        echo "export GAUSSLOG=${log_path}" >> ${env_file}
        info "[set GAUSSLOG environment variables success.]"
    fi
}

function init_db()
{
    cd ${app_path}/bin
    if [ ! -e gs_initdb ]
    then
        die "no gauss installation file is found in app-path. check the installation directory."
    fi

    info "cmd : gs_initdb -D ${data_path} --nodename=${nodename}  ${init_show_parameters}"
    ./gs_initdb -D ${data_path} --nodename=${nodename} ${init_parameters} | tee -a ${log_file}
	if [ ${PIPESTATUS[0]} -ne 0 ]
	then
		die "[init primary datanode failed.]"
	else
		log "[init primary datanode success.]"
	fi
}

function config_db()
{
	cd ${app_path}/bin
	if [ ! -e gs_guc ]
    then
        die "no gauss installation file is found in app-path. check the installation directory."
    fi
	cmd="./gs_guc set -D ${data_path} ${config_parameters} -c \"application_name='${nodename}'\" "
	info "cmd : ${cmd}"
	eval ${cmd} | tee -a ${log_file}
	if [ ${PIPESTATUS[0]} -ne 0 ]
	then
		die "config datanode failed, pleae check whether the database is instantiated properly."
	fi
	listen_host_flag=$(echo ${config_parameters} | grep "listen_addresses")
	if [ "${listen_host_flag}" = "" ]
	then
        localhostscount=${#localhosts[*]}
        index=0
	    if [ ${localhostscount} -eq 0 ]
        then
            sed -i "/^#listen_addresses/c\listen_addresses = 'localhost,${localips[0]}'"  ${data_path}/postgresql.conf
            log "sed -i /^#listen_addresses/c\listen_addresses = 'localhost,${localips[0]}'  ${data_path}/postgresql.conf"
        else
            listen_addresses=""
            while [ ${index} -lt ${localhostscount} ]
            do
                listen_addresses="${listen_addresses}"",""${localhosts[${index}]}"
                index=$[ $index + 1 ]
            done
            sed -i "/^#listen_addresses/c\listen_addresses = 'localhost ${listen_addresses}'"  ${data_path}/postgresql.conf
		    log "sed -i /^#listen_addresses/c\listen_addresses = 'localhost ${listen_addresses}'  ${data_path}/postgresql.conf"
        fi

	fi

	remote_read_mode_flag=$(echo ${config_parameters} | grep "remote_read_mode")
	if [ "${remote_read_mode_flag}" = "" ]
	then
	    sed -i "/.*remote_read_mode = non_authentication/d" ${data_path}/postgresql.conf
		echo "remote_read_mode = non_authentication" | tee -a ${data_path}/postgresql.conf
		log "echo remote_read_mode = non_authentication | tee -a ${data_path}/postgresql.conf"
	fi

    mask_length=32
    localhostscount=${#localhosts[*]}
    index=0
    if [ ${localhostscount} -eq 0 ]
    then
        is_valid_ipv6 localips[0]
        if [ $? -eq 0 ]
        then
            mask_length=128
        fi
        sed -i "/.*host\\s*all\\s*all\\s*${localips[0]}\/${mask_length}\\s*trust/d" ${data_path}/pg_hba.conf
        echo "host    all             all             ${localips[0]}/${mask_length}            trust" | tee -a ${data_path}/pg_hba.conf
        log "echo host    all             all             ${localips[0]}/${mask_length}            trust | tee -a ${data_path}/pg_hba.conf"
    else
        while [ ${index} -lt ${localhostscount} ]
        do
            is_valid_ipv6 ${localhosts[${index}]}
            if [ $? -eq 0 ]
            then
                mask_length=128
            else
                mask_length=32
            fi
            sed -i "/.*host\\s*all\\s*${user}\\s*${localhosts[${index}]}\/${mask_length}\\s*trust/d" ${data_path}/pg_hba.conf
            echo "host    all             ${user}             ${localhosts[${index}]}/${mask_length}            trust" | tee -a ${data_path}/pg_hba.conf
            log "echo host    all             ${user}             ${localhosts[${index}]}/${mask_length}            trust | tee -a ${data_path}/pg_hba.conf"
            index=$[ $index + 1 ]
        done
    fi

    if [ "${mode_type}" != "single" ]
    then
        remoteipcount=${#remoteips[*]}
        index=0
        while [ ${index} -lt ${remoteipcount} ]
        do
            is_valid_ipv6 ${remoteips[${index}]}
            if [ $? -eq 0 ]
            then
                mask_length=128
            else
                mask_length=32
            fi
            sed -i "/.*host\\s*all\\s*${user}\\s*${remoteips[${index}]}\/${mask_length}\\s*trust/d" ${data_path}/pg_hba.conf
            echo "host    all             ${user}             ${remoteips[${index}]}/${mask_length}            trust" | tee -a ${data_path}/pg_hba.conf
            log "echo host    all             ${user}             ${remoteips[${index}]}/${mask_length}            trust | tee -a ${data_path}/pg_hba.conf"
            index=$[ $index + 1 ]
        done
    fi
	log "[config datanode success.]"
}

function guc_db()
{
    cd ${app_path}/bin
    if [ ! -e gs_guc ]
    then
        die "no gauss installation file is found in app-path. check the installation directory."
    fi
    guc_config_parameters=""
    for  line  in  $(cat ${guc_file} | sed '/^$/d')
    do
        guc_config_parameters="${guc_config_parameters}"" -c \"${line}\""
    done
    cmd="./gs_guc set -D ${data_path} ${guc_config_parameters}"
	info "cmd : ${cmd}"
	eval ${cmd} | tee -a ${log_file}
	if [ ${PIPESTATUS[0]} -ne 0 ]
	then
		warn "guc config failed, you need to manually configure the GUC."
	else
	    log "guc config success."
	fi
}

function cert_db()
{
    cd ${app_path}/bin
    if [ ! -e gs_guc ]
    then
        die "no gauss installation file (gs_guc) is found in app-path,
        check whether OpenGauss has been properly installed in the installation directory."
    fi
    for filename in server.crt server.key cacert.pem server.key.cipher server.key.rand
    do
        if ! cp ${cert_path}/${filename} ${data_path} ; then
            die "copy ${cert_path}/${filename} to ${data_path} failed"
        fi
    done
    guc_config_parameters=" -c \"ssl=on\" -c \"ssl_ciphers='ALL'\"
     -c \"ssl_cert_file='server.crt'\" -c \"ssl_key_file='server.key'\"
      -c \"ssl_ca_file='cacert.pem'\" "
    cmd="./gs_guc set -D ${data_path} ${guc_config_parameters}"
	info "the command of guc config is : ${cmd}"
	eval ${cmd} | tee -a ${log_file}
	if [ ${PIPESTATUS[0]} -ne 0 ]
	then
		die "guc config failed, execute cmd ${cmd} failed."
	fi

    if [ "${client_ip}" != "" ]
    then
        is_valid_ipv4 "${client_ip}"
        if [ $? -ne 0 ]
        then
            is_valid_ipv6 "${client_ip}"
            if [ $? -ne 0 ]
            then
                die "the client ip address ${client_ip} is invalid."
            fi
        fi
        cmd="./gs_guc set -D ${data_path} \"-h hostssl   all all ${client_ip}/32  cert\" "
        info "the command of setting client ip is : ${cmd}"
        eval ${cmd} | tee -a ${log_file}
        if [ ${PIPESTATUS[0]} -ne 0 ]
        then
            warn "failed to configure the client IP address."
        fi
    fi
	log "the ssl certificate of the database is configured successfully."
}

function start_db()
{
    cd ${app_path}/bin
    if [ ! -e gs_ctl ]
    then
        die "no gauss installation file is found in app-path, check the installation directory."
    fi
    info "cmd: gs_ctl ${action} -D ${data_path} ${mode} | tee -a ${log_file}"
    ./gs_ctl ${action} -D ${data_path} ${mode} | tee -a ${log_file}
	if [ ${PIPESTATUS[0]} -ne 0 ]
	then
		die "start datanode failed, maybe the database not be properly installed. please refer to install.log for more detailed information."
	else
		log "start datanode success."
		sleep 5s
		eval "gs_ctl query -D ${data_path}"
	fi
}

while [ $# -gt 0 ]
do
case "$1" in
    -h|--help)
        usage
        exit 1
        ;;
    -m|--mode)
        if [ "$2" = "" ]
        then
            die "the parameter '-m|--mode' cannot be empty."
        fi
        mode_type=$2
        if [[ "${mode_type}" != "single" && "${mode_type}" != "primary" && "${mode_type}" != "standby" ]]
        then
            die "only three modes are supported: single、primary、and standby, default single"
        fi
        shift 2
        ;;
    -n|--nodename)
        if [ "$2" = "" ]
        then
            die "the parameter '-n|--name' cannot be empty."
        fi
        security_check "$2"
        nodename=$2
        shift 2
        ;;
    -D|--data-path)
        if [ "$2" = "" ]
        then
            die "the parameter '-D|--data-path' cannot be empty."
        fi
        data_path=$2
        if [ ${data_path:0:1} != "/" ]
        then
            data_path="$(pwd)/${data_path}"
        fi
        shift 2
        ;;
    -R|--app-path)
        if [ "$2" = "" ]
        then
            die "the parameter '-R|--app-path' cannot be empty."
        fi
        app_path=$2
        if [ ${app_path:0:1} != "/" ]
        then
            app_path="$(pwd)/${app_path}"
        fi
        shift 2
        ;;
    -P|--gsinit-parameter)
        if [ "$2" = "" ]
        then
            die "the parameter '-P|--gsinit-parameter' cannot be empty."
        fi
		tmp_parameter=$(echo "$2" | sed "s#'##g" | sed 's#"##g')
        tmp_passwd_flag=$(echo "${tmp_parameter}" | grep -E "\-w\s+|\-\-pwpasswd=")
        if [ "${tmp_passwd_flag}" = "" ]
        then
            security_check "$2"
        fi
        check_init_paramters "$2"
        shift 2
        ;;
    -l|--log-path)
        if [ "$2" = "" ]
        then
            die "the parameter '-l|--log-path' cannot be empty."
        fi
        log_path="$2"
        if [ ${log_path:0:1} != "/" ]
        then
            log_path="$(pwd)/${log_path}"
        fi
        shift 2
        ;;
    -f|--guc-file)
        if [ "$2" = "" ]
        then
            die "the parameter '-f|--guc-file' cannot be empty."
        fi
        guc_file="$2"
        if [ ${guc_file:0:1} != "/" ]
        then
            guc_file="$(pwd)/${guc_file}"
        fi
        check_red_permission "${guc_file}"
        shift 2
        ;;
    -C|--dn-guc)
        if [ "$2" = "" ]
        then
            die "the parameter '-C|--dn-guc' cannot be empty."
        fi
        security_check "$2"
        check_conf_paramters "$2"
        shift 2
        ;;
    --env-sep-file)
        if [ "$2" = "" ]
        then
            die "the parameter '--env-sep-file' cannot be empty."
        fi
        env_file=$2
        if [ ${env_file:0:1} != "/" ]
        then
            env_file="$(pwd)/${env_file}"
        fi
        shift 2
        ;;
    --start)
        start=0
        shift 1
        ;;
    --ulimit)
        ulimit_flag=0
        shift 1
        ;;
    --cert-path)
        if [ "$2" = "" ]
        then
            die "the parameter '--cert-path' cannot be empty."
        fi
        cert_path=$2
        if [ ${cert_path:0:1} != "/" ]
        then
            cert_path="$(pwd)/${cert_path}"
        fi
        shift 2
        ;;
    --ssl-client-ip)
        if [ "$2" = "" ]
        then
            die "the parameter '--ssl-client-ip' cannot be empty."
        fi
        client_ip=$2
        shift 2
        ;;
    *)
        log "internal error: option processing error" 1>&2
        log "please input right paramtenter, the following command may help you"
        log "sh install.sh --help or sh install.sh -h"
        exit 1
esac
done

function env_file_set()
{
    if [ "${env_file}"X != ~/.bashrc"X" ]
    then
        if [ ! -e "${env_file}" ]
        then
            env_file_path=$(dirname ${env_file})
            mkdir -p ${env_file_path}
            if [ $? -ne 0 ]
            then
                die "touch ${env_file} failed."
            fi
            touch "${env_file}"
            chmod 700 "${env_file}"
            if [ $? -ne 0 ]
            then
                die "an error occurred when assigning read/write/execute permission to the directory $1."
            fi
        fi
        check_red_permission "${env_file}"
        check_write_permission "${env_file}"
        check_execute_permission "${env_file}"
        sed -i "/.*export\\s*GAUSSENV=/d" "${env_file}"
        echo "export GAUSSENV=${env_file}" >> "${env_file}"
    fi
}
function app()
{
    if [ "${app_path}" = "" ]
    then
        app_path=$(cat ${env_file} | grep -e ".*export\\s*GAUSSHOME="| awk -F "=" '{print $2}')
    fi
    if [ "${app_path}" != "" ]
    then
        check_path "${app_path}" "install"
        set_environment "${app_path}" "app"
        if [ ${install_path_full_flag} -eq 1 ]
        then
            decompress
            info "[begin decompressing binary files success.]"
        else
            info "[binary files already decompressed, pass.]"
        fi
    fi
}

function data()
{
    if [ "${data_path}" = "" ]
    then
        data_path=$(cat ${env_file} | grep -e ".*export\\s*GAUSSDATA="| awk -F "=" '{print $2}')
    fi
    if [ "${data_path}" != "" ]
    then
        check_path "${data_path}" "data"
        set_environment "${data_path}" "data"
    fi
}

function cert()
{
    if [ "${cert_path}" != "" ]
    then
        if [ ! -e "${cert_path}" ]
        then
            die "the certificate path does not exist."
        fi
        check_path "${cert_path}" "cert"
        if [ -z $(ls -A "${cert_path}" | grep server.crt) ]
        then
            die "no matching SSL certificate file(server.crt) is found. as a result,
            SSL may fail to be opened. Check ssl config carefully after the installation."
        else
            check_red_permission "${cert_path}/server.crt"
        fi
        if [ -z $(ls -A "${cert_path}" | grep server.key) ]
        then
            die "no matching SSL certificate file(server.key) is found. as a result,
            SSL may fail to be opened. Check ssl config carefully after the installation."
        fi
        if [ -z $(ls -A "${cert_path}" | grep cacert.pem) ]
        then
            die "no matching SSL certificate file(cacert.pem) is found. as a result,
            SSL may fail to be opened. Check ssl config carefully after the installation."
        fi
    fi
}

function log_file_set()
{
    if [ "${log_path}" != "" ]
    then
        check_path "${log_path}" "log"
        data_contain_log_flag=$(echo ${data_path} | grep "${log_path}")
        if [ "${data_contain_log_flag}" != "" ]
        then
            die "log-path cannot be a subdirectory of data-path."
        fi
        set_environment ${log_path} log
    fi
}

function start()
{
    if [ "${mode_type}" = "primary" ]
    then
        mode="-M primary"
        if [ "${nodename}" = "" ]
        then
            nodename="master"
        fi
    elif [ "${mode_type}" = "standby" ]
    then
        mode="-b full"
        action="build"
        if [ "${nodename}" = "" ]
        then
            nodename="slave"
        fi
    else
        mode="-Z single_node"
        if [ "${nodename}" = "" ]
        then
            nodename="single"
        fi
    fi
    log "mode_type is ${mode_type} and mode is ${mode}, node name is ${nodename}"
    # check whether the app-path and data-path paths are contained.
    cd ${app_path}
    if [ $? -ne 0 ]
    then
        die "cd app-path failed, no such file or directory."
    fi
    app_path=$(pwd)
    cd ${data_path}
    if [ $? -ne 0 ]
    then
        die "cd data-path failed, no such file or directory."
    fi
    data_path=$(pwd)
    app_contain_data_flag=$(echo ${app_path} | grep "${data_path}")
    data_contain_app_flag=$(echo ${data_path} | grep "${app_path}")
    if [ "${app_contain_data_flag}" != "" ]
    then
        die "data-path cannot be a subdirectory of app-path."
    fi
    if [ "${data_contain_app_flag}" != "" ]
    then
        die "app-path cannot be a subdirectory of data-path."
    fi
    if [ ${data_full_flag} -ne 0 ]
    then
        if [ ${interactive_passwd_flag} -eq 1 ]
        then
            if [ "${password}" = "" ]
            then
                index=0
                while [ "${password}" = "" ]
                do
                    if [ ${index} -gt 2 ]
                    then
                        die "maximum number of retries exceeded"
                    fi
                    hide_password 0
                    echo -e "\n"
                    hide_password 1
                    echo -e "\n"
                    if [ "${password}" != "${password_check}" ]
                    then
                        info "the two passwords entered are inconsistent, please retry."
                        index=$[ ${index} + 1 ]
                        password=""
                        continue
                    fi
                    check_passwd "${password}"
                    if [ $? -ne 0 ]
                    then
                        die "install gaussdb failed, the password can contain only 8 to 32 characters
                         and at least three types of the following characters: uppercase letters, lowercase letters, digits, and special characters."
                    fi
                    index=$[ ${index} + 1 ]
                done
                init_parameters="${init_parameters} -w ${password}"
                init_show_parameters="${init_show_parameters} ""-w ******"
            else
                check_passwd "${password}"
                if [ $? -ne 0 ]
                then
                    die "install gaussdb failed, the password can contain only 8 to 32 characters
                     and at least three types of the following characters: uppercase letters, lowercase letters, digits, and special characters."
                fi
                init_parameters="${init_parameters} -w ${password}"
                init_show_parameters="${init_show_parameters} ""-w ******"
            fi
        fi
        init_db
        data_full_flag=0
        info "[install datanode success.]"
    fi

    if [ "${config_parameters}" != "" ]
    then
        config_db
        info "[config datanode success.]"
    fi

    if [ "$guc_file" = "" ]
    then
        if [ -e "${root_path}/opengauss_lite.conf" ]
        then
            guc_file="${root_path}/opengauss_lite.conf"
        fi
    fi

    if [ "${guc_file}" != "" ]
    then
        guc_db
    fi

    if [ "${cert_path}" != "" ]
    then
        cert_db
    fi

    if [ ${start} -eq 0 ]
    then
        start_db
    fi
}

function main()
{
    check_os
    data
    if [ "${app_path}" != "" -a "${data_path}" != "" ]
    then
        if [ ${data_full_flag} -ne 0 ]
        then
            check
        fi
    fi
    env_file_set
    cert
    app
    log_file_set
    source ${env_file}
    if [ "${app_path}" != "" -a "${data_path}" != "" ]
    then
        start
    fi
    info "run cmd 'source ${env_file}' to make the environment variables take effect."
}
main
exit 0