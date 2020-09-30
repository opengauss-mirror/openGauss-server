#!/bin/bash

readonly cur_path=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd && cd - &>/dev/null)

source $cur_path"/common.sh"

function fn_print_help()
{
    echo "Usage: $0 [OPTION]
    -?|--help                         show help information
    -U|--user_name                    cluster user
    -G|--user_grp                     group of the cluster user
    -h|--host_ip                      intranet IP address of the host in the backend storage network
    -p|--port                         database server port
    -D|--install_location             installation directory of the openGauss program
    "
}

function fn_get_param()
{
    fn_prase_input_param $@
    host_name=`hostname -f`
    system_arch=`uname -p`
    system_name=`cat /etc/os-release | grep '^ID=".*' | grep -o -E '(openEuler|centos)'`
    install_tar="/home/$user_name/openGaussTar"     #安装包所在路径(可修改)
    if [ ! $install_location ]
    then
        install_location="/opt/$user_name"          #数据库安装位置(可修改)
    fi
}

function fn_prase_input_param()
{
    while [ $# -gt 0 ]; do
        case $1 in
            -\?|--help )
                fn_print_help
                exit 1
                ;;
            -U|--user_name )
                fn_check_param user_name $2
                user_name=$2
                shift 2
                ;;
            -G|--user_grp )
                fn_check_param user_grp $2
                user_grp=$2
                shift 2
                ;;
            -h|--host_ip )
                fn_check_param host_ip $2
                host_ip=$2
                shift 2
                ;;
            -p|--port )
                fn_check_param port $2
                host_port=$2
                shift 2
                ;;
            -D|--install_location )
                fn_check_param install_location $2
                install_location=$2
                shift 2
                ;;
            * )
                echo "Please input right paramtenter, the following command may help you"
                echo "sh install.sh --help or sh install.sh -?"
                exit 1
        esac
    done
}

function fn_check_param()
{
    if [ "$2"X = X ]
    then
        echo "no given $1, the following command may help you"
        echo "sh install.sh --help or sh install.sh -?"
        exit 1
    fi
}

function fn_get_openGauss_tar()
{
    mkdir -p "$install_tar" 2>/dev/null
    chown -R $user_name:$user_grp "$install_tar"
    if [ "$system_name" == "openEuler" ] && [ "$system_arch" == "aarch64" ]
    then
        system_arch="arm"
    elif [ "$system_name" == "openEuler" ] && [ "$system_arch" == "x86_64" ]
    then
        system_arch="x86"
    elif [ "$system_name" == "centos" ] && [ "$system_arch" == "x86_64" ]
    then
        system_name="CentOS"
        system_arch="x86"
    else
        echo "We only support CentOS+x86, openEuler+arm and openEuler+x86 by now."
        return 1
    fi
    if [ "`find $cur_path/../ -maxdepth 1 -name "openGauss-1.0.1*tar.gz"`" == "" ]
    then
        cd "$install_tar"
        if [ "`find . -maxdepth 1 -name "openGauss-1.0.1*tar.gz"`" == "" ]
        then
            url="https://opengauss.obs.cn-south-1.myhuaweicloud.com/1.0.1/${system_arch}/openGauss-1.0.1-${system_name}-64bit.tar.gz"
            echo "Downloading openGauss tar from official website at ${install_tar}"
            wget $url --timeout=30 --tries=3
            if [ $? -ne 0 ]
            then
                echo "wget error."
                return 1
            else
                echo "wget success."
            fi
        fi
    else
        cp "$cur_path/../openGauss-1.0.1-${system_name}-64bit.tar.gz" "$install_tar"
        if [ $? -ne 0 ]
        then
            echo "copy Installation package error."
            return 1
        else
            echo "copy Installation package success."
        fi
    fi
    return 0
}

function fn_create_file()
{
    mkdir -p $install_location
    chmod -R 755 $install_location
    chown -R $user_name:$user_grp $install_location

    local install_location=${install_location//\//\\\/}

    if [ ! -e $cur_path/template.xml ]
    then
        echo "cannot find template.xml"
        return 1
    fi
    sed 's/@{host_name}/'$host_name'/g' $cur_path/template.xml | sed 's/@{host_ip}/'$host_ip'/g' | sed 's/@{user_name}/'$user_name'/g' | sed 's/@{host_port}/'$host_port'/g' | sed 's/@{install_location}/'$install_location'/g' > $cur_path/single.xml
    cp $cur_path/single.xml /home/$user_name/
    echo "create config file success."
    return 0
}

function fn_post_check()
{
    fn_check_user
    if [ $? -ne 0 ]
    then
        echo "Check user failed."
        return 1
    else
        echo "Check user success."
    fi
    fn_check_input
    if [ $? -ne 0 ]
    then
        echo "Check input failed."
        return 1
    else
        echo "Check input success."
    fi
    fn_check_firewall $host_port
    if [ $? -ne 0 ]
    then
        echo "Check firewall failed."
        return 1
    else
        echo "Check firewall success."
    fi
    fn_selinux
    if [ $? -ne 0 ]
    then
        echo "Set selinux failed."
        return 1
    else
        echo "Set selinux success."
    fi
    fn_swapoff
    if [ $? -ne 0 ]
    then
        echo "Swapoff failed."
        return 1
    else
        echo "Swapoff success."
    fi
    return 0
}

function fn_check_input()
{
    if [ ! "$user_name" -o ! "$user_grp" -o ! "$host_ip" -o ! "$host_port" ]
    then
        echo "Usage: sh install.sh -U user_name -G user_grp -h ip -p port"
        echo "The following command may help you"
        echo "sh install.sh --help or sh install.sh -?"
        return 1
    fi
    if [ "`netstat -anp | grep -w $host_port`" ]
    then 
        echo "port $host_port occupied,please choose another."
        return 1
    fi
    return 0
}

function fn_check_user()
{
    if [ `id -u` -ne 0 ]
    then
        echo "Only a user with the root permission can run this script."
        return 1
    fi
    return 0
}

function fn_install()
{
    fn_tar
    if [ $? -ne 0 ]
    then
        echo "Get openGauss Installation package or tar package failed."
        return 1
    else
        echo "Get openGauss Installation package and tar package success."
    fi
    export LD_LIBRARY_PATH="${install_tar}/script/gspylib/clib:"$LD_LIBRARY_PATH
    python3 "${install_tar}/script/gs_preinstall" -U $user_name -G $user_grp -X '/home/'$user_name'/single.xml' --sep-env-file='/home/'$user_name'/env_single'
    if [ $? -ne 0 ]
    then
        echo "Preinstall failed."
        return 1
    else
        echo "Preinstall success."
    fi
    chmod 755 "/home/$user_name/single.xml"
    chown $user_name:$user_grp "/home/$user_name/single.xml"
    su - $user_name -c "source /home/$user_name/env_single;gs_install -X /home/$user_name/single.xml"
    if [ $? -ne 0 ]
    then
        echo "Install failed."
        return 1
    else
        echo "Install success."
    fi
    return 0
}

function fn_tar()
{
    fn_get_openGauss_tar
    if [ $? -ne 0 ]
    then
        echo "Get openGauss Installation package error."
        return 1
    else
        echo "Get openGauss Installation package success."
    fi
    cd "${install_tar}"
    tar -zxf "openGauss-1.0.1-${system_name}-64bit.tar.gz"
    if [ $? -ne 0 ]
    then
        echo "tar package error."
        return 1
    else
        echo "tar package success."
    fi
    return 0
}

function fn_install_demoDB()
{
    input=$1
    if [ "$input"X = X ]
    then
        read -p "Would you like to create a demo database (yes/no)? " input
    fi
    if [ $input == "yes" ]
    then
        fn_load_demoDB 1>$cur_path/load.log 2>&1
        fn_check_demoDB
    elif [ $input == "no" ]
    then
        return 2
    else
        read -p "Please type 'yes' or 'no': " input
        fn_install_demoDB $input
    fi
    return $?
}

function fn_load_demoDB()
{
    cp $cur_path/{school.sql,finance.sql} /home/$user_name
    chown $user_name:$user_grp /home/$user_name/{school.sql,finance.sql}
    su - $user_name -c "
    source ~/env_single
    gs_guc set -D $install_location/cluster/dn1/ -c \"modify_initial_password = false\"
    gs_om -t stop && gs_om -t start
    sleep 1
    gsql -d postgres -p $host_port -f /home/$user_name/school.sql
    gsql -d postgres -p $host_port -f /home/$user_name/finance.sql
    gs_guc set -D $install_location/cluster/dn1/ -c \"modify_initial_password = true\"
    gs_om -t stop && gs_om -t start"
}

function fn_check_demoDB()
{
    if [ "`cat $cur_path/load.log | grep ROLLBACK`" != "" ]
    then
        return 1
    elif [ "`cat $cur_path/load.log | grep '\[GAUSS-[0-9]*\]'`" != "" ]
    then
        return 1
    elif [ "`cat $cur_path/load.log | grep ERROR`" != "" ]
    then
        return 1
    elif [ "`cat $cur_path/load.log | grep Unknown`" != "" ]
    then
        return 1
    fi
    return 0
}

function main()
{
    fn_get_param $@

    fn_post_check
    if [ $? -ne 0 ]
    then
        echo "Post check failed."
        return 1
    else
        echo "Post check success."
    fi
    fn_create_user $user_name $user_grp
    if [ $? -ne 0 ]
    then
        echo "User test failed."
        return 1
    else
        echo "User test success."
    fi
    fn_create_file
    if [ $? -ne 0 ]
    then
        echo "Create file failed."
        return 1
    else
        echo "Create file success."
    fi
    fn_install
    if [ $? -ne 0 ]
    then
        echo "Installation failed."
        return 1
    else
        echo "Installation success."
    fi
    fn_install_demoDB
    local returnFlag=$?
    if [ $returnFlag -eq 0 ]
    then
        echo "Load demoDB [school,finance] success."
        return 1
    elif [ $returnFlag -eq 1 ]
    then
        echo "Load demoDB failed, you can check load.log for more details"
    fi
    return 0
}

main $@
exit $?

