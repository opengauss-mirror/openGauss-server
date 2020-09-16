#!/bin/bash

readonly cur_path=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd && cd - &>/dev/null)

source $cur_path"/common.sh"

user_name=$1
user_grp=$2
host_ip=$3
host_port=$4
host_name=`hostname -f`
system_name=`cat /etc/os-release | grep '^ID=".*' | grep -o -E '(openEuler|centos)'`
system_arch=`uname -p`
install_tar="/home/$user_name/openGaussTar"     #安装包所在路径(可修改)
install_location="/opt/$user_name"               #数据库安装位置(可修改)

function fn_get_openGauss_tar()
{
    mkdir -p "$install_tar" 2>/dev/null
    chown -R $user_name:$user_grp "$install_tar"
    if [ "$system_name" == "openEuler" ] && [ "$system_arch" == "aarch64" ]
    then
        system_arch="arm"
    elif [ "$system_name" == "centos" ] && [ "$system_arch" == "x86_64" ]
    then
        system_name="CentOS"
        system_arch="x86"
    else
        echo "We only support CentOS+x86 and openEuler+arm by now."
        return 1
    fi
    if [ "`find $cur_path/../ -maxdepth 1 -name "openGauss-1.0.0*tar.gz"`" == "" ]
    then
        cd "$install_tar"
        if [ "`find . -maxdepth 1 -name "openGauss-1.0.0*tar.gz"`" == "" ]
        then
            url="https://opengauss.obs.cn-south-1.myhuaweicloud.com/1.0.0/${system_arch}/openGauss-1.0.0-${system_name}-64bit.tar.gz"
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
        cp "$cur_path/../openGauss-1.0.0-${system_name}-64bit.tar.gz" "$install_tar"
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
    host_ip=$1
    host_name=$2
    user_name=$3
    host_port=$4
    install_location=$5
    
    mkdir -p $install_location
    chmod -R 755 $install_location
    chown -R $user_name:$user_grp $install_location

    install_location=${install_location//\//\\\/}

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
        echo "Usage: sh install.sh user_name user_grp ip port"
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
    python3 "${install_tar}/script/gs_preinstall" -U $1 -G $2 -X '/home/'$1'/single.xml' --sep-env-file='/home/'$1'/env_single'
    if [ $? -ne 0 ]
    then
        echo "Preinstall failed."
        return 1
    else
        echo "Preinstall success."
    fi
    chmod 755 "/home/$1/single.xml"
    chown $1:$2 "/home/$1/single.xml"
    su - $1 -c "source /home/$1/env_single;gs_install -X /home/$1/single.xml"
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
    tar -zxf "openGauss-1.0.0-${system_name}-64bit.tar.gz"
    if [ $? -ne 0 ]
    then
        echo "tar package error."
        return 1
    else
        echo "tar package success."
    fi
    return 0
}

function main()
{
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
    fn_create_file $host_ip $host_name $user_name $host_port $install_location
    if [ $? -ne 0 ]
    then
        echo "Create file failed."
        return 1
    else
        echo "Create file success."
    fi
    fn_install $user_name $user_grp
    if [ $? -ne 0 ]
    then
        echo "Installation failed."
        return 1
    else
        echo "Installation success."
    fi
    return 0
}
main "$@"
exit $?
