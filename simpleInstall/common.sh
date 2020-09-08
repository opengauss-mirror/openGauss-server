if [ "$COMMON_SH" ]; then
    return;
fi

function fn_create_user()
{
    user_name=$1
    user_grp=$2
    groupadd $user_grp 2>/dev/null
    egrep "^$user_name" /etc/passwd >& /dev/null
    if [ $? -ne 0 ]
    then
        useradd -g $user_grp -d /home/$user_name -m -s /bin/bash $user_name 2>/dev/null
        echo "enter password for user " $user_name
        passwd $user_name
        echo "create user success."
    else
        echo "user has already exists."
    fi
    
    return 0
}

function fn_check_firewall()
{
    host_port=$1
    firewall-cmd --permanent --add-port="$host_port/tcp"
    firewall-cmd --reload
    return 0
}

function fn_selinux()
{
    sed -i "s/SELINUX=.*/SELINUX=disabled/g" /etc/selinux/config
    return 0
}

function fn_swapoff()
{
    # 关闭交换内存
    swapoff -a
    return 0
}

COMMON_SH="common.sh"