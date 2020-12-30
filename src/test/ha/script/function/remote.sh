#!/bin/sh
# \
exec expect -- "$0" ${1+"$@"}

###################################################
#  用法: 脚本名 -i IP.地址 -p 密码 -t 超时时间 -m 命令模式 -c 具体命令
###################################################
#  功能描述：
#  1:在远端的服务器上执行命令(使用-m ssh-cmd);
#  2:把本地文件拷贝至远端服务器(使用-m scp-out);
#    备注:如果远端没有scp命令,需要使用cat+ssh
#         把本地文件拷贝至远端服务器
#  3:把远端文件拷贝至本地(使用-m scp-in);
#  4:对于建立互信的环境可以不使用-p参数设置密码;
#  5:默认的用户名为root
#  6:默认的端口号为22
#  7:默认的脚本超时时间为120秒
#  8:执行结果以标准输出的形式输出
#  9:密码或用户名错误返回128
#  10:超时返回129
###################################################

#设置默认值
set port 22
set user "root"
set timeout  120
set password ""
set host ""
set mode ""
set command ""
set src ""
set dst ""

set key "$env(g_aes_rand_key)"

###############################################
# 显示帮助信息
###############################################
proc help {} {
    global argv0
    send_user "usage: $argv0\n"
    send_user "    -i <ip>           Host or IP\n"
    send_user "    -P <port>         Port. Default = 22\n"
    send_user "    -u <user>         UserName. Default = root\n"
    send_user "    -p <password>     Password.\n"
    send_user "    -t <timeout>      Timeout. Default = 120\n"
    send_user "    -m <mode>         Mode. include: ssh-cmd, scp-out, scp-in\n"
    send_user "    -c <command>      Ssh Command\n"
    send_user "    -s <src>          Scp Source File\n"
    send_user "    -d <dst>          Scp Destination File\n"
    send_user "    -a <aes-file>     Use aes encrypt passwd\n"
    send_user "    -v                Version\n"
    send_user "    -h                Help\n"
    send_user "Sample:\n"
    send_user "$argv0 -i 0.0.0.0 -p pass -t 5 -m ssh-cmd -c ifconfig\n"
    send_user "$argv0 -i 0.0.0.0 -p pass -m scp-out -s /etc/passwd -d /tmp/passwd\n"
}

###############################################
# 输出错误日志
###############################################
proc errlog {errmsg h code} {
    global host
    send_user "Error: $errmsg on $host (${code}) \n"
    if {[string compare "$h" "yes"] == 0} {
        help
    }
    exit $code
}

#参数个数不能为0
if {[llength $argv] == 0} {
    errlog "argv is null" "yes" "1"
}

#参数解析
while {[llength $argv]>0} {
    set flag [lindex $argv 0]
    switch -- $flag "-i" {
        set host [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-P" {
        set port [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-u" {
        set user [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-p" {
        set password [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-t" {
        set timeout [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-m" {
        set mode [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-c" {
        set command [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-s" {
        set src [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-d" {
        set dst [lindex $argv 1]
        set argv [lrange $argv 2 end]
    } "-a" {
        set password [ exec openssl enc -aes-256-cbc -salt -a -d -k "$key" -in [lindex $argv 1] 2> /dev/null ]
        set argv [lrange $argv 2 end]
    } "-v" {
        send_user "Ver: 1.0\n"
        exit 0
    } "-h" {
        help
        exit 0
    } default {
        set user [lindex $argv 0]
        set argv [lrange $argv 1 end]
        break
    }
}

#主机名或IP为空
if {"$host" == ""} {
    errlog "host is null" "yes" "1"
}

#执行命令
if {[string compare "$mode" "ssh-cmd"] == 0} {
    if {"$command" == ""} {
        errlog "command is null" "yes" "1"
    }
    spawn ssh -oServerAliveInterval=60 -oStrictHostKeyChecking=no -oVerifyHostKeyDNS=yes -oUserKnownHostsFile=/dev/null -t -p $port $user@$host "$command"
} elseif {[string compare "$mode" "scp-out"] == 0} {
    if {"$src" == "" || "$dst" == ""} {
        errlog "src or dst is null" "yes" "1"
    }
    spawn scp -r -oServerAliveInterval=60 -oStrictHostKeyChecking=no -oVerifyHostKeyDNS=yes -oUserKnownHostsFile=/dev/null -P $port $src $user@$host:$dst
} elseif {[string compare "$mode" "scp-in"] == 0} {
    if {"$src" == "" || "$dst" == ""} {
        errlog "src or dst is null" "yes" "1"
    }
    spawn scp -r -oServerAliveInterval=60 -oStrictHostKeyChecking=no -oVerifyHostKeyDNS=yes -oUserKnownHostsFile=/dev/null -P $port $user@$host:$src $dst
} else {
    errlog "mode($mode) invalid" "yes" "1"
}

#命令执行结果
expect {
    -nocase -re "please try again" {
        errlog "Bad Password/UserName, Or Account locked" "no" "128"
    }
    -nocase -re "password" {
        send "$password\r"
        exp_continue
    }
    timeout {
        errlog "Executing timeout" "no" "129"
    }
}

#获取命令执行结果
catch wait result
set ret [lindex $result 3]
if { $ret != 0 } {
    #如有远端没有scp命令的话,scp会失败的,此时需要使用cat+ssh的方法拷贝数据
    #暂不考虑从远端拷贝数据至本地,且远端无scp的场景
    if {$ret == 1 && [string compare "$mode" "scp-out"] == 0} {
        spawn /bin/sh -c "cat $src | ssh -oServerAliveInterval=60 -oStrictHostKeyChecking=no -oVerifyHostKeyDNS=yes -oUserKnownHostsFile=/dev/null -t -p $port $user@$host 'cat > $dst'"
        #命令执行结果
        expect {
            -nocase -re "please try again" {
                errlog "Bad Password/UserName, Or Account locked" "no" "128"
            }
            -nocase -re "password" {
                send "$password\r"
                exp_continue
            }
            timeout {
                errlog "Executing timeout" "no" "129"
            }
        }

        #获取命令执行结果
        catch wait result
        set ret [lindex $result 3]
        if { $ret == 0 } {
            exit 0
        }
    }
    errlog "Execute failed" "no" "$ret"
}
exit $ret

