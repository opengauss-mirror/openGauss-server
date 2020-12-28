#!/bin/bash

# username:omm  usergroup:dbgrp 工具脚本路径：/home/om/ xml文件路径：/home/om/cluster_1m0s_config.xml

workspace=$1
echo -e "\033[32m 注意事项：  \033[0m"
echo -e "\033[32m 	1、软件包路径请设置成不同于xml配置的安装路径以及日志保存路径 \033[0m"
echo -e "\033[32m       2、确保软件包路径除了xml配置不含有其他文件 \033[0m"
echo -e "\033[32m 	3、需要互信的主机手动修改密码为设置密码  \033[0m"
echo -e "\033[32m       4、升级包命名为upgrade.tar.gz，并且解压以后文件夹名称为upgrade  \033[0m"

if [ $# -lt 1 ]
then
	echo -e "\033[31m 缺少参数，请检验参数是否传入 工作目录 \033[0m"
	exit
fi
scriptpath=${workspace}/om/package/script
xmlpath=/home/OM/cluster_1m0s_config.xml
username="autotest"
password=`cat ${xmlpath} |grep -w "password"|awk -F'"' '{print $4}'`
usergroup="dbgrp"
envfilepath="${workspace}/om/env"
logfilesavepath=/usr1/opengaussautoinstall/log
package_path_om=${workspace}/openGauss/package/openGauss-1.1.0-CentOS-64bit-om.tar.gz
package_path_app=${workspace}/openGauss/package/openGauss-1.0.1-CentOS-64bit.tar.bz2
package_path_app_sha256=${workspace}/openGauss/package/openGauss-1.0.1-CentOS-64bit.sha256
shellfilepath=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
hostname=`hostname`
ip=`hostname -i|awk '{print $1}'`
main()
{

mkdir /home/OM/ssh
cp  ~/.ssh/* /home/OM/ssh
sed -i "s/ip/${ip}/g" /home/OM/cluster_1m0s_config.xml
sed -i "s/hostname/${hostname}/g" /home/OM/cluster_1m0s_config.xml


if [ ! -e ${xmlpath} ]
then
        echo -e "\033[31m 配置文件XML不存在：${xmlpath}  \033[0m"
else
        installpath=`cat ${xmlpath} | grep name=\"gaussdbAppPath\"`
        installpath=${installpath#*/}
        installpath=${installpath%cluster*}
        echo "xml配置的安装路径：/$installpath"
        if [[ -n ${installpath} && -e /${installpath} ]]
        then
                rm -rf /${installpath}
        fi
fi


if [ -e ${workspace}/om/package/ ]
then
	rm -rf ${workspace}/om/package/*
else
	mkdir -p ${workspace}/om/package
fi
chmod -R 777 ${workspace}/om/package

tar -zvxf ${package_path_om} -C ${workspace}/om/package/
cp ${package_path_app} ${workspace}/om/package/
cp ${package_path_app_sha256} ${workspace}/om/package/
chmod -R 777 ${workspace}/om

if [ ! -e ${scriptpath} ]
then
	echo -e "\033[31m 工具脚本路径不存在：${scriptpath} \033[0m"
fi


if [ ! -e ${envfilepath} ]
then
	touch ${envfilepath}
else
	true > ${envfilepath}
fi
#对环境变量文件赋权777
chmod 777 ${envfilepath}

chmod -R 777 ${workspace}/om
echo -e "\033[32m 脚本存放路径：${scriptpath}；xml路径：${xmlpath}；用户名：${username}；用户组：${usergroup}；环境变量文件保存路径：${envfilepath}； \033[0m"
#confirmContinue
if [ -e ${logfilesavepath} ]
then
        rm -rf ${logfilesavepath}
        mkdir -p ${logfilesavepath}
        chmod -R 777 ${logfilesavepath}
else
        mkdir -p ${logfilesavepath}
        chmod -R 777 ${logfilesavepath}
fi

#检查用户
userexist=`cat /etc/passwd | cut -f1 -d':' | grep -w "${username}" -c`
if [ $userexist -gt 0 ]
then
     touch ${logfilesavepath}/cleanup.log
     chmod 777 ${logfilesavepath}/cleanup.log
     logdir=`logCheck cleanup.log`
     userdel -r ${username}  > ${logdir} 2>&1
     processresult=$(cat ${logdir} | grep "is currently used by process")
     echo $processresult
     if [ -n "${processresult}" ]
     then
        #processresult=${processresult} | sed 's/ //g'
        processresult1=${processresult#*process }
        expr ${processresult1} + 1 > /dev/null
        if [[ -n ${processresult1} && $? ]]
        then
                kill -9 ${processresult1}
                userdel -r ${username}
                echo "删除用户${username}"
        fi
     fi
fi

}


# 用户校验
checkUser()
{
        if [ $(env | grep USER | cut -d "=" -f 2) != "root" ]
        then
                echo -e "\033[31m 当前登录的用户为 $(env | grep USER | cut -d "=" -f 2) 为避免未知异常，请使用root用户操作，其中涉及到om
		用户的操作，脚本会自动切换  \033[0m"
                echo -e "\033[31m 脚本退出 \033[0m"
                exit
        fi
}


checkUser

# 程序运行倒计时
countDown()
{
	for i in `seq 5 -1 1`
        do
                echo -en "\033[32m $1 操作将在 $i 秒后执行......\r \033[0m"
                sleep 1s
        done
}

# 脚本运行日志
logCheck()
{
        if [ ! -e ${logfilesavepath}/$1 ]
        then
                touch ${logfilesavepath}/$1
                chmod 777 ${logfilesavepath}/$1
        else
                true > ${logfilesavepath}/$1
                chmod 777 ${logfilesavepath}/$1
        fi
        echo "${logfilesavepath}/$1"
}

# 确认程序是否进行
confirmContinue()
{
        echo "是否继续运行，确认（yes/no）"
        read confirmutils
        if [[ -n ${confirmutils} && ${confirmutils} != "yes" ]]
        then
                echo "脚本退出"
                exit
        fi
}


# 预安装
preinstall()
{
	logdir=`logCheck gs_preinstall.log`
	echo "正在执行：${scriptpath}/gs_preinstall -U ${username} -G ${usergroup} -X ${xmlpath} --sep-env-file=${envfilepath}"
	#spawn ${scriptpath}/gs_preinstall -U ${username} -G ${usergroup} -X ${xmlpath} --sep-env-file=${envfilepath}
	expect -f $(dirname $0)/auto_preinstall.sh ${username} ${usergroup} ${xmlpath} ${envfilepath} ${scriptpath} ${password}| tee ${logdir}
	preinstallresult=$?
        preinstalllogcheck=$(cat ${logdir} | grep "Preinstallation succeeded")
        if [[ ${preinstallresult} == 0 && -n ${preinstalllogcheck} ]]
        then
                echo -e "\033[32m 执行预安装操作成功 \033[0m"
		source ${envfilepath}
        else
                echo -e "\033[31m 执行预安装操作失败，日志同步保存在${logdir} \033[0m"
		        echo -e "\033[31m 脚本即将退出 \033[0m"
		        cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}


# 清理环境变量
postuninstall()
{
        echo -e "\033[32m 正在运行命令：/root/gauss_om/${username}/script/gs_postuninstall -U ${username} -X ${xmlpath} --delete-user...... \033[0m"
        #source $envfilepath
        logdir=`logCheck gs_postuninstall.log`
        /root/gauss_om/${username}/script/gs_postuninstall -U ${username} -X ${xmlpath} --delete-user | tee $logdir
        clearresult=$?
        # 根据日志校验是否正常执行清理工作
        logcheck=$(cat ${logdir} | grep "Successfully cleaned environment")
        if [[ ${clearresult} == 0 && -n "${logcheck}" ]]
        then
                echo -e "\033[32m 自动清理完成...... \033[0m"
        else
                echo -e "\033[31m 自动清理环境失败，请手动清理 command:gs_postuninstall -U ${username} -X ${xmlpath} --delete-user \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}


# 安装
install()
{
	chmod -R 755 ${scriptpath}
        chown -R ${username}:${usergroup} ${scriptpath}
        logdir=`logCheck gs_install.log`
        echo " 正在执行：gs_install -X "
        #expect -c "set timeout 400;
        su - ${username} -c '
                source $1
                #$2/gs_install -X $3 --dn-guc="modify_initial_password=false" | tee $4
                expect -f $2/auto_install.sh $3 $4 $5 | tee $6
        ' - ${envfilepath} ${shellfilepath} ${password} ${xmlpath}  ${scriptpath} ${logdir}
        #' - ${envfilepath} ${scriptpath} ${xmlpath} ${logdir}
        #expect {
         #   \"Please enter password for database\" {send \"Huawei@123\n\";exp_continue};
          #  \"Please repeat for database\" {send \"Huawei@123\n\";exp_continue};
           # eof {exit 0;}
           #}"
        installresult=$?
        installlogcheck=$(cat ${logdir} | grep "Successfully installed application")
        if [[ ${installresult} == 0 && -n ${installlogcheck} ]]
        then
                echo -e "\033[32m 执行安装操作成功 \033[0m"
        else
                echo -e "\033[31m 执行安装操作失败，日志同步保存在${logdir} \033[0m"
                postuninstall
				cp /home/OM/ssh/* ~/.ssh/
		        exit 1
        fi
}


# 查看状态
statusdb()
{
	logdir1=`logCheck gs_status.log`
	logdir2=`logCheck gs_status_hostname.log`
	logdir3=`logCheck gs_status_output.log`
	logdir4=`logCheck gs_status_detail.log`
        echo -e "\033[32m 正在执行：gs_om -t status \033[0m"
        echo $1
        su - ${username} -c'
		source $1
                gs_om -t status | tee $2
		gs_om -t status -h $(hostname) | tee $3
		mkdir $6/status
		touch $6/status/output.log
		gs_om -t status -o $6/status/output.log | tee $4
		gs_om -t status --detail | tee $5
        ' - ${envfilepath} ${logdir1} ${logdir2} ${logdir3} ${logdir4} ${logfilesavepath}

        statuslogcheck1=$(cat ${logdir1} | grep "cluster_state")
        if [ -n "${statuslogcheck1}" ]
        then
                echo -e "\033[32m gs_om -t status操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t status操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

	statuslogcheck2=$(cat ${logdir2} | grep "cluster_state")
	if [ -n "${statuslogcheck2}" ]
        then
                echo -e "\033[32m gs_om -t status -h $(hostname)操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t status -h $(hostname)操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

	statuslogcheck3=$(cat ${logfilesavepath}/status/output.log | grep "cluster_state")
	if [ -n "${statuslogcheck3}" ]
        then
                echo -e "\033[32m gs_om -t status -o ${logfilesavepath}/status/output.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t status -o ${logfilesavepath}/status/output.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

	statuslogcheck4=$(cat ${logdir4} | grep "cluster_state")
	if [ -n "${statuslogcheck4}" ]
        then
                echo -e "\033[32m gs_om -t status --detail 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t status --detail 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# 查看静态配置
view()
{
	logdir1=`logCheck gs_view.log`
        logdir2=`logCheck gs_view_output.log`
	echo -e "\033[32m 正在执行：gs_om -t view \033[0m"
	su - ${username} -c'
		source $1
                gs_om -t view | tee $2
		mkdir $4/view
		touch $4/view/output.log
		gs_om -t view -o $4/view/output.log | tee $3
        ' - ${envfilepath} ${logdir1} ${logdir2} ${logfilesavepath}

        viewlogcheck1=$(cat $logdir1 | grep "NodeHeader")
        if [ -n "${viewlogcheck1}" ]
        then
                echo -e "\033[32m gs_om -t view 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t view 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

	viewlogcheck2=$(cat ${logfilesavepath}/view/output.log | grep "NodeHeader")
        if [ -n "${viewlogcheck2}" ]
        then
                echo -e "\033[32m gs_om -t view -o ${logfilesavepath}/view/output.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t view -o ${logfilesavepath}/view/output.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# 查看状态
query()
{
	logdir1=`logCheck gs_query.log`
        logdir2=`logCheck gs_query_output.log`
        echo -e "\033[32m 正在执行：gs_om -t query \033[0m"
        su - ${username} -c'
                source $1
                gs_om -t query | tee $2
                mkdir $4/query
                touch $4/query/output.log
                gs_om -t query -o $4/query/output.log | tee $3
        ' - ${envfilepath} ${logdir1} ${logdir2} ${logfilesavepath}

        querylogcheck1=$(cat $logdir1 | grep "Cluster State")
        if [ -n "${querylogcheck1}" ]
        then
                echo -e "\033[32m gs_om -t query 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t query 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        querylogcheck2=$(cat ${logfilesavepath}/query/output.log | grep "Cluster State")
        if [ -n "${querylogcheck2}" ]
        then
                echo -e "\033[32m gs_om -t query -o ${logfilesavepath}/query/output.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t query -o ${logfilesavepath}/query/output.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

}

# 生成静态配置文件
generateconf()
{
	logdir1=`logCheck gs_generateconf.log`
        logdir2=`logCheck gs_generateconf_hostname.log`
        logdir3=`logCheck gs_generateconf_output.log`
        echo -e "\033[32m 正在执行：gs_om -t generateconf -X \033[0m"
        su - ${username} -c'
                source $1
                gs_om -t generateconf -X $2 | tee $3
                gs_om -t generateconf -X $2 --distribute | tee $4
                mkdir $6/generateconf
                touch $6/generateconf/output.log
                gs_om -t generateconf -X $2 -l $6/generateconf/output.log | tee $5
        ' - ${envfilepath} ${xmlpath} ${logdir1} ${logdir2} ${logdir3} ${logfilesavepath}

        generateconflogcheck1=$(cat ${logdir1} | grep "Successfully")
        if [ -n "${generateconflogcheck1}" ]
        then
                echo -e "\033[32m gs_om -t generateconf -X ${xmlpath} 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t generateconf -X ${xmlpath} 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        generateconflogcheck2=$(cat ${logdir2} | grep "Successfully")
        if [ -n "${generateconflogcheck2}" ]
        then
                echo -e "\033[32m gs_om -t generateconf -X ${xmlpath} --distribute 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t generateconf -X ${xmlpath} --distribute 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        generateconflogcheck3=$(cat ${logdir3} | grep "Successfully")
        if [ -n "${generateconflogcheck3}" ]
        then
                echo -e "\033[32m gs_om -t generateconf -X ${xmlpath} -l ${logfilesavepath}/generateconf/output.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t generateconf -X ${xmlpath} -l ${logfilesavepath}/generateconf/output.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

}

# 备份
backup()
{
	logdir1=`logCheck gs_backup.log`
        logdir2=`logCheck gs_backup_hostname.log`
        logdir3=`logCheck gs_backup_parameter.log`
	logdir4=`logCheck gs_backup_binary.log`
        logdir5=`logCheck gs_backup_all.log`
        logdir6=`logCheck gs_backup_log.log`
        echo -e "\033[32m 正在执行：gs_backup -t backup --backup-dir \033[0m"
        su - ${username} -c'
                source $1
		mkdir $8/backup
                touch $8/backup/log.log
                gs_backup -t backup --backup-dir=$8/backup/ | tee $2
                gs_backup -t backup --backup-dir=$8/backup/ -h $(hostname) | tee $3
		gs_backup -t backup --backup-dir=$8/backup/ --parameter | tee $4
		gs_backup -t backup --backup-dir=$8/backup/ --binary | tee $5
		gs_backup -t backup --backup-dir=$8/backup/ --all | tee $6
                gs_backup -t backup --backup-dir=$8/backup/ -l $8/backup/log.log | tee $7
        ' - ${envfilepath} ${logdir1} ${logdir2} ${logdir3} ${logdir4} ${logdir5} ${logdir6} ${logfilesavepath}

        backuplogcheck1=$(cat ${logdir1} | grep "Successfully backed up")
        if [ -n "${backuplogcheck1}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        backuplogcheck2=$(cat ${logdir2} | grep "Successfully backed up")
        if [ -n "${backuplogcheck2}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ -h $(hostname) 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ -h $(hostname) 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        backuplogcheck3=$(cat ${logdir3} | grep "Successfully backed up")
        if [ -n "${backuplogcheck3}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --parameter 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --parameter 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

	backuplogcheck4=$(cat ${logdir4} | grep "Successfully backed up")
        if [ -n "${backuplogcheck4}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --binary 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --binary 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        backuplogcheck5=$(cat ${logdir5} | grep "Successfully backed up")
        if [ -n "${backuplogcheck5}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --all 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ --all 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        backuplogcheck6=$(cat ${logdir6} | grep "Successfully backed up")
        if [ -n "${backuplogcheck6}" ]
        then
                echo -e "\033[32m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ -l ${logfilesavepath}/backup/log.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t backup --backup-dir=${logfilesavepath}/backup/ -l ${logfilesavepath}/backup/log.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# 恢复
restore()
{
	logdir1=`logCheck gs_restore.log`
        logdir2=`logCheck gs_restore_hostname.log`
        logdir3=`logCheck gs_restore_parameter.log`
        logdir4=`logCheck gs_restore_binary.log`
        logdir5=`logCheck gs_restore_all.log`
        logdir6=`logCheck gs_restore_log.log`
        echo -e "\033[32m 正在执行：gs_backup -t restore --backup-dir \033[0m"
        su - ${username} -c'
                source $1
                mkdir $8/restore
                touch $8/restore/log.log
                gs_backup -t restore --backup-dir=$8/backup/ | tee $2
                gs_backup -t restore --backup-dir=$8/backup/ -h $(hostname) | tee $3
                gs_backup -t restore --backup-dir=$8/backup/ --parameter | tee $4
                gs_backup -t restore --backup-dir=$8/backup/ --binary | tee $5
                gs_backup -t restore --backup-dir=$8/backup/ --all | tee $6
                gs_backup -t restore --backup-dir=$8/backup/ -l $8/restore/log.log | tee $7
        ' - ${envfilepath} ${logdir1} ${logdir2} ${logdir3} ${logdir4} ${logdir5} ${logdir6} ${logfilesavepath}

        restorelogcheck1=$(cat ${logdir1} | grep "Successfully restored")
        if [ -n "${restorelogcheck1}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        restorelogcheck2=$(cat ${logdir2} | grep "Successfully restored")
        if [ -n "${restorelogcheck2}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ -h $(hostname) 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ -h $(hostname) 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        restorelogcheck3=$(cat ${logdir3} | grep "Successfully restored")
        if [ -n "${restorelogcheck3}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --parameter 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --parameter 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        restorelogcheck4=$(cat ${logdir4} | grep "Successfully restored")
        if [ -n "${restorelogcheck4}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --binary 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --binary 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        restorelogcheck5=$(cat ${logdir5} | grep "Successfully restored")
	if [ -n "${restorelogcheck5}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --all 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ --all 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

        restorelogcheck6=$(cat ${logdir6} | grep "Successfully restored")
        if [ -n "${restorelogcheck6}" ]
        then
                echo -e "\033[32m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ -l ${logfilesavepath}/restore/log.log 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_backup -t restore --backup-dir=${logfilesavepath}/backup/ -l ${logfilesavepath}/restore/log.log 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

}

# ssh
sshdb()
{
	logdir=`logCheck ssh.log`
        echo -e "\033[32m 正在执行ssh操作：gs_ssh -c hostname \033[0m"
        su - ${username} -c'
		source $1
                gs_ssh -c hostname | tee $2
        ' - ${envfilepath} ${logdir}
        cmddbresult=$?
        cmdlogcheck=$(cat ${logdir} | grep "Successfully execute command on all nodes")
        if [[ ${cmddbresult} == 0 && -n "${cmdlogcheck}" ]]
        then
                echo -e "\033[32m gs_ssh -c hostname 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_ssh -c hostname 操作执行失败 \033[0m"
        fi
}

# 互信
sshexkey()
{
	logdir=`logCheck sshexkey.log`
        echo -e "\033[32m 正在执行sshexkey操作：gs_sshexkey -f ${logfilesavepath}/sshexkey/hostfile \033[0m"
        su - ${username} -c'
		source $1
		mkdir ${logfilesavepath}/sshexkey/
		touch ${logfilesavepath}/sshexkey/hostfile
		echo $( /sbin/ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6 | awk "{print \$2}" | tr -d "addr:"|head -1) >> ${logfilesavepath}/sshexkey/hostfile
		echo "0.0.0.0" >> ${logfilesavepath}/sshexkey/hostfile
                $2/gs_sshexkey -f ${logfilesavepath}/sshexkey/hostfile -W $3 | tee $4
        ' - ${envfilepath}  ${scriptpath} ${password}  ${logdir}
        trustdbresult=$?
        trustlogcheck=$(cat $logdir | grep "Successfully created SSH trust")
        if [[ ${trustdbresult} == 0 && -n "${trustlogcheck}" ]]
        then
                echo -e "\033[32m gs_sshexkey -f ${logfilesavepath}/sshexkey/hostfile操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_sshexkey -f ${logfilesavepath}/sshexkey/hostfile操作执行失败 \033[0m"
        fi
}


# 日志收集
collector()
{
	logdir=`logCheck collector.log`
	starttimeY=$(date -d -1day +%Y%m%d)
	starttimeH=$(date +%H)
	starttimeM=$(date +%M)
	starttime="$starttimeY"" ""${starttimeH}:${starttimeM}"
        endtimeY=$(date +%Y%m%d)
        endtimeH=$(date +%H)
        endtimeM=$(date +%M)
        endtime="$endtimeY"" ""${endtimeH}:${endtimeM}"
	echo -e "\033[32m 正在执行collector操作：gs_collector --begin-time=\"${starttime}\" --end-time=\"${endtime}\" \033[0m"
        su - ${username} -c'
		source $1
		echo gs_collector --begin-time="$2" --end-time="$3"
                gs_collector --begin-time="$2" --end-time="$3" | tee $4
        ' - ${envfilepath} "${starttime}" "${endtime}" ${logdir}
        collectordbresult=$?
        collectorlogcheck=$(cat ${logdir} | grep "Successfully collected files")
        if [[ ${collectordbresult} == 0 && -n "${collectorlogcheck}" ]]
        then
                echo -e "\033[32m gs_collector --begin-time=\"$starttime\" --end-time=\"$endtime\"操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_collector --begin-time=\"$starttime\" --end-time=\"$endtime\"操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# checkos 检查操作系统
checkos()
{
	logdir=`logCheck checkos.log`
        echo -e "\033[32m 正在执行checkos操作：gs_checkos -i A -h $(hostname) -X ${xmlpath} --detail \033[0m"
        ${scriptpath}/gs_checkos -i A -h $(hostname) -X ${xmlpath} --detail | tee ${logdir}
        checkosdbresult=$?
        checkoslogcheck=$(cat ${logdir} | grep "Checking items:")
        if [[ ${checkosdbresult} == 0 && -n "${checkoslogcheck}" ]]
        then
                echo -e "\033[32m gs_checkos -i A -h $(hostname) -X ${xmlpath} --detail -o ${logfilesavepath}/checkos 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_checkos -i A -h $(hostname) -X ${xmlpath} --detail -o ${logfilesavepath}/checkos 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# checkpref
checkpref()
{
	logdir1=`logCheck checkpref1.log`
	logdir2=`logCheck checkpref2.log`
        echo -e "\033[32m 正在执行：gs_checkpref PMK \033[0m"
        su - ${username} -c'
                source $1
		mkdir $4/checkperf
		touch $4/checkperf/log.log
                gs_checkperf -i PMK -U $2 -l $4/checkperf/log.log | tee $3
        ' - ${envfilepath} ${username} ${logdir1} ${logfilesavepath}
        checkprefdbresult1=$?
        checkpreflogcheck1=$(cat ${logdir1} | grep "Cluster statistics information")
        if [[ ${checkprefdbresult1} == 0 && -n "${checkpreflogcheck1}" ]]
        then
                echo -e "\033[32m gs_checkperf -i pmk -U ${username} 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_checkperf -i pmk -U ${username} 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
	countDown 检查SSD
	echo -e "\033[32m 正在执行：gs_checkpref SSD \033[0m"
	${scriptpath}/gs_checkperf -i ssd -U ${username} | tee ${logdir2}
	checkprefdbresult2=$?
        checkpreflogcheck2=$(cat ${logdir2} | grep "Cluster statistics information")
        if [[ ${checkprefdbresult2} == 0 && -n "${checkpreflogcheck2}" ]]
        then
                echo -e "\033[32m gs_checkperf -i SSD -U ${username} 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_checkperf -i SSD -U ${username} 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# 停止
stopdb()
{
	logdir=`logCheck stopdb.log`
        echo -e "\033[32m 正在执行stop操作：gs_om -t stop \033[0m"
        su - ${username} -c'
		source $1
                gs_om -t stop | tee $2
        ' - ${envfilepath} ${logdir}
        stopdbresult=$?
        stoplogcheck=$(cat ${logdir} | grep "Successfully stopped")
        if [[ ${stopdbresult} == 0 && -n "${stoplogcheck}" ]]
        then
                echo -e "\033[32m gs_om -t stop操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t stop操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

}

# 启动
startdb()
{
	logdir=`logCheck startdb.log`
        echo -e "\033[32m 正在执行start操作：gs_om -t start \033[0m"
        su - ${username} -c'
		source $1
                gs_om -t start | tee $2
        ' - ${envfilepath} ${logdir}
        startdbresult=$?
        startlogcheck=$(cat ${logdir} | grep "Successfully started")
        if [[ ${startdbresult} == 0 && -n "${startlogcheck}" ]]
        then
                echo -e "\033[32m gs_om -t start操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t start操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}

# 停止
stopwithhostname()
{
        logdir=`logCheck stophostname.log`
        echo -e "\033[32m 正在执行stop操作：gs_om -t stop -h $(hostname) \033[0m"
        su - ${username} -c'
                source $1
                gs_om -t stop -h $(hostname) | tee $2
        ' - ${envfilepath} ${logdir}
        stophresult=$?
        stophlogcheck=$(cat ${logdir} | grep "Successfully stopped")
        if [[ ${stophresult} == 0 && -n "${stophlogcheck}" ]]
        then
                echo -e "\033[32m gs_om -t stop -h $(hostname) 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t stop -h $(hostname) 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi

}

# 启动
startwithhostname()
{
        logdir=`logCheck starthostname.log`
        echo -e "\033[32m 正在执行start操作：gs_om -t start -h $(hostname) \033[0m"
        su - ${username} -c'
                source $1
                gs_om -t start -h $(hostname) | tee $2
        ' - ${envfilepath} ${logdir}
        starthresult=$?
        starthlogcheck=$(cat ${logdir} | grep "Successfully started")
        if [[ ${starthresult} == 0 && -n "${starthlogcheck}" ]]
        then
                echo -e "\033[32m gs_om -t start -h $(hostname) 操作执行成功 \033[0m"
        else
                echo -e "\033[31m gs_om -t start -h $(hostname) 操作执行失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}
# 卸载
uninstall()
{
        logdir=`logCheck gs_uninstall.log`
        echo -e "\033[32m 正在运行命令：gs_uninstall --delete-data \033[0m"
        su - ${username} -c'
                source $1
                gs_uninstall --delete-data | tee $2
        ' - ${envfilepath} ${logdir}
        uninstallresult=$?
        uninstalllogcheck=$(cat ${logdir} | grep "Uninstallation succeeded")
        if [[ ${uninstallresult} == 0 && -n "${uninstalllogcheck}" ]]
        then
                echo -e "\033[32m 执行gs_uninstall --delete-data操作成功 \033[0m"
        else
                echo -e "\033[31m 执行gs_uninstall --delete-data操作失败 \033[0m"
				cp /home/OM/ssh/* ~/.ssh/
                exit 1
        fi
}



main
countDown "预安装"
preinstall
countDown "安装"
install
countDown "状态信息查看"
statusdb
countDown "查看静态配置"
view
countDown "查看opengauss状态"
query
countDown "生成静态配置文件"
generateconf
countDown "备份"
backup
countDown "恢复"
restore
#countDown "ssh"
#sshdb
#countDown "互信"
#sshexkey
countDown "收集日志"
collector
countDown "检查系统配置"
checkos
#countDown "集群资源检查"
#checkpref
countDown "数据库停止"
stopdb
countDown "数据库启动"
startdb
countDown "数据库停止 指定hostname"
stopwithhostname
countDown "数据库启动 指定hostname"
startwithhostname
countDown "数据库卸载"
uninstall
countDown "环境清理"
postuninstall

cp /home/OM/ssh/* ~/.ssh/
echo -e "\033[32m 自动化测试完成......程序退出！ \033[0m"
