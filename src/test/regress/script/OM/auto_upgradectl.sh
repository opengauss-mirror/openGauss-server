#pengass om 自动化升级测试脚本
# username:omm  usergroup:dbgrp 工具脚本路径：/usr1/opengaussautomattest/script xml文件路径：/usr1/clusterconfig.xml

workdir=$1
upgradepackage=$2
echo -e "\033[32m 注意事项：  \033[0m"
echo -e "\033[32m       1、软件包路径请设置成不同于xml配置的安装路径以及日志保存路径 \033[0m"
echo -e "\033[32m       2、确保软件包路径除了xml配置不含有其他文件 \033[0m"
echo -e "\033[32m       3、需要互信的主机手动修改密码为设置密码  \033[0m"
echo -e "\033[32m       4、升级包命名为upgrade.tar.gz，并且解压以后文件夹名称为upgrade  \033[0m"

if [ $# -lt 2 ]
then
        echo -e "\033[31m 缺少参数，请检验参数长度 分别是 工作目录、软件包名称 \033[0m"
        exit
fi
scriptpath=${workdir}/om/package/script
xmlpath=/home/OM/cluster_1m0s_config.xml
username="autotest"
password=`cat ${xmlpath} |grep -w "password"|awk -F'"' '{print $4}'`
usergroup="dbgrp"
envfilepath="${workdir}/om/env1"
logfilesavepath=/home/OM/log
upgradefilepath_om=${workdir}/openGauss/package/openGauss-1.1.0-CentOS-64bit-om.tar.gz
upgradefilepath_app=${workdir}/openGauss/package/openGauss-1.0.1-CentOS-64bit.tar.bz2
upgradefilepath_app_sha256=${workdir}/openGauss/package/openGauss-1.0.1-CentOS-64bit.sha256
package_path=/home/openGauss-1.0.1-CentOS-b109-64bit.tar.gz 
shellfilepath=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
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

main()
{

sed -i "s/ip/${ip}/g" /home/OM/cluster_1m0s_config.xml
sed -i "s/hostname/${hostname}/g" /home/OM/cluster_1m0s_config.xml

if [ -e ${workdir}/om/package/ ]
then
        rm -rf ${workdir}/om/package/*
else
        mkdir -p ${workdir}/om/package
fi
chmod -R 777 ${workdir}/package

tar -zvxf ${package_path} -C ${workdir}/om/package/

chmod -R 777 ${workdir}/om

if [ ! -e ${scriptpath} ]
then
        echo -e "\033[31m 工具脚本路径不存在：${scriptpath} \033[0m"
fi

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

if [ ! -e ${envfilepath} ]
then
        touch ${envfilepath}
else
        true > ${envfilepath}
fi
#对环境变量文件赋权777
chmod 777 ${envfilepath}

chmod -R 777 ${workdir}
echo -e "\033[32m 脚本存放路径：${scriptpath}；xml路径：${xmlpath}；用户名：${username}；用户组：${usergroup}；环境变量文件保存路径：${envfilepath}； \033[0m"
#confirmContinue
if [ -e ${logfilesavepath} ]
then
        rm -rf ${logfilesavepath}
        mkdir ${logfilesavepath}
        chmod -R 777 ${logfilesavepath}
        chown -R ${username}:${usergroup}  ${logfilesavepath}
else
        mkdir ${logfilesavepath}
        chmod -R 777 ${logfilesavepath}
        chown -R ${username}:${usergroup}  ${logfilesavepath}
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
        expect -f $(dirname $0)/auto_preinstall.sh ${username} ${usergroup} ${xmlpath} ${envfilepath} ${scriptpath} ${password} | tee ${logdir}
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
                #$2/gs_install -X $3 | tee $4
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
#升级
upgradectl()
{
        chmod 777 ${upgradefilepath_om}
        chmod 777 ${upgradefilepath_app}
        chmod 777 ${upgradefilepath_app_sha256}
        echo "准备停止集群"
        logdir=`logCheck stopdb.log`
        echo -e "\033[32m 正在执行stop操作：gs_om -t stop \033[0m"
        su - ${username} -c'
                source $1
                gs_om -t stop | tee $2
        ' - ${envfilepath} ${logdir}
        stopdbresult=$?
        stoplogcheck=$(cat ${logdir} | grep "Successfully stopped cluster")
        if [[ ${stopdbresult} == 0 && -n ${stoplogcheck} ]]
        then
                echo "gs_om -t 停止集群操作执行成功，准备进行升级包解压"
                if [ ! -e ${workdir}/package/upgrade/ ]
                then
                        tar -zxvf ${upgradefilepath_om} -C ${workdir}/om/package/
                        cp -rf ${upgradefilepath_app} ${workdir}/om/package/
                        cp -rf ${upgradefilepath_app_sha256} ${workdir}/om/package/
                fi
                if [ $? == 0 ]
                then
                        echo "文件解压完成，准备执行预安装"
                        echo -e "\033[32m 准备执行预安装操作...... \033[0m"
                echo -e "\033[32m 正在运行：${scriptpath}/gs_preinstall -U ${username} -G ${usergroup} -X ${xmlpath} --sep-env-file=${envfilepath} --non-interactive  ；\033[0m"
                logdir=`logCheck gs_preinstall1.log`
                ${scriptpath}/gs_preinstall -U ${username} -G ${usergroup} -X ${xmlpath} --sep-env-file=${envfilepath} --non-interactive  | tee ${logdir}
                preinstallresult=$?
                preinstalllogcheck=$(cat $logdir | grep "Preinstallation succeeded")
                if [[ ${preinstallresult} == 0 && -n ${preinstalllogcheck} ]]
                then
                        echo -e "\033[32m 执行预安装操作成功，准备进行升级操作 \033[0m"
                        logdir=`logCheck upgradectl.log`
                        su - ${username} -c'
                                source $1
                                gs_upgradectl -t auto-upgrade -X $2 | tee $3
                        ' - ${envfilepath} ${xmlpath} ${logdir}
                        upgraderesult=$?
                        upgradelogcheck=$(cat ${logdir} | grep "Upgrade main process has been finished")
                        if [[ ${upgraderesult} == 0 && -n ${upgradelogcheck} ]]
                        then
                                echo -e "\033[32m 升级完成，进行gs_upgradectl -t commit-upgrade校验 \033[0m"
                                logdir=`logCheck upgradecheck.log`
                                su - ${username} -c'
                                        source $1
                                        gs_upgradectl -t commit-upgrade -X $2 | tee $3
                                ' - ${envfilepath} ${xmlpath} ${logdir}
                                upgraderesult1=$?
                                upgradelogcheck1=$(cat ${logdir} | grep "Successfully")
                                if [[ ${upgraderesult1} == 0 && -n ${upgradelogcheck1} ]]
                                then
                                        echo -e "\033[32m 升级完成 \033[0m"
                                else
                                        echo -e "\033[32m 升级完成，但是自动检查不通过，建议手动检查是否正常升级，确保功能无误 \033[0m"
                                fi
                        else
                                echo -e "\033[31m 升级失败 \033[0m"
								cp /home/OM/ssh/* ~/.ssh/
								exit 1
                        fi
                else
                        echo -e "\033[31m 执行预安装操作失败，升级终止 \033[0m"
						exit 1
						cp /home/OM/ssh/* ~/.ssh/
                fi
                else
                        echo "文件解压出错，升级终止"
						cp /home/OM/ssh/* ~/.ssh/
						exit 1

                fi
        else
                echo -e "\033[31m gs_om -t 停止集群操作执行失败，升级终止 \033[0m"
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

# 清理环境变量
postuninstall()
{
        echo -e "\033[32m 正在运行命令：${scriptpath}/gs_postuninstall -U ${username} -X ${xmlpath} --delete-user...... \033[0m"
        #source $envfilepath
        logdir=`logCheck gs_postuninstall.log`
        ${scriptpath}/gs_postuninstall -U ${username} -X ${xmlpath} --delete-user | tee $logdir
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

main
countDown "预安装"
preinstall
countDown "安装"
install
countDown "数据库启动 指定hostname"
startwithhostname
countDown "集群升级"
upgradectl
countDown "数据库卸载"
uninstall
countDown "环境清理"
postuninstall


