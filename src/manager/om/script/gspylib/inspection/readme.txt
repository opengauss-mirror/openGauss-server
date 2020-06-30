1、在集群中选定一个含有cn的节点；

2、准备一个名为hostfile的文本文件，文件内容为集群所有数据节点的hostname列表(一行一个hostname)，并将该文件上传到/home/omm/目录下
   若要进行扩容新节点检查，hostfile中应包含所有扩容新节点IP
   若要检查扩容新老节点一致性，hostfile中应同时包含新节点和老节点

3、登录该CN节点，用omm用户执行下边命令：
        su - omm
        source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile
        for i in `cat /home/omm/hostfile`;do ssh $i "hostname;rm -rf /home/omm/test_check" ;done
        for i in `cat /home/omm/hostfile`;do ssh $i "hostname;mkdir /home/omm/test_check" ;done

4、将Check包解压后，用omm用户上传至该CN节点的/home/omm/test_check目录下，

5、修改test_check目录及目录下文件的属主为omm
    
    chown -R omm:wheel /home/omm/test_check/Check/
    
6、给检查脚本赋予执行权限

    cd /home/omm/test_check

    chmod +x -R /home/omm/test_check/
    
    
7、执行脚本，进行集群检查、信息收集

    for i in `cat /home/omm/hostfile`;do scp -r /home/omm/test_check/* $i:/home/omm/test_check/;done
    
    cd /home/omm/test_check/Check
    
    source /opt/huawei/Bigdata/mppdb/.mppdbgs_profile
    
    若要巡检集群，则使用如下命令
    ./gs_check -e inspect -U omm 
	
    若要进行升级前检查，则使用如下命令
    ./gs_check -e upgrade -U omm

    若要进行扩容前检查，则使用如下命令
    ./gs_check -e expand -U omm

    若要进行新节点检查，则使用如下命令
    1. 切换到root或有root权限的用户
    2. 执行 ./gs_check -e expand_new_node --hosts=/home/omm/hostfile

    若要进行信息收集，则使用如下命令
    ./gs_check -i CheckCollector -L

    注意：上述检查命令执行时中间若涉及到root检查项，会提示输入root密码

8、将/home/omm/test_check/Check/inspection目录下的output文件夹压缩后传回。