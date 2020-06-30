使用说明：
1.将cluster_check脚本上传至任意一台装有cn的节点上，并且确保cluster_check路径对omm用户有写的权限。
2.getClusterInfo.sh脚本使用omm用户执行；getOSInfo.sh脚本使用root用户执行，需要输入所有节点的root密码。
3.确认主运行脚本getClusterInfo.sh中PORT=25308端口配置是否正确，不正确需要修改为正确的数据库连接端口。

4.配置当前路径下hostfile文件，添加集群所有主机的主机名或者IP地址。

5.切换路径到检查工具所在的路径，执行以下命令：
sh getClusterInfo.sh

6.生成的结果会保存在脚本所在路径下的out文件里面。

