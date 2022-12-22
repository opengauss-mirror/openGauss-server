容器化升级操作指南  
前置条件：  
旧版本容器集群状态正常  
容器化升级操作步骤：  
1. 准备升级工具、新版本容器和包  
(1) 新版本容器镜像需载入到环境  
(2) 将升级工具和新版本包放入升级路径
相关路径说明：  
GAUSSDATA：数据库数据目录，默认为/var/lib/opengauss/data，升级工具中可以通过-D指定  
GAUSS_UPGRADE_BASE_PATH：升级根路径，默认为$GAUSSDATA/upgrade，升级工具中可以通过-B指定  
UPGRADE_NEW_PKG_PATH：新包路径，默认为$GAUSS_UPGRADE_BASE_PATH/pkg_new，升级工具中可以通过-N指定  
新版本包中必须包含version.cfg、upgrade_sql.tar.gz和upgrade_sql.sha256  
2. 在**所有旧版本主备机容器**中执行前置升级动作  
sh upgrade.sh -t upgrade_pre
3. 停掉所有旧版本库
4. 启动所有新版本库并在所有新版本主备机容器中执行upgrade_bin、upgrade_post和upgrade_commit动作  
sh upgrade.sh -t upgrade_bin  
sh upgrade.sh -t upgrade_post  
sh upgrade.sh -t upgrade_commit  

回退操作步骤：  
case 1: 已切换到新二进制  
sh upgrade.sh -t rollback_post (已执行upgrade_post动作)  
sh upgrade.sh -t rollback_bin  (已执行upgrade_bin动作)  
退出新版本容器  

case 2: 仅执行了upgrade_pre动作  
sh upgrade.sh -t rollback_pre  

**注意：**  
如果升级路径和新包路径自行规划，则需要在新旧容器中做着两个路径的卷映射