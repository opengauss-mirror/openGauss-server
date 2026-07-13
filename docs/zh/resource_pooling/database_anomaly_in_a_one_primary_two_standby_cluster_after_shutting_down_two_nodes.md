# 一主两备集群停掉两个节点数据库异常

## 现象

一主两备集群，使用cm_ctl stop命令停掉两个节点后，数据库状态异常。通过cm_ctl query -Cv命令查询数据库状态无法连接cm_server，出现"cm_ctl:can't connect to cm_server"字样。

## 原因

一主多备集群下，cm_server正常运行需要满足多数派，至少需要一半节点的cm_server正常运行。
 >[!NOTE]说明  
  >
  > - 在一主一备的集群下，是允许存在一个节点的，即cm_server的primary节点。
 
## 解决方案

保证一半以上的节点正常。
