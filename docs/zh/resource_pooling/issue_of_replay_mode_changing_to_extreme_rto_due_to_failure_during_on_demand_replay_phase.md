# 因按需回放阶段发生故障导致回放模式变更为极致RTO的问题

## 问题现象

数据库配置了按需回放，但是主机发生故障后，数据库使用极致RTO进行回放。

## 定位方法

检查数据库日志，目录为`$GAUSSLOG/pg_log/dn_xxxx`，可以看出数据库集群在上一轮按需回放中未成功，因此本次选择了极致RTO回放。

```shell
2024-10-15 17:14:06.807 670e3259.5407 [unknown] 281329567313824 dn_6003 0 dn_6001_6002_6003 00000  0 [BACKEND] LOG:  [SS reform][On-demand] do not allow replay in ondemand recovery if last ondemand recovery crash, replayed in extreme rto recovery mode
```

此时如果查询控制文件，可以查询到`Cluster status`状态为`in on-demand build`或者`in on-demand redo`。该信息为上一轮按需回放的残留状态，实际回放状态判断应当以日志中的状态为准，残留信息在本轮故障恢复结束后会刷新为`normal`。

```shell
[chendong@openGauss137 dn_6003]$ pg_controldata +data

pg_control data

...

reformer data (last page id 64)

Reform control version number:        1
Stable instances list:                6
Primary instance ID:                  2
Recovery instance ID:                 0
Cluster status:                       in on-demand redo
Cluster run mode:                     primary cluster
```

## 问题根因

按需回放特性约束，如果使用按需回放进行故障恢复，但是实际回放未成功（包含`in on-demand build`阶段和`in on-demand redo`阶段未成功）。再次故障恢复时，如果恢复节点的回放模式为按需回放时，会选择使用极致RTO回放。

## 解决方案

等待本轮回放结束后，控制文件`Cluster status`状态恢复为`normal`。之后如果再发生故障，且恢复节点配置了按需回放，则会选择按需回放进行故障恢复。

```shell
[chendong@openGauss137 dn_6003]$ pg_controldata +data

pg_control data

...

reformer data (last page id 64)

Reform control version number:        1
Stable instances list:                6
Primary instance ID:                  2
Recovery instance ID:                 0
Cluster status:                       normal
Cluster run mode:                     primary cluster
```
