# 因postgres.conf中参数不符合标准导致启动或重启集群失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为正常，`Datanode State`中有的节点的状态为`Down  Manually stopped`, `cluster_state`为`Degraded`。

## 二、定位方法

登录故障节点机器，进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下几种报错信息：

```shell
0 [BACKEND] LOG:  1048576 is outside the valid range for parameter "max_process_memory" (2097152 .. 2147483647)

2024-10-11 09:23:11.594 67087dff.10000 [unknown] 281468814098448 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

```shell
0 [BACKEND] LOG:  invalid value for parameter "max_process_memory": "2247482647"

0 [BACKEND] HINT:  Value exceeds integer range.

2024-10-11 09:55:28.257 67088590.10000 [unknown] 281468215689232 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

通过上述这两种报错信息可以看出是`postgresqk.conf`文件内`max_process_memory`参数值过小或过大导致的。

```shell
 0 [BACKEND] LOG:  15 is outside the valid range for parameter "shared_buffers" (16 .. 1073741823)

2024-10-11 10:23:10.891 67088c0e.10000 [unknown] 281466526957584 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

```shell
 0 [BACKEND] LOG:  1083741823 is outside the valid range for parameter "shared_buffers" (16 .. 1073741823)

2024-10-11 10:37:44.079 67088f78.10000 [unknown] 281460336885776 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] FATAL:  configuration file "/.../.../dn1/postgresql.conf" contains errors
```

通过上述这两种报错信息可以看出是`postgresqk.conf`文件内`shared_buffers`参数值过小或过大导致的。

其他的参数不符合标准的问题与上述类似，在`DBstart.log`日志中均有具体的报错信息。

另外，还有一种情况是参数`enable_incremental_checkpoint`的值设置为 off 后重启集群，有的节点会有小概率启动失败，状态为`Down`。这种情况在`DBstart.log`日志中会有如下报错信息：

```shell
2024-09-03 21:40:53.303 66d711e5.1 [unknown] 281457756798992 [unknown] 0 dn_6001_6002_6003 58000 0 [BACKEND] FATAL:  archive functions are not supported when DMS and DSS enabled
```

上述报错信息可以看出 archive functions 被打开且目前暂不支持此功能导致集群启动失败，计划将在 7.0.0-RC1 版本支持开启 xlog 归档。目前相关信息可以参考社区之前的 issue ：[设置GUC参数enable_incremental_checkpoint设置为off后重启集群，主节点概率无法启动成功](https://gitee.com/opengauss/openGauss-server/issues/IAQYW7?from=project-issue)。

## 三、问题根因

系统配置文件`postgresql.conf`内各种参数出现异常导致的启动或重启集群失败。具体参数说明参考[Guc参数说明](https://docs.opengauss.org/zh/docs/latest/database_reference/guc_parameter_usage.html)。

## 四、解决方案

针对该问题的解决方案为，查看`DBstart.log`日志找到具体报错的参数，修改报错的参数值为正常值，然后重启集群即可。
