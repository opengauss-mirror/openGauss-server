# 因控制文件异常导致集群启动失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为正常，`Datanode State`中所有节点的状态为`Down  Manually stopped`, `cluster_state`为`Unavailable`。

## 二、定位方法

1. 进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下报错信息：

   ```shell
   2024-10-10 15:35:00.648 670783a4.1 [unknown] 281469404839952 [unknown] 0 dn_6001_6002 00000  0 [BACKEND] LOG:  base_page_saved_interval is 400, ori is 400.

   gaussdb: could not find the database system

   Expected to find it in the directory "/.../.../dn1",

   but could not open file "+data/pg_control": Unknown error 2190
   ```

   通过上述报错信息可以看出是`+data/pg_control`文件异常导致。

2. 使用`cm_ctl stop`命令停止 cm 相关服务，然后使用`dssserver -M &`命令启动 dss 服务，当回显`DSS SERVER STARTED.`时说明启动 dss 服务成功。

3. 在启动 dss 服务成功后，可以使用`dsscmd -ls -p +data`命令查看`pg_control`文件是否存在、是否损坏、是否改名，即可找到对应问题所在。

## 三、问题根因

系统控制文件`pg_control`出现异常导致的启动或重启集群失败。

## 四、解决方案

针对该问题，有两种情况分别有不同的解决方案，如下：

1. 若`pg_control`文件不存在或损坏了，卸载集群重新安装即可。

2. 若`pg_control`文件存在且未损坏，只是更换了文件名称：

   ```shell
   dsscmd rename       将文件名称恢复
   
   举例，将现有文件 pg_control.old 恢复其名称为 pg_control, 命令如下：
   dsscmd rename -o +data/pg_control.old -n +data/pg_control

   dsscmd stopdss  停止 dss 服务

   cm_ctl start    启动集群即可  
   ```
