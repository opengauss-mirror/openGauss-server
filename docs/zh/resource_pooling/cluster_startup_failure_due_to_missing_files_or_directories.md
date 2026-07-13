# 因文件、目录不存在导致集群启动失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点正常，`Datanode State`中有的节点的状态为`Down  Manually stopped`, `cluster_state`为`Degraded`。

## 二、定位方法

1. 登录故障节点机器，进入`$GAUSSLOG/cm/cm_agent`目录下，寻找该节点最近时间点的`cm_agent`日志，发现如下报错信息：

   ```shell
   2024-10-10 09:26:02.270 tid=2015951  LOG: gaussdb state file "/.../.../dn1/gaussdb.state" is not exist, could not get the build infomation: No such file or directory

   2024-10-10 09:26:02.746 tid=2015996 DiskUsageCheck ERROR: [GetDiskUsageForPath][line:908] GetDiskUsageForPath /.../.../dn1 disk usage failed! errno:2 err:No such file or directory.

   2024-10-10 09:26:02.880 tid=2015948  ERROR: [get_connection: 1526]: fail to read pid file (/.../.../dn1/postmaster.pid).
   2024-10-10 09:26:02.880 tid=2015948  ERROR: failed to connect to datanode:/.../...//dn1
   ```
   
   绝对路径`/.../.../dn1`就是`$PGDATA`，上述报错信息表明问题是`$PGDATA`目录不存在。

2. 使用`cd $PGDATA`命令却回显以下信息，印证了问题是`$PGDATA`目录不存在。

   ```shell
   [  ~]$ cd $PGDATA
   -bash: cd: /.../.../dn1: No such file or directory
   ```

3. 若在`cm_agent`中未发现明确的报错信息，但能进入`$PGDATA`目录下，则查看`DBstart.log`日志，发现如下报错信息：

   ```shell
   2024-10-10 09:55:35.343 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

   2024-10-10 09:55:35.543 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

   2024-10-10 09:55:35.743 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 58P01  0 [BACKEND] LOG:  could not open configuration file "/.../.../dn1/pg_hba.conf": No such file or directory

   2024-10-10 09:55:35.743 67073417.1 [unknown] 281471761580048 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
   ```
  
   这个数据库启动日志的报错明确说明了`pg_hba.conf`文件不存在。

## 三、问题根因

系统配置文件、目录缺失导致集群启动或重启失败。

## 四、解决方案

若还存在状态正常的节点机器，可以将正常节点机器的目录或文件复制到故障节点机器上；若没有正常节点机器，可以卸载然后重新进行安装启动。
