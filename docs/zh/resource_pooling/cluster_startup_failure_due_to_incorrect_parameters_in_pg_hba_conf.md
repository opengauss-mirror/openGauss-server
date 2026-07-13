# 因 pg_hba.confg 中参数配置问题导致启动或重启集群失败的问题

## 一、问题现象

若openGauss资源池化集群启动或重启失败，有如下报错信息：

```shell
cm_ctl: start cluster failed in (601)s!

HINT: Maybe the cluster is continually being started in the background.

You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
```

报告启动集群失败，10min 超时。

使用`cm_ctl query -Cvipd`查询集群状态后显示`CMServer State`中所有节点为正常，`Datanode State`中有的节点的状态为`Down`。

## 二、定位方法

1. 登录故障节点机器，进入`$PGDATA`目录下，查看`DBstart.log`日志，发现如下几种报错信息：

    ```shell
    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] LOG:  end-of-line before authentication method

    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] CONTEXT:  line 94 of configuration file "/.../.../dn1/pg_hba.conf"

    2024-10-11 14:27:29.056 6708c550.1 [unknown] 281459392315408 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
    ```

    ```shell
    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] LOG:  invalid authentication method "sm9"

    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 F0000  0 [BACKEND] CONTEXT:  line 94 of configuration file "/.../.../dn1/pg_hba.conf"

    2024-10-11 14:46:31.121 6708c9c6.1 [unknown] 281464592334864 [unknown] 0 dn_6001_6002 42809  0 [BACKEND] FATAL:  could not load pg_hba.conf
    ```

    通过上述这两种报错信息可以看出是`pg_hba.conf`文件内第 94 行 method 没有设置或设置的认证方式暂不支持。

2. 进入`$GAUSSLOG/pg_log/dn_xxx`目录下，寻找该节点最近时间点的`postgresql-xxx.log`日志，发现如下报错信息：

    ```shell
    2024-10-11 16:14:42.190 6708de72.5065 postgres 281464533200800 dn_6002 0 dn_6001_6002 28000  0 [BACKEND] FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL on

    2024-10-11 16:14:42.216 6708de72.5065 postgres 281464533200800 dn_6002 0 dn_6001_6002 28000  0 [BACKEND] FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    2024-10-11 16:14:42.233 6708de34.6257 [unknown] 281464591396768 dn_6002 0 dn_6001_6002 YY004  0 [BACKEND] ERROR:  Could not connect to the postgres, we have tried 10 times, the connection info: FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL on
    
            FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off
    ```

3. 进入`$GAUSSLOG/cm/cm_agent`目录下，寻找该节点最近时间点的`cm_agent`日志，发现如下报错信息：

    ```shell
    2024-10-11 16:49:55.955 tid=2425227  ERROR: get connect failed for dn(/.../.../dn1/postmaster.pid) phony dead check, errmsg is FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    2024-10-11 16:49:55.955 tid=2425227  LOG: has found 51 times for instance(dn_6002) phony dead check.

    2024-10-11 16:49:56.431 tid=2425223  ERROR: failed to connect to datanode:/.../.../dn1

    2024-10-11 16:49:56.431 tid=2425223  ERROR: connection return errmsg : FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off

    FATAL:  no pg_hba.conf entry for host "::1", user "xxx", database "postgres", SSL off
    ```

    通过第二、三中报错信息可以看出是`pg_hba.conf`文件内 ip 设置异常导致的启动或重启集群失败。

## 三、问题根因

系统配置文件`pg_hba.conf`内各种参数出现异常导致的启动或重启集群失败。具体参数说明参考[配置文件参考](../database_administration_guide/configuration_file_reference.md)。

## 四、解决方案

针对该问题的解决方案为，查看`DBstart.log`和`postgresql-xxx.log`日志找到具体报错的参数，修改报错的参数为正常值或推荐值，然后重启集群即可。
