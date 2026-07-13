# 因网络原因导致集群无法启动的问题

## 一、问题现象

在openGauss资源池化集群启动过程中，因网络原因会导致集群中节点启动失败。根本原因为网络问题导致`CM server`主节点的命令无法下发至`CM Agent`节点，从而使得数据库没有收到启动行为。本文将会以防火墙开启场景为例，详细描述排查步骤和结果。

## 二、定位方法

以防火墙开启场景为例，使用`cm_ctl start`指令启动集群后，第二个节点存在故障，无法成功拉起。经由`cm_ctl query -Cvipdw`查询，状态为`Down Starting`，定位排查流程如下：

1. 进入`$GAUSSLOG/pg_log`目录中，可以看到没有`Gaussdb`的启动日志，确定为在CM仲裁层面启动失败。
2. 进入`$GAUSSHOME/cm/cm_agent`目录中，利用`zgrep`指令在最近时间点的文件日志中过滤`restart`关键字，可发现近期没有`restart`启动记录，可以认为`CM Agent`并无启动行为。
3. 通过`cm_ctl query -Cvipdw`命令，得到`CM server`主节点。进入该主节点的`$GAUSSHOME/cm/cm_server`目录下，查看最近的日志，得到如下信息：

    ```shell
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: Network base_time:2024-09-26 17:59:33
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|2024-09-26 17:59:32|2024-09-26 17:59:32|
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |1970-01-01 08:00:00|1970-01-01 08:00:00|1970-01-01 08:00:00|
    2024-09-26 17:59:33.855 tid=1008512 MaxClusterAb LOG: [RHB] hb infos: |2024-09-26 17:59:30|2024-09-26 17:59:30|1970-01-01 08:00:00|    
    ```

    上述日志的第二行为故障节点收到的`heart beat`心跳消息记录，可看到其他节点均收到该节点的心跳，但是故障节点并未收到其他节点的心跳记录。
4. 进一步的，进入非故障节点的`$GAUSSHOME/cm/cm_agent`目录下，在最近时间点日志中可观测到如下记录：

    ```shell
    2024-09-26 18:59:48.432 tid=1008255 RHB ERROR: [mes] send pipe to dst_inst[1] priority[0] is not ready.
    ```

    上述日志说明该节点和故障节点的底层链接无法建立，根本原因为TCP通信无法有效建立，存在链路层面的故障。
5. 根据上述思路，排查链路原因，利用防火墙命令的可以查询故障节点的防火墙运行状况：

    ```shell
    ● firewalld.service - firewalld - dynamic firewall daemon
    Loaded: loaded (/usr/lib/systemd/system/firewalld.service; enabled; vendor preset: enabled)
    Active: active (dead) since Thu 2024-09-26 16:50:19 CST; 10s ago
        Docs: man:firewalld(1)
    Process: 176150 ExecStart=/usr/sbin/firewalld --nofork --nopid $FIREWALLD_ARGS (code=exited, status=0/SUCCESS)
    Main PID: 176150 (code=exited, status=0/SUCCESS)
    ```

    可看到链路无法建立的根本原因为故障节点的防火墙开启，导致其他节点到该节点的单向链路不通。

## 三、问题根因

当某节点的`CM Agent`检测到本地满足运行条件时，`cm_ctl start`命令下发便会启动该节点，使该节点加入集群。因此该类问题首先需要确定如下两点：

1. `CM Agent`是否有明确的启动行为。
2. `CM Server`主节点与其他`CM Agent`之间是否有明确的链接建立成功或中断行为。
通过上述思路，可以排查出因网络原因导致集群节点启动失败的绝大部分情况。

## 四、解决方案

对于该类型问题，如为系统网络故障，恢复网络设置并将故障节点手动拉起即可。
