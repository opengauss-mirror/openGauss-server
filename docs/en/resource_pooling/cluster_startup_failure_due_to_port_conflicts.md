# Cluster Startup Failure Due to Port Conflicts

## Symptom

This section describes the issue of cluster startup or restart failure caused by port conflicts, using a one-primary-one-standby resource pool deployment as an example.

The primary node IP is `aaa.aaa.aaa.aaa`, the standby node IP is `bbb.bbb.bbb.bbb`, and both nodes use port number `xxx`.

1. The openGauss resource pool deployment cluster starts or restarts successfully, but executing `cm_ctl query -Cvipd` reveals that the `state` of a specific node within the `CMServer State` is `Down`.

2. The openGauss resource pool deployment cluster fails to start or restart, reporting the following error message:

    ```shell
    cm_ctl: start cluster failed in (601)s!

    HINT: Maybe the cluster is continually being started in the background.
    You can wait for a while and check whether the cluster starts, or increase the value of parameter "-t", e.g -t 600.
    ```

    The report indicates cluster startup failure with a 10-minute timeout.

## Troubleshooting Method

**For Symptom 1:**

1. Log in to the affected node machine and navigate to the `$GAUSSLOG/cm/cm_agent` directory. Inspect the most recent `cm_agent` log file for the following error message:

    ```shell
    2024-09-29 16:52:33.198 tid=3554807 StartAndStop WARNING: port:xxx already in use.
    ```

    The aforementioned log confirms that port `xxx` is already in use.

2. Inspect the `$GAUSSLOG/cm/cm_ctl` log file for the following information:

    ```shell
    2024-09-29 16:37:21.420 tid=3554057  DEBUG1: begin to create ssl connection

    2024-09-29 16:37:21.437 tid=3554057  DEBUG1: connect to cmserver success, remotehost is bbb.bbb.bbb.bbb:xxx.
    ```

    The aforementioned logs confirm that the connection to the cmserver was successful, with the connected IP address being `bbb.bbb.bbb.bbb` and the port number being `xxx`. Additionally, it can be observed that there is no log entry indicating a successful connection to the primary node's cmserver.

Through the above method, it can be confirmed that the `Down` state of a specific node within the `CMServer State` is caused by a CM port conflict on that node.

**For Symptom 2:**

After the `cm_ctl start` command returns the aforementioned error message, the following scenarios may be encountered:

1. Executing `cm_ctl query -Cvipd` to query the cluster status results in a direct error, with no query information displayed. This indicates that the CM ports on all node machines are in conflict.

2. First, execute `cm_ctl query -Cvipd` to query the cluster status. The `CMServer State` of all nodes appears `Normal`, but the `Datanode State` of all nodes (or a specific node) is not `Normal`.

    Subsequently, log in to the affected node machine and navigate to the `$GAUSSLOG/cm/cm_ctl` directory. Inspect the most recent `cm_ctl1` log file for the following error message:

    ```shell
    DEBUG1: starting exceeds 2 mins, instance status:g_cluster_start_status=3,g_az_start_status=5, g_node_start_status=5, g_instance_start_status=5
    ```

    This indicates that the database port on some or all node machines is experiencing a conflict.

## Root Cause

During the startup of each component, specific ports are required for socket communication. If all node CM ports are occupied, the cluster fails to establish connections during the startup or restart process, resulting in CM startup failure and cluster status query errors. If only some node CM ports are occupied, the cluster starts or restarts successfully, but the corresponding node's cmserver state is `Down` while the database state remains `Normal`. If all or some node database ports are occupied, the cluster fails to establish connections during the startup or restart process, resulting in cluster startup or restart failure; in this case, all node database states are not `Normal`, but the cmserver states are all `Normal`.

Therefore, during the database startup or restart process, ensure that ports are used exclusively and do not conflict with other processes or default system ports.

## Solution

After the startup or restart process completes (regardless of success), use the `lsof -i:port_number` command or other equivalent utilities to identify the process occupying the conflicting port. Terminate that process to prevent startup or restart failure. After termination, wait for a brief period and query the cluster status again. If the cluster has not yet returned to normal, restart the cluster.

```csv
原文, 译文, 辨析, 翻译参考出处
端口冲突, Port Conflicts, 网络通信基础故障类型，指多个进程争用同一端口，标准术语, IANA Port Number Registry
资源池化, Resource Pool Deployment, openGauss 存算分离/共享存储架构的专有称呼，标准术语, openGauss 官方文档
一主一备, One-Primary-One-Standby, openGauss 最小高可用部署架构，标准术语, openGauss 官方文档
cm_ctl, cm_ctl, openGauss 集群管理命令行工具，专有名词保留, https://docs.opengauss.org/
CMServer State, CMServer State, CM 服务端进程状态标识，专有名词保留, openGauss CM Reference
Datanode State, Datanode State, 数据库节点状态标识，专有名词保留, openGauss CM Reference
Down, Down, 集群节点离线状态标识，专有名词保留, openGauss CM Reference
Normal, Normal, 集群节点正常状态标识，专有名词保留, openGauss CM Reference
$GAUSSLOG, $GAUSSLOG, openGauss 日志根目录环境变量，专有名词保留, openGauss 官方文档
cm_agent, cm_agent, openGauss 集群管理代理进程，专有名词保留, https://docs.opengauss.org/
cm_ctl1, cm_ctl1, cm_ctl 启动阶段日志文件，专有名词保留, openGauss 官方文档
already in use, already in use, 端口占用标准错误提示，原样保留, Linux errno.h
ssl connection, SSL Connection, 安全套接字连接，标准术语，保留小写, RFC 5246
socket通信, Socket Communication, 网络通信基础机制，标准术语, POSIX Standard
lsof, lsof, Linux 列出打开文件/端口的命令行工具，专有名词保留, Linux man-pages
端口号, Port Number, 网络通信端口编号，标准术语, IANA Port Number Registry
默认系统端口, Default System Ports, 指操作系统预留端口（如 0-1023），标准术语, IANA Port Number Registry
杀掉进程, Terminate the Process, 运维操作标准术语，对应 kill 命令, Linux man-pages
```
