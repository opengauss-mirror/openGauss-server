# 服务启动失败

## 问题现象<a name="section385257175213"></a>

服务启动失败。

## 原因分析<a name="section16300111295211"></a>

- 配置参数不合理，数据库因系统资源不足，或者配置参数不满足内部约束，启动失败。
- 由于部分数据节点状态不正常，导致数据库启动失败。
- 目录权限不够。例如对/tmp目录、数据库数据目录的权限不足。
- 配置的端口已经被占用。
- 开启了系统防火墙导致数据库启动失败。
- 组成数据库的各台机器之间需要正确建立互信关系，在互信关系出现异常的情况下，数据库将无法启动。
- 数据库控制文件损坏。

## 处理办法<a name="section7637151695218"></a>

- 确认是否由于参数配置不合理导致系统资源不足或不满足内部约束启动失败。

    - 登录启动失败的节点，检查运行日志确认是否因资源不足启动失败或配置参数不满足内部约束。例如出现Out of memory的错误或如下错误提示均为资源不足或配置参数不满足内部约束导致的启动失败。

```
FATAL: hot standby is not possible because max_connections = 10 is a lower setting than on the master server (its value was 100)
```

- 检查GUC参数配置的合理性。例如，shared\_buffers、effective\_cache\_size、bulk\_write\_ring\_size等消耗资源过大的参数；或max\_connections等增加后不容易减少的参数。

- 确认是否由于实例状态不正常，导致数据库启动失败。通过gs\_om -t status --detail工具，查询当前数据库各主备机实例的状态。

    - 如果某一节点上的所有实例都异常，请进行主机替换。

    - 如果发现某一实例状态为Unknown、Pending和Down的状态，则以数据库用户登录到状态不正常的实例所在节点，查看该实例的日志检查状态异常的原因。例如：

        ```
        2014-11-27 14:10:07.022 CST 140720185366288 FATAL:  database "postgres" does not exist 2014-11-27 14:10:07.022 CST 140720185366288 DETAIL:  The database subdirectory "base/ 13252" is missing.
        ```

        如果日志中出现上面这种报错信息，则说明该数据节点的数据目录文件遭到破坏，该实例无法执行正常查询，需要进行替换实例操作。

- 目录权限不够处理办法。例如，对/tmp目录、数据库数据目录的权限不足。

    - 根据错误提示，确认权限不足的目录名称。

    - 使用chmod命令修改目录权限使其满足要求。对于/tmp目录，数据库用户需要具有读写权限。对于数据库数据目录，请参考权限无问题的同类目录进行设置。

- 确认是否由于配置的端口已经被占用，导致数据库启动失败。

    - 登录启动失败的节点，查看实例进程是否存在。

    - 如果实例进程不存在，则可以通过查看该实例的日志来检查启动异常的原因。例如：

        ```
        2014-10-17 19:38:23.637 CST 139875904172320 LOG:  could not bind IPv4 socket at the 0 time: Address already in use 2014-10-17 19:38:23.637 CST 139875904172320 HINT:  Is another postmaster already running on port 40005? If not, wait a few seconds and retry.
        ```

        如果日志中出现上面这种报错信息，则说明该数据节点的TCP端口已经被占用，该实例无法正常启动。

        ```
        2015-06-10 10:01:50 CST 140329975478400 [SCTP MODE] WARNING: (sctp bind)         bind(socket=9, [addr:0.0.0.0,port:1024]):Address already in use  --  attempt 10/10 2015-06-10 10:01:50 CST 140329975478400 [SCTP MODE] ERROR: (sctp bind)   Maximum bind() attempts. Die now...
        ```

        如果日志中出现上面这种报错信息，则说明该数据节点的SCTP端口已经被占用，该实例无法正常启动。

- 通过sysctl -a查看net.ipv4.ip\_local\_port\_range，如果该实例配置的端口在系统随机占用端口号的范围内，则可以修改系统随机占用端口号的范围，确保xml文件中所有实例端口号均不在这个范围内。检查某个端口是否被占用的命令如下。

    ```
    netstat -anop | grep 端口号
    ```

    示例如下。

    ```
    [root@openGauss36 ~]# netstat -anop | grep 15970
    tcp        0      0 127.0.0.1:15970         0.0.0.0:*               LISTEN      3920251/gaussdb      off (0.00/0/0)
    tcp6       0      0 ::1:15970               :::*                    LISTEN      3920251/gaussdb      off (0.00/0/0)
    unix  2      [ ACC ]     STREAM     LISTENING     197399441 3920251/gaussdb      /tmp/.s.PGSQL.15970
    unix  3      [ ]         STREAM     CONNECTED     197461142 3920251/gaussdb      /tmp/.s.PGSQL.15970
    ```

- 确认是否是由于开启了系统防火墙导致数据库启动失败。
- 确认是否由于互信关系出现异常，导致数据库无法启动。重新配置实例中各台机器的互信关系解决此问题。
- 确认是否由于数据库控制文件如gaussdb.state损坏或文件被清空，导致数据库无法启动。若主机控制文件损坏，可触发备机failover，然后通过重建恢复原主机；若备机控制文件损坏，可直接通过重建方式恢复备机。
