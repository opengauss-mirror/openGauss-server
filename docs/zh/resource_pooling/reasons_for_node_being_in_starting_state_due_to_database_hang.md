# 因数据库僵死导致节点处于Starting的原因

## 一、问题现象

在集群运行状态时，如果突然发现某节点处于`Starting`状态，并且发现其他资源实例都没有异常，此时需要进一步排查日志来查明是何种原因所导致的。

## 二、定位方法

1. 先利用`ps ux`命令查看数据库进程（gaussdb进程）是否存在，若存在但是DN还是有故障，则认为有可能是数据库僵死。

2. 进入$GAUSSLOG/cm/cm_agent日志，搜索关键字`phony dead`，出现类似如下日志:

    ```shell
    2024-10-10 16:03:54.868 tid=3762978  LOG: has found 1 times for instance(dn_6001) phony dead check.
    2024-10-10 16:03:55.019 tid=3762975  ERROR: failed to connect to datanode:/usr1/ertao/openGauss/cluster/dn1
    2024-10-10 16:03:55.019 tid=3762975  ERROR: connection return errmsg : wait 20.20.20.79:36964 timeout expired
    ```

    则认为数据库进入了僵死检测，如果发现僵死次数大于5次，则会杀死数据库。

3. 在cm_agent日志中，去查看为何会进入僵死检测，是由于数据库连接不上，出现`failed to connect to datanode`的字样，还是由于一些cm_agent对数据库检测的sql执行失败所致，出现类似于`sqlcommand failed`的字样，确认僵死以后需要通过数据库日志进一步排查。

## 三、问题根因

1. cm_agent会对数据库进行僵死检测，检测机制如下：
    cm_agent会从linux进程文件中读取进程状态`/proc/%d/status`，检测是否处于T，Z，D状态（D状态不会上报僵死，因为D状态只能重启服务器；Z状态是不可持续状态，系统会自动处理）。如果不是上述状态，就会连接数据库执行sql语句尝试连接数据库，如果连接失败，或者执行sql失败，都会认为数据库僵死。
2. cm_server处理数据库僵死机制如下：
    cm_server接收到cm_agent上报的数据库僵死状态，发现僵死次数大于等于phony_dead_effective_time（默认5次），开始进行僵死处理，立即向cm_agent下发强制杀死数据的命令，cm_agent杀死数据库后，cm_server进行标记，需要等待instance_phony_dead_restart_intervel（默认21600s），才能再次对该实例进行僵死处理。
