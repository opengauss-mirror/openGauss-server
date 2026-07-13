# 因本地盘空间不足导致节点进入ReadOnly状态的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，在`ss_enable_check_sys_disk_usage`参数开启条件下可能出现以下状况：

1. 使用`cm_ctl query -Cvipdw`查询集群状态，发现某个节点状态为`ReadOnly`状态，其他节点为`Normal`状态。
2. 在主节点正常执行写业务流程中，出现如下错误：

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

上述状态说明某个节点进入`ReadOnly`只读状态，且当主节点进入该状态时，无法进行写业务。

## 二、定位方法

该问题的表现与定位流程如下所示：

1. 在对集群主节点进行写业务的时候，发现业务报错：

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

    报错原因为无法对只读事务进行写操作。相应的，可在集群主节点在`$GAUSSLOG/pg_log`目录下，寻找最近时间点的日志文件，可发现如下记录：

    ```shell
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] ERROR:  cannot execute INSERT in a read-only transaction
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] CONTEXT:  SQL statement "INSERT INTO user_logs (user_data)
    ```

    同样说明了详细报错原因，为主节点只读事务无法进行写操作。
2. 使用`cm_ctl query -Cvipdw`查询集群状态，发现某个节点状态为`ReadOnly`状态，即使该状况不影响备节点读业务。
3. 进入集群`CM Server`主节点的`$GAUSSLOG/cm/cm_server`目录下，寻找最近时间点的日志，可看到如下记录：

    ```shell
    2024-10-09 02:39:39.063 tid=1286737 StorageDetect LOG: [PreAlarmForNodeThreshold] [logDisk usage] Pre Alarm threshold reached, node=1, log_disk_usage=93.
    2024-10-09 02:39:39.064 tid=1286737 StorageDetect LOG: [PreAlarmForNodeThreshold] [dataDisk usage] Pre Alarm threshold reached, instanceId=6001,disk_usage=93, shared_disk_usage_for_data=1, shared_disk_usage_for_log=1.
    2024-10-09 02:39:39.064 tid=1286737 StorageDetect LOG: [ReadOnlyActDoNoting] instance 6001 is transaction read only, disk_usage:93,shared_disk_usage_for_data:1, shared_disk_usage_for_log:1, read_only_threshold:85
    ```

    可以看到，`CM server`已检测到本地空间不足，需要进行清理。
4. 同时，使用`df -h`查看只读节点的使用情况：

    ```shell
    Filesystem                  Size  Used Avail Use% Mounted on
    devtmpfs                    383G     0  383G   0% /dev
    tmpfs                       383G  1.1M  383G   1% /dev/shm
    tmpfs                       383G   75M  383G   1% /run
    tmpfs                       383G     0  383G   0% /sys/fs/cgroup
    /dev/mapper/openeuler-root   49G   31G   17G  65% /
    tmpfs                       383G  1.0M  383G   1% /tmp
    /dev/sda2                   976M  108M  801M  12% /boot
    /dev/sda1                   200M  5.8M  195M   3% /boot/efi
    /dev/mapper/openeuler-home  3.6T  818G  2.6T  24% /home
    tmpfs                        77G     0   77G   0% /run/user/1243
    tmpfs                        77G     0   77G   0% /run/user/0
    /dev/nvme0n1                3.0T  2.8T  183G  94% /usr1
    ```

    本实例中的`$PGDATA`目录在`/usr1`目录下，可以看到该目录占用率达到94%状态，导致该节点进入`Read Only`状态。

## 三、问题根因

对于各个节点而言，需保本地盘使用率低于默认阈值`85%`，才可以保证写业务正常进行。因此当本地盘存储容量低于`15%`时，便会使该节点进入`Read Only`状态，切当该节点为主节点时，无法进行写业务。

## 四、解决方案

对于该问题，有如下几种解决措施：

1. 将本地盘进行清理，清理不必要的文件。
2. 使用`cm_ctl`重新设置`ss_enable_check_sys_disk_usage`参数，关闭集群的本地盘容量检测，避免集群重某个节点因本地盘容量不足进入`Read Only`状态。

    ```shell
    cm_ctl set --param --server -k ss_enable_check_sys_disk_usage=0
    cm_ctl reload --param --server
    ```

3. 使用`cm_ctl`命令重新设置`datastorage_threshold_value_check`参数大小，增大容量检测阈值：

    ```shell
    cm_ctl set --param --server -k datastorage_threshold_value_check=xx
    cm_ctl reload --param --server
    ```

    通过上述命令设置完容量检测阈值后，可重新让该节点进入正常状态。
