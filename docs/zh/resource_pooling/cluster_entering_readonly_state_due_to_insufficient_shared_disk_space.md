# 因共享盘空间不足导致集群进入ReadOnly状态的问题

## 一、问题现象

在openGauss资源池化集群使用过程中，可能出现以下状况：

1. 使用`cm_ctl query -Cvipdw`查询集群状态，发现各个节点状态均为`Primary ReadOnly`或`Standby ReadOnly`。
2. 在正常执行业务流程中，出现如下错误：

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

上述状态说明集群进入`ReadOnly`只读状态，无法进行写业务。

## 二、定位方法

该问题的表现与定位流程如下所示：

1. 在对集群进行写业务的时候，发现业务报错，详情为：

    ```SQL
    ERROR:  cannot execute INSERT in a read-only transaction
    ```

    报错原因为无法对只读事务进行写操作，说明此时集群为只读状态。
2. 相应的，可以在`$GAUSSLOG/pg_log`目录下，寻找最近时间点的日志文件，可发现如下记录：

    ```shell
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] ERROR:  cannot execute INSERT in a read-only transaction
    2024-10-08 16:46:45.522 6704f169.5069 test 281439387299744 gsql 0 dn_6001_6002_6003 25006  562949953464622 [BACKEND] CONTEXT:  SQL statement "INSERT INTO user_logs (user_data)
    ```

    同样说明了详细报错原因，为只读事务无法进行写操作。
3. 使用`dsscmd lsvg -m G`指令，对共享盘容量进行查询，得到以下结果：

    ```shell
    vg_name       volume_count        size                 free                 used                 percent(%)          
    data          1                   30.00000             2.43750              27.56250             91.88               
    log           1                   50.00000             23.75000             26.25000             52.50   
    ```

    可以看到，共享盘`data`盘使用阈值达到了`91%`，达到`85%`默认告警阈值，从而导致集群进入`Read Only`状态。

## 三、问题根因

对于集群而言，默认情况下需保证共享盘使用率低于`85%`，才允许写业务正常进行。因此当共享盘存储容量低于`15%`时，便会使得集群进入`Read Only`状态，从而导致集群无法进行写业务。

## 四、解决方案

对于该问题，有如下几种解决措施：

1. 重新安装部署，使用更大的共享盘进行部署安装，避免容量耗尽问题。
2. 使用共享存储扩容命令对共享盘进行扩容：

    ```shell
    dsscmd adv <-g vg_name> <-v vol_path> [-f] [-D DSS_HOME] [-U UDS:socket_domain] 
    ```

    扩 容完成后，可解决该问题；
3. 使用`cm_ctl`命令重新设置`datastorage_threshold_value_check`参数大小，设置新的容量阈值：

    ```shell
    cm_ctl set --param --server -k datastorage_threshold_value_check=xx
    cm_ctl reload --param --server
    ```

    通过上述命令设置新的容量阈值后，可重新让集群进入正常状态。
