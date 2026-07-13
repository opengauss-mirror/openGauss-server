# 因环境中存在逻辑复制槽残留导致远程增量备份失败问题

## 一、问题现象

gs_probackup执行远程增量备份时报错。   

1. 新建备份目录并初始化：  
`gs_probackup init -B xxx`  
2. 在备份路径内初始化一个新的备份实例：  
`gs_probackup add-instance -B xxx --instance=instance2 -D xxx/cluster/dn1 --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/cluster/app/bin --remote-libpath=xxx/cluster/app/lib`
3. 远程全量备份：  
`gs_probackup backup -B xxx -b full --instance=instance2 -p xxx -d xxx -U xxx-W xxx --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/app/bin --remote-libpath=xxx/app/lib --backup-pg-replslot`
4. 远程增量备份：  
`gs_probackup backup -B xxx -b PTRACK --stream --instance=instance2 -p xxx -d xxx -U xxx-W xxx --remote-proto=ssh --remote-host=xxx --remote-port=22 --remote-user=xxx --remote-path=xxx/app/bin --remote-libpath=xxx/app/lib --backup-pg-replslot`  

* 报错信息如下所示：  

    ```shell
    LOG: Backup destination is initialized
    LOG: This openGauss instance was initialized with data block checksums. Data block corruption will be detected
    LOG: Start SSH client process, pid 2816081
    LOG: Database backup start
    WARNING: logical replication slots for subscriptions will be backed up. If dont use them after restoring, please drop them to avoid affecting xlog recycling.
    LOG: Latest valid FULL backup: RKZOTD
    INFO: Parent backup: RKZOTD
    LOG: started streaming WAL at 0/7000000 (timeline 1)
    [2022-11-07 17:25:41]: check identify system success
    [2022-11-07 17:25:41]: send START_REPLICATION Q/7000000 success
    [2022-11-07 17:25:41]: keepalive message is received
    LOG: SSH process 2816081 is terminated with status O
    INFO: PGDATA size: 644MB
    LOG: Current tli: 1
    LOG: Parent start_lsn: 0/776D5A8
    LOG: start lsn: 0/776D5A8
    INFO: Extracting pagemap of changed blocks
    INFO: change bitmap start lsn location is 0/776D5A8
    INFO: change bitmap end lsn location is 00000000/2E000028
    ERROR: query failed: ERROR: could not find valid CBM file that contains the merging start point 00000000/0776D5A8
    query was: SELECT path,changed_block number,changed_block_list FROM pg_cbm_get changed_block($1, $2)
    WARNING: backup in progress, stop backup
    INFO: wait for pg_stop backup()
    INFO: pg_stop backup() successfully executed
    WARNING: Backup RKZOUS is running, setting its status to ERROR
    ```

## 二、定位方法

1. 通过以下报错信息发现远程增量备份过程中无法找到有效的CBM文件，其查询路径和页面编号出错：

    ```shell
    ERROR: query failed: ERROR: could not find valid CBM file that contains the merging start point 00000000/0776D5A8
    query was: SELECT path,changed_block number,changed_block_list FROM pg_cbm_get changed_block($1, $2)
    ```

2. 因为增量备份通过start_lsn去查询cbm文件，再去执行操作，因此先去查询每个cbm文件的start_lsn。
通过使用以下命令可以发现，逻辑复制槽的存在残留，导致两个逻辑复制槽处于false状态。因此可以确定是逻辑复制槽中的start_lsn错误导致查询了错误的cbm文件：

    ```shell
    select * from pg_recvlogical ('table_slot','pgoutput');
    ```

    <table>
        <tr>
            <th>slot_name</th>
            <th>plugin</th>
            <th>slot_type</th>
            <th>datoid</th>
            <th>active</th>
            <th>catalog_xmin</th>
            <th>restart_lsn</th>
            <th>dummy_standby</th>
            <th>confirmed_flush</th>
        </tr>
        <tr>
            <td>pg_16420_synv_16394</td>
            <td>pgoutput</td>
            <td>logical</td>
            <td>16384</td>
            <td>t</td>
            <td>436010</td>
            <td>0/776D5AB</td>
            <td>false</td>
            <td>0/776D5AB</td>
        </tr>
                <tr>
            <td>pg_16420_synv_16389</td>
            <td>pgoutput</td>
            <td>logical</td>
            <td>16384</td>
            <td>t</td>
            <td>436010</td>
            <td>0/776D700</td>
            <td>false</td>
            <td>0/776D700</td>
        </tr>
    </table>

## 三、问题根因

原因是环境中存在发布订阅的逻辑复制槽的残留，两个逻辑复制槽处于false状态，因此他们的`restart_lsn`一直保持不变。在进行备份时，备份命令中如果指定：
    ```
--backup-pg-replslot
    ```
就会遍历所有的逻辑复制槽相关的信息，最后获得一个最小的`restart_lsn`作为备份的`start_lsn`，此时由于发布订阅的逻辑复制槽存在残留，因此每次获得的都是`0776D5A8`，而增量备份会根据`start_lsn`查询`cbm`文件，由于这个处于`false`状态的逻辑复制槽的`restart_lsn`不变且非常久远，所以在执行`pg_cbm_get_changed_block()`时会报如上错误，实际参数问题的原因是发布订阅的逻辑复制槽没有被删除。  

## 四、解决方案

1. 在清理逻辑复制槽里针对不同的结果返回不同的发布者的信号。  
2. 清理环境残留复制槽后远程增量备份成功。  
