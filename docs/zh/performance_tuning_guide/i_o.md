# I/O

通过iostat、pidstat命令或openGauss健康检查工具查看openGauss内节点I/O繁忙度和吞吐量，分析是否存在由于I/O导致的性能瓶颈。

## 查看I/O状况<a name="zh-cn_topic_0237121488_zh-cn_topic_0073253548_zh-cn_topic_0040046485_section49799026113827"></a>

查询服务器I/O的方法主要有以下三种方式：

- 使用iostat命令查看I/O情况。此命令主要关注单个硬盘的I/O使用率和每秒读取、写入的数量。

    ```
    iostat -xm 1  //1为间隔时间
    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
    sdc               0.01   519.62    2.35   44.10     0.31     2.17   109.66     0.68   14.62    2.80   15.25   0.31   1.42
    sdb               0.01   515.95    5.84   44.78     0.89     2.16   123.51     0.72   14.19    1.55   15.84   0.31   1.55
    sdd               0.02   519.93    2.36   43.91     0.32     2.17   110.16     0.65   14.12    2.58   14.74   0.30   1.38
    sde               0.02   520.26    2.34   45.17     0.31     2.18   107.46     0.80   16.86    2.92   17.58   0.34   1.63
    sda              12.07    15.72    3.97    5.01     0.07     0.08    34.11     0.28   30.64   10.11   46.92   0.98   0.88
    ```

    “rMB/s”为每秒读取的MB数，“wMB/s”为每秒写入的MB数，“%util”为硬盘使用率。

- 使用pidstat命令查看I/O情况。此命令主要关注单个进程每秒读取、写入的数量。

    ```
    pidstat -d 1 10  //1为采样间隔时间，10为采样次数
    03:17:12 PM   UID       PID   kB_rd/s   kB_wr/s kB_ccwr/s  Command
    03:17:13 PM  1006     36134      0.00  59436.00      0.00  gaussdb
    
    ```

    “kB\_rd/s”为每秒读取的kB数，“kB\_wr/s”为每秒写入的kB数。

- 使用gs\_checkperf工具对openGauss进行性能检查，需要以omm用户登录。

    ```
    gs_checkperf
    Cluster statistics information:
        Host CPU busy time ratio                     :    .69        %
        MPPDB CPU time % in busy time                :    .35        %
        Shared Buffer Hit ratio                      :    99.92      %
        In-memory sort ratio                         :    100.00     %
        Physical Reads                               :    8581
        Physical Writes                              :    2603
        DB size                                      :    281        MB
        Total Physical writes                        :    1944
        Active SQL count                             :    3
        Session count                                :    11
    ```

    显示结果包括每个节点的I/O使用情况，物理读写次数。

    也可以使用gs\_checkperf --detail命令查询每个节点的详细性能信息。

## 性能参数分析<a name="zh-cn_topic_0237121488_zh-cn_topic_0073253548_zh-cn_topic_0040046485_section401001449238"></a>

1. 检查磁盘空间使用率，建议不要超过60%。

    ```
    df -T
    ```

2. 若I/O持续过高，建议尝试以下方式降低I/O。
    - 降低并发数。
    - 对查询相关表做VACUUM FULL。

        ```
        vacuum full tablename;
        ```

        >[!NOTE]说明  
        >建议用户在系统空闲时进行VACUUM FULL操作，VACUUM FULL操作会造成短时间内I/O负载重，反而不利于降低I/O。  
