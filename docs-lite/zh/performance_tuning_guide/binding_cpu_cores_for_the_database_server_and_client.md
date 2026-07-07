# 数据库服务端及客户端绑核<a name="ZH-CN_TOPIC_0289900458"></a>

1. 安装openGauss数据库，具体操作请参考《安装指南》。
2. 停止数据库，具体操作请参考《数据库运维指南》中的“启停openGauss”章节。
3. 使用gs\_guc工具修改数据库端口、IP等，gs\_guc的使用请参考《工具与命令参考》中的“服务端工具\>gs\_guc”章节。
4. 使用gs\_guc工具设置如下参数。

    ```conf
    advance_xlog_file_num = 100
    numa_distribute_mode = 'all'
    thread_pool_attr = '464,4,(cpubind:1-27,32-59,64-91,96-123)'
    xloginsert_locks = 16
    walwriter_cpu_bind=0
    wal_file_init_num = 20
    walwriter_sleep_threshold = 50000
    pagewriter_sleep = 10ms
    ```

5. 执行如下命令以绑核方式启动服务端数据库。

    ```
    numactl -C 1-27,32-59,64-91,96-123 gaussdb --single_node -D /data1/gaussdata  -p 3625 &
    ```

    其中0核用于wal\_writer，1-27，32-59，64-91，96-123表示使用111个核运行TPCC程序，其余的16个核用来处理服务端的网络中断。

6. 执行如下命令，将客户端CPU的48个核与网卡中断队列进行绑定：

    ```
    sh bind_net_irq.sh  48
    ```
