# Binding CPU Cores for the Database Server and Client<a name="EN-US_TOPIC_0289900458"></a>

1. Install the openGauss database. For details, see  _openGauss Installation Guide_.
2. Stop the database. For details, see section "Starting and Stopping openGauss" in  _openGauss Administrator Guide_.
3. Use the gs\_guc tool to modify the database port and IP address. For details about how to use the gs\_guc tool, see section "Server Tools \> gs\_guc" in  _openGauss Tool Reference_.
4. Use the gs\_guc tool to set the following parameters.

    ```
    advance_xlog_file_num = 100
    numa_distribute_mode = 'all'
    numa_distribute_mode = 'all'
    thread_pool_attr = '812,4,(cpubind:0-27,32-59,64-91,96-123)'
    thread_pool_attr = '464,4,(cpubind:1-27,32-59,64-91,96-123)'
    xloginsert_locks = 16
    wal_writer_cpu=0
    wal_file_init_num = 20
    xlog_idle_flushes_before_sleep = 500000000
    pagewriter_sleep = 10ms
    ```

5. Run the following command to start the database on the server in core binding mode.

    ```
    numactl -C 1-27,32-59,64-91,96-123 gaussdb --single_node -D /data1/gaussdata  -p 3625 &
    ```

    Core  **0**  is used for wal\_writer, and  **1-27,32-59,64-91,96-123**  indicates that 111 cores are used to run the TPC-C program and the other 16 cores are used to process the network interruption of the server.

6. Run the following command to bind the 48 cores of the client CPU to the NIC interrupt queue.

    ```
    sh bind_net_irq.sh  48
    ```
