# 因磁阵hang住导致集群业务卡住的问题

## 问题现象

资源池化性能机异常，集群业务卡住。

## 定位方法

1. 查看pg_stat_activity视图，查看业务卡住的基本信息，如线程号、SQL、查询开始时间等。

    ```shell
    openGauss=# select * from pg_stat_activity;
    -[ RECORD 1 ]----+------------------------------------------------------------------------------------------------------------------------------------
    datid            | 15199
    datname          | postgres
    pid              | 281450315045296
    sessionid        | 281450315045296
    usesysid         | 10
    usename          | carrot
    application_name | gsql
    client_addr      |
    client_hostname  |
    client_port      | -1
    backend_start    | 2024-10-12 10:17:01.195964+08
    xact_start       | 2024-10-12 10:17:02.421981+08
    query_start      | 2024-10-12 10:17:02.421981+08
    state_change     | 2024-10-12 10:17:02.421988+08
    waiting          | f
    enqueue          |
    state            | active
    resource_pool    | default_pool
    query_id         | 562949953434164
    query            | insert into a select * from a;
    connection_info  | {"driver_name":"libpq","driver_version":"(openGauss 6.0.0 build d1f02e70) compiled at 2024-10-11 10:39:30 commit 0 last mr  debug"}
    unique_sql_id    | 3750366782
    ....
    ```

2. 查看pg_thread_wait_status，查看卡死的业务的等待信息，均在等待IO。

    ```shell
    openGauss=# select * from pg_thread_wait_status;
    node_name | db_name  |      thread_name       |    query_id     |       tid       |    sessionid    |  lwtid  | psessionid | tlevel | smpid | wait_status |  wait_event   | locktag | lockmode | block_sessionid |
    global_sessionid
    -----------+----------+------------------------+-----------------+-----------------+-----------------+---------+------------+--------+-------+-------------+---------------+---------+----------+-----------------+
    ------------------
    dn1       |          | PageWriter             |               0 | 281451104491952 | 281451104491952 | 2956573 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       |          | PageWriter             |               0 | 281451123431856 | 281451123431856 | 2956572 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       |          | PageWriter             |               0 | 281451140274608 | 281451140274608 | 2956571 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       |          | PageWriter             |               0 | 281451177892272 | 281451177892272 | 2956569 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       |          | PageWriter             |               0 | 281451161049520 | 281451161049520 | 2956570 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       |          | undo recycler          |               0 | 281450350696880 | 281450350696880 | 2980833 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       |          | LWLock Monitor         |               0 | 281450779564464 | 281450779564464 | 2956800 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       |          | TwoPhase Cleaner       |               0 | 281450800470448 | 281450800470448 | 2956799 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       |          | Wal Writer Auxiliary   |               0 | 281450963589552 | 281450963589552 | 2956790 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       |          | Wal Writer             |               0 | 281451266628016 | 281451266628016 | 2956789 |            |      0 |     0 | wait io     | WalWrite      |         |          |                 |
    0:0#0
    dn1       |          | CheckPointer           |               0 | 281451211577776 | 281451211577776 | 2956567 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       |          | InvalidBufferBgWriter  |               0 | 281451194735024 | 281451194735024 | 2956568 |            |      0 |     0 | none        | none          |         |          |                 |
    0:0#0
    dn1       | postgres | gsql                   | 562949953434190 | 281450315045296 | 281450315045296 |  680205 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    dn1       | postgres | gsql                   | 562949953434195 | 281450080098736 | 281450080098736 |  671968 |            |      0 |     0 | wait io     | DataFileWrite |         |          |                 |
    0:0#0
    ```

3. 已确认为存储系统问题，登录磁阵平台，查看磁阵网络状态、IO状态、日志等，进一步确认磁阵故障。

## 问题根因

磁阵网络异常或功能异常。

## 解决方案

若磁阵网络异常，则恢复网络。

若磁阵功能异常，则需要按照磁阵故障手册继续排查。
