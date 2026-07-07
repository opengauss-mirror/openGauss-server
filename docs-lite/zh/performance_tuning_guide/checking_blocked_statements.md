# 分析作业是否被阻塞<a name="ZH-CN_TOPIC_0289900192"></a>

数据库系统运行时，在某些业务场景下查询语句会被阻塞，导致语句运行时间过长，可以强制结束有问题的会话。

## 操作步骤<a name="zh-cn_topic_0283137653_zh-cn_topic_0237121491_zh-cn_topic_0073253543_zh-cn_topic_0040046536_section19654526113952"></a>

1. 以操作系统用户omm登录数据库节点。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000
    ```

    postgres为需要连接的数据库名称，8000为数据库节点的端口号。

    连接成功后，系统显示类似如下信息：

    ```
    gsql((openGauss x.x.x build f521c606) compiled at 2021-09-16 14:55:22 commit 2935 last mr 6385 release)
    Non-SSL connection (SSL connection is recommended when requiring high-security)
    Type "help" for help.
    
    openGauss=# 
    ```

3. 查看阻塞的查询语句及阻塞查询的表、模式信息。

    ```
    SELECT w.query as waiting_query,
    w.pid as w_pid,
    w.usename as w_user,
    l.query as locking_query,
    l.pid as l_pid,
    l.usename as l_user,
    t.schemaname || '.' || t.relname as tablename
    from pg_stat_activity w join pg_locks l1 on w.pid = l1.pid
    and not l1.granted join pg_locks l2 on l1.relation = l2.relation
    and l2.granted join pg_stat_activity l on l2.pid = l.pid join pg_stat_user_tables t on l1.relation = t.relid
    where w.waiting;
    ```

    该查询返回线程ID、用户信息、查询状态，以及导致阻塞的表、模式信息。

4. 使用如下命令结束相应的会话。其中，139834762094352为线程ID。

    ```
    SELECT PG_TERMINATE_BACKEND(139834762094352);
    ```

    显示类似如下信息，表示结束会话成功。

    ```
     PG_TERMINATE_BACKEND
    ----------------------
     t
    (1 row)
    ```

    显示类似如下信息，表示用户正在尝试结束当前会话，此时仅会重连会话，而不是结束会话。

    ```
    FATAL:  terminating connection due to administrator command
    FATAL:  terminating connection due to administrator command
    The connection to the server was lost. Attempting reset: Succeeded.
    ```

    >[!NOTE]说明
    >
    >gsql客户端使用PG\_TERMINATE\_BACKEND函数终止本会话后台线程时，客户端不会退出而是自动重连。
