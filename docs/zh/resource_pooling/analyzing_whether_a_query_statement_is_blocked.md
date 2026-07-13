# 分析查询语句是否被阻塞

## 问题现象<a name="section148995712711"></a>

数据库系统运行时，在某些业务场景下，查询语句会被阻塞，导致语句运行时间过长。

## 原因分析<a name="section1916631418712"></a>

查询语句需要通过加锁来保护其要访问的数据对象。当要进行加锁时发现要访问的数据对象已经被其他会话加锁，则查询语句会被阻塞，等待其他会话完成操作并释放锁资源。这些需要加锁访问的数据对象主要包括表、元组等。

## 处理办法<a name="section16731125079"></a>

1. 以操作系统用户omm登录主机。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000
    ```

    postgres为需要连接的数据库名称，8000为端口号。

3. 从当前活动会话视图查找问题会话的线程ID。

    ```
    SELECT w.query AS waiting_query, w.pid AS w_pid, w.usename AS w_user, l.query AS locking_query, l.pid AS l_pid, l.usename AS l_user, t.schemaname || '.' || t.relname AS tablename FROM pg_stat_activity w JOIN pg_locks l1 ON w.pid = l1.pid AND NOT l1.granted JOIN pg_locks l2 ON l1.relation = l2.relation AND l2.granted JOIN pg_stat_activity l ON l2.pid = l.pid JOIN pg_stat_user_tables t ON l1.relation = t.relid WHERE w.waiting = true;
    ```

4. 根据线程ID结束会话。

    ```
    SELECT pg_terminate_backend(139834762094352);
    ```

    显示类似如下信息，表示结束会话成功。

    ```
    pg_terminate_backend 
    ---------------------
    t
    (1 row)
    ```

    显示类似如下信息，表示用户正在尝试结束当前会话，此时仅会重连会话，而不是结束会话。

    ```
    FATAL:  terminating connection due to administrator command 
    FATAL:  terminating connection due to administrator command The connection to the server was lost. Attempting reset: Succeeded.
    ```
