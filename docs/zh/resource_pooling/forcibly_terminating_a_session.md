# 强制结束指定的问题会话

## 问题现象<a name="section14515143811220"></a>

有些情况下，为了使系统继续提供服务，管理员需要强制结束有问题的会话。

## 处理办法<a name="section1678174320128"></a>

1. 以操作系统用户omm登录主机。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000
    ```

    postgres为需要连接的数据库名称，8000为端口号。

3. 从当前活动会话视图查找问题会话的线程ID。

    ```
    SELECT datid, pid, state, query FROM pg_stat_activity; 
    ```

    显示类似如下信息，其中pid的值即为该会话的线程ID。

    ```
    datid |       pid       | state  | query 
    -------+-----------------+--------+------ 
    13205 | 139834762094352 | active | 
    13205 | 139834759993104 | idle   | 
    (2 rows) 
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
