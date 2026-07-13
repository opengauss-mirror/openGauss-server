# 分析查询语句运行状态

## 问题现象<a name="section4792154194213"></a>

系统中部分查询语句运行时间过长，需要分析查询语句的运行状态。

## 处理办法<a name="section1587514811424"></a>

1. 以操作系统用户omm登录主机。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000
    ```

    postgres为需要连接的数据库名称，8000为端口号。

3. 设置参数track\_activities为on。

    ```
    SET track_activities = on;
    ```

    当此参数为on时，数据库系统才会收集当前活动查询的运行信息。

4. 查看正在运行的查询语句。以查看视图pg\_stat\_activity为例。

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity; 
    datname  | usename | state  | query 
    ----------+---------+--------+-------
    postgres | omm     | idle   | 
    postgres | omm     | active | 
    (2 rows) 
    ```

    如果state字段显示为idle，则表明此连接处于空闲，等待用户输入命令。 如果仅需要查看非空闲的查询语句，则使用如下命令查看。

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity WHERE state != 'idle';
    ```

5. 分析查询语句为活跃状态还是阻塞状态。通过如下命令查看当前处于阻塞状态的查询语句。

    ```
    SELECT datname, usename, state, query FROM pg_stat_activity WHERE waiting = true;
    ```

    查询结果中包含了当前被阻塞的查询语句，该查询语句所请求的锁资源可能被其他会话持有，正在等待持有会话释放锁资源。
