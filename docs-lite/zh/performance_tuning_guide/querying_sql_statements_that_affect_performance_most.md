# 查询最耗性能的SQL<a name="ZH-CN_TOPIC_0289900933"></a>

系统中有些SQL语句运行了很长时间还没有结束，这些语句会消耗很多的系统性能，请根据本章内容查询长时间运行的SQL语句。

## 操作步骤<a name="zh-cn_topic_0283136757_zh-cn_topic_0237121490_zh-cn_topic_0073253542_zh-cn_topic_0040046535_section43790015111840"></a>

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

3. 查询系统中长时间运行的查询语句。

    ```
    SELECT current_timestamp - query_start AS runtime, datname, usename, query FROM pg_stat_activity where state != 'idle' ORDER BY 1 desc;
    ```

    查询后会按执行时间从长到短顺序返回查询语句列表，第一条结果就是当前系统中执行时间最长的查询语句。返回结果中包含了系统调用的SQL语句和用户执行SQL语句，请根据实际找到用户执行时间长的语句。

    若当前系统较为繁忙，可以通过限制current\_timestamp - query\_start大于某一阈值来查看执行时间超过此阈值的查询语句。

    ```
    SELECT query FROM pg_stat_activity WHERE current_timestamp - query_start > interval '1 days';
    ```

4. 设置参数track\_activities为on。

    ```
    SET track_activities = on;
    ```

    当此参数为on时，数据库系统才会收集当前活动查询的运行信息。

5. 查看正在运行的查询语句。

    以查看视图pg\_stat\_activity为例：

    ```
    SELECT datname, usename, state FROM pg_stat_activity;
     datname  | usename | state  |
    ----------+---------+--------+
     postgres |   omm   | idle   |
     postgres |   omm   | active |
    (2 rows)
    ```

    如果state字段显示为idle，则表明此连接处于空闲，等待用户输入命令。

    如果仅需要查看非空闲的查询语句，则使用如下命令查看：

    ```
    SELECT datname, usename, state FROM pg_stat_activity WHERE state != 'idle';
    ```

6. 分析长时间运行的查询语句状态。
    - 若查询语句处于正常状态，则等待其执行完毕。
    - 若查询语句阻塞，则通过如下命令查看当前处于阻塞状态的查询语句：

        ```
        SELECT datname, usename, state, query FROM pg_stat_activity WHERE waiting = true;
        ```

        查询结果中包含了当前被阻塞的查询语句，该查询语句所请求的锁资源可能被其他会话持有，正在等待持有会话释放锁资源。

        >[!NOTE]说明
        >
        >只有当查询阻塞在系统内部锁资源时，waiting字段才显示为true。尽管等待锁资源是数据库系统最常见的阻塞行为，但是在某些场景下查询也会阻塞在等待其他系统资源上，例如写文件、定时器等。但是这种情况的查询阻塞，不会在视图pg\_stat\_activity中体现。
