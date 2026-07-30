# 分析查询语句长时间运行的问题

## 问题现象<a name="section262711486472"></a>

系统中部分查询语句运行时间过长。

## 原因分析<a name="section101846244812"></a>

- 查询语句较为复杂，需要长时间运行。

- 查询语句阻塞。

## 处理办法<a name="section954884820"></a>

1. 以操作系统用户omm登录主机。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000
    ```

    postgres为需要连接的数据库名称，8000为端口号。

3. 查看系统中长时间运行的查询语句。

   ```
   SELECT timestampdiff(minutes, query_start, current_timestamp) AS runtime, datname, usename, query FROM pg_stat_activity WHERE state != 'idle' ORDER BY 1 desc;
   ```

   > 注意：该函数仅在openGauss兼容MY类型时（即dbcompatibility = 'B'）有效，其他类型不支持该函数。

   查询会返回按执行时间长短从大到小排列的查询语句列表。第一条结果就是当前系统中执行时间长的查询语句。

   如果当前系统较为繁忙，可以使用[TIMESTAMPDIFF](https://docs.opengauss.org/zh/docs/latest/sql_reference/date_and_time_processing_functions_and_operators.html#zh-cn_topic_0283136846_section5629194495516)函数通过限制current\_timestamp和query\_start大于某一阈值查看执行时间超过此阈值的查询语句。timestampdiff的第一个参数为时间差单位。例如，执行超过2分钟的查询语句可以通过如下语句查询。

   ```
   SELECT query FROM pg_stat_activity WHERE timestampdiff(minutes, query_start, current_timestamp) > 2;
   ```

4. 分析长时间运行的查询语句状态。
    - 如果查询语句处于正常状态，则等待其执行完毕。
    - 如果查询语句阻塞，请参见[分析查询语句是否被阻塞](analyzing_whether_a_query_statement_is_blocked.md)处理。
