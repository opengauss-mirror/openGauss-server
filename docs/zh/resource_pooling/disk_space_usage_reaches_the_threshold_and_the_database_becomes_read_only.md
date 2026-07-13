# 磁盘空间达到阈值，数据库只读

## 问题现象<a name="section19815185425919"></a>

执行非只读SQL时报错如下。

```
ERROR: cannot execute %s in a read-only transaction.
```

或者运行中部分非只读SQL（insert、update、create table as、create index、alter table及copy from等）时报错。

```
canceling statement due to default_transaction_read_only is on.
```

## 原因分析<a name="section192473272047"></a>

磁盘空间达到阈值后，设置数据库只读，只允许只读语句执行。

## 处理办法<a name="section17713758135913"></a>

1. 使用maintenance模式连接数据库，以下两种方法均可。
    - 方式一

        ```
        gsql -d postgres -p 8000 -r -m
        ```

    - 方式二

        ```
        gsql -d postgres -p 8000 -r
        ```

        连接成功后，执行如下命令：

        ```
        set xc_maintenance_mode=on;
        ```

2. 使用DROP/TRUNCATE语句删除当前不再使用的用户表，直至磁盘空间使用率小于设定的阈值。

    删除用户表只能暂时缓解磁盘空间不足的问题，建议尽早通过扩容解决磁盘空间不足的问题。

3. 使用系统用户omm设置数据库只读模式关闭。

    ```
    gs_guc reload -D /gaussdb/data/dbnode -c "default_transaction_read_only=off"
    ```
