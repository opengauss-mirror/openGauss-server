# btree 索引故障情况下应对策略

## 问题现象<a name="section14883333175911"></a>

偶发索引丢失错误，报错如下。

```
ERROR: index 'xxxx_index' contains unexpected zero page
或
ERROR: index 'pg_xxxx_index' contains unexpected zero page
或 
ERROR: compressed data is corrupt
```

## 原因分析<a name="section14246173814590"></a>

该类错误是因为索引发生故障导致的，可能引发故障的原因如下：

- 由于软件bug或者硬件原因导致的索引不再可用。
- 索引包含许多空的页面或者几乎为空的页面。
- 并发执行DDL过程中，发生了网络闪断故障。
- 创建并发索引时失败，遗留了一个失效的索引，这样的索引无法被使用。
- 执行DDL或者DML操作时，网络出现故障。

## 处理办法<a name="section237115426595"></a>

执行REINDEX命令进行索引重建。

1. 以操作系统用户omm登录主机。
2. 使用如下命令连接数据库。

    ```
    gsql -d postgres -p 8000 -r
    ```

3. 重建索引。
    - 如果进行DDL或DML操作时，因软硬件故障导致索引问题，请执行如下命令重建表索引。

        ```
        REINDEX TABLE tablename;
        ```

    - 如果错误中提示是xxxx\_index，其中xxxx代表用户表名。请执行如下命令之一重建表的索引。

        ```
        REINDEX INDEX indexname; 
        ```

        或者

        ```
        REINDEX TABLE tablename;
        ```

    - 如果错误中提示pg\_xxxx\_index，说明是系统表索引存在问题。请执行如下命令重建表索引。

```
REINDEX SYSTEM databasename;
```
