# 因数据库最大连接数超限导致连接失败的问题

## 问题现象

连接出现报错：

```shell
2024-09-27 10:55:20 Exception: [127.0.0.1:34406/127.0.0.1:20088] FATAL: Too many clients already, current: 203, max_connections/reserved: 200/3.
        at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2,935)
        at org.postgresql.core.v3.QueryExecutorImpl.readStartupMessages(QueryExecutorImpl.java:3,065)
        at org.postgresql.core.v3.QueryExecutorImpl.<init>(QueryExecutorImpl.java:147)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:309)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:53)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:270)
        at org.postgresql.Driver.makeConnection(Driver.java:568)
        at org.postgresql.Driver.connect(Driver.java:317)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at ...
```

## 定位方法

根据报错内容可知，此问题为最大连接数超出限制。

数据库内关于最大连接数的GUC配置参数有 `max_connections`，`sysadmin_reserved_connections`，分别表示最大连接数、管理员预留连接数。

```shell
openGauss=# show max_connections ;
max_connections
-----------------
200
(1 row)

openGauss=# show sysadmin_reserved_connections;
sysadmin_reserved_connections
-------------------------------
3
(1 row)
```

可以看出连接数已满。

## 问题根因

最大连接数超出限制。

## 解决方案

按需修改参数`max_connections`，`sysadmin_reserved_connections`即可。
