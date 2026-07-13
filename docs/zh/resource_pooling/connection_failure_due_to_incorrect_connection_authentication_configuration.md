
# 因连接认证配置错误导致连接失败的问题

## 问题现象

连接数据库报错。

```shell
Exception in thread "main" org.postgresql.util.PSQLException: [A.B.C.D:40822/A.B.C.D:20088] FATAL: Forbid remote connection with trust method!
        at org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:636)
        at org.postgresql.core.v3.ConnectionFactoryImpl.tryConnect(ConnectionFactoryImpl.java:172)
        at org.postgresql.core.v3.ConnectionFactoryImpl.openConnectionImpl(ConnectionFactoryImpl.java:254)
        at org.postgresql.core.ConnectionFactory.openConnection(ConnectionFactory.java:53)
        at org.postgresql.jdbc.PgConnection.<init>(PgConnection.java:270)
        at org.postgresql.Driver.makeConnection(Driver.java:568)
        at org.postgresql.Driver.connect(Driver.java:317)
        at java.sql.DriverManager.getConnection(DriverManager.java:664)
        at java.sql.DriverManager.getConnection(DriverManager.java:247)
        at Main.run(Main.java:88)
        at Main.main(Main.java:25)
```

## 定位方法

报错信息内容为向监控地址`A.B.C.D`发起连接由于认证为trust模式被禁止。

数据库连接认证配置文件为 `pg_hba.conf`，查看远程登录认证配置：

```shell
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
host    all             all             A.B.C.D/32              trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
```

确认确实使用了`trust`的远程登录认证模式。

## 问题根因

数据库不支持以`trust`模式远程登录。

## 解决方案

数据库不支持以`trust`模式远程登录，需要修改为`sha256`、`md5`等。

```shell
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
host    all             all             A.B.C.D/32              sha256
# IPv6 local connections:
host    all             all             ::1/128                 trust
```
