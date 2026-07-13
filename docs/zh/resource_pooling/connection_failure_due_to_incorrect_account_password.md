# 因账号密码错误导致连接失败的问题

## 问题现象

连接数据库报错：

```shell
Exception in thread "main" org.postgresql.util.PSQLException: [A.B.C.D:40822/A.B.C.D:20088] FATAL: Invalid username/password,login denied.
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

根据报错可以明确知道问题为：账户密码错误，连接被拒绝。

## 问题根因

根据报错可以明确知道问题为：账户密码错误，连接被拒绝。

## 解决方案

使用正确的账户与密码进行连接。

忘记密码请按照业务方相关流程进行修改。
