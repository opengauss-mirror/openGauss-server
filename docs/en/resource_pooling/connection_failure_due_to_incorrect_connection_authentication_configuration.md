# Connection Failure Due to Incorrect Connection Authentication Configuration

## Symptom

Attempts to connect to the database fail with the following error message:

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

## Troubleshooting Method

The error message explicitly indicates that the connection attempt to the monitoring address `A.B.C.D` was rejected because the authentication method was set to `trust`.

The database connection authentication configuration file is `pg_hba.conf`. Inspect the remote login authentication configuration:

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

Verification confirms that the `trust` method is indeed configured for remote connections.

## Root Cause

The database does not support remote logins using the `trust` authentication method.

## Solution

Since remote logins using the `trust` method are not supported, modify the configuration to use `sha256`, `md5`, or other supported authentication methods.

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
