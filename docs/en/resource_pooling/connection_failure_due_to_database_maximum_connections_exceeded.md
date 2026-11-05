# Connection Failure Due to Database Maximum Connections Exceeded

## Symptom

Connection attempts fail with the following error message:

```shell
2024-09-27 10:55:20 Exception: [127.0.0.1:34406/127.0.0.1:20088] FATAL: Too many clients already, current: 203, max_connections/reserved: 200/3.
        at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2935)
        at org.postgresql.core.v3.QueryExecutorImpl.readStartupMessages(QueryExecutorImpl.java:3065)
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

## Troubleshooting Method

The error message explicitly indicates that the maximum number of connections has been exceeded. Within the database, the GUC parameters governing connection limits are `max_connections` and `sysadmin_reserved_connections`, which define the maximum allowable connections and the number of connections reserved for system administrators, respectively.

Verify the current configuration by executing the following SQL statements:

```sql
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

The output confirms that the connection pool is saturated.

## Root Cause

The number of active client connections has surpassed the configured limit.

## Solution

Adjust the `max_connections` and `sysadmin_reserved_connections` parameters according to business requirements and system capacity.
