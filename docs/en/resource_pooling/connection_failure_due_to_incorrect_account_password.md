# Connection Failure Due to Incorrect Account Password

## Symptom

Connection attempts fail with the following error message:

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

## Troubleshooting Method

The error message explicitly indicates an authentication failure. The database rejected the connection request due to an invalid username or password combination.

## Root Cause

The connection attempt was rejected because the provided credentials (username or password) do not match the records stored in the database system catalog.

## Solution

Establish the connection using the correct account name and password.

If the password has been forgotten, follow the official password reset procedure defined by your organization's operational policies.
