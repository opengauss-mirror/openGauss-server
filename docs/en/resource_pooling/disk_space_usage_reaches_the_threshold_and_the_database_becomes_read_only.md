# Disk Space Usage Reaches the Threshold and the Database Becomes Read-only<a name="EN-US_TOPIC_0291615095"></a>

## Symptom<a name="section19815185425919"></a>

The following error is reported when a non-read-only SQL statement is executed.

```
ERROR: cannot execute %s in a read-only transaction.
```

An error is reported when some non-read-only SQL statements \(such as insert, update, create table as, create index, alter table, and copy from\) are executed.

```
canceling statement due to default_transaction_read_only is on.
```

## Cause Analysis<a name="section192473272047"></a>

After the disk space usage reaches the threshold, the database enters the read-only mode. In this mode, only read-only statements can be executed.

## Procedure<a name="section17713758135913"></a>

1. Use either of the following methods to connect to the database in maintenance mode:
    - Method 1

        ```
        gsql -d postgres -p 8000 -r -m
        ```

    - Method 2

        ```
        gsql -d postgres -p 8000 -r
        ```

        After the connection is successful, run the following command.

        ```
        set xc_maintenance_mode=on;
        ```

2. Run the **DROP** or **TRUNCATE** statement to delete user tables that are no longer used until the disk space usage falls below the threshold.

    Deleting user tables can only temporarily relieve the insufficient disk space. To permanently solve the problem, expand the disk space.

3. Disable the read-only mode of the database as user **omm**.

    ```
    gs_guc reload -D /gaussdb/data/dbnode -c "default_transaction_read_only=off"
    ```
