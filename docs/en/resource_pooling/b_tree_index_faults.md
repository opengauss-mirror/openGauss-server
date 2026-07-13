# B-tree Index Faults<a name="EN-US_TOPIC_0291615109"></a>

## Symptom<a name="section14883333175911"></a>

The following error message is displayed, indicating that the index is lost occasionally.

```
ERROR: index 'xxxx_index' contains unexpected zero page
Or
ERROR: index 'pg_xxxx_index' contains unexpected zero page
Or
ERROR: compressed data is corrupt
```

## Cause Analysis<a name="section14246173814590"></a>

This type of error is caused by the index fault. The possible causes are as follows:

- The index is unavailable due to software bugs or hardware faults.
- The index contains many empty pages or almost empty pages.
- During concurrent DDL execution, the network is intermittently disconnected.
- The index failed to be created when indexes are concurrently created.
- A network fault occurs when a DDL or DML operation is performed.

## Procedure<a name="section237115426595"></a>

Run the REINDEX command to rebuild the index.

1. Log in to the host as the OS user  **omm**.
2. Run the following command to connect to the database:

    ```
    gsql -d postgres -p 8000 -r
    ```

3. Rebuild the index.
    - During DDL or DML operations, if index problems occur due to software or hardware faults, run the following command to rebuild the index:

        ```
        REINDEX TABLE tablename;
        ```

    - If the error message contains  _xxxx_**\_index**, the index of a user table is faulty.  _xxxx_  indicates the name of the user table. Run either of the following commands to rebuild the index:

        ```
        REINDEX INDEX indexname; 
        ```

        Or

        ```
        REINDEX TABLE tablename;
        ```

    - If the error message contains  **pg\_**_xxxx_**\_index**, the index of the system catalog is faulty. Run the following command to rebuild the index:

```
REINDEX SYSTEM databasename;
```
