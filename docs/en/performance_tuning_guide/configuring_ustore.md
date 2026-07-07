# Configuring Ustore<a name="EN-US_TOPIC_0000001179816320"></a>

The Ustore storage engine, also called the in-place update storage engine, is a new storage mode added to the openGauss kernel. The row storage engine used by the earlier openGauss versions is in append update mode. Append update has good performance in service addition, deletion, and heap only tuple \(HOT\) update \(that is, update on the same page\). However, recycling is not efficient in cross-data-page non-HOT update scenarios. Therefore, Ustore comes into being.

## Design Principle<a name="section101901757153119"></a>

Ustore stores valid data of the latest version and junk data of earlier versions separately. The valid data of the latest version is stored on the data page, and an independent UNDO space is created for managing the junk data of earlier versions in a unified manner. Therefore, the data space does not expand due to frequent updates, and the junk data is recycled more efficiently.

Ustore adopts the NUMA-aware UNDO subsystem design, which enables the UNDO subsystem to be effectively expanded on the multi-core platform. In addition, Ustore adopts the multi-version index technology to clear indexes and improve the efficiency of reclaiming and reusing storage space.

Ustore works with the UNDO space to implement more efficient and comprehensive flashback query and recycle bin mechanisms, quickly roll back misoperations, and provide abundant enterprise-level functions for openGauss.

## Core Advantages<a name="section69751648124511"></a>

- **High performance**: For services with different loads, such as insertion, update, and deletion, the performance and resource usage are relatively balanced. The in-place update mode is recommended in frequent update scenarios, featuring higher and more stable performance. It is suitable for typical OLTP service scenarios that require  **short**  transactions,  **frequent**  updates, and  **high**  performance.
- **Efficient storage**: Maximizes in-place update, greatly saving space. Rollback segments and data pages are stored separately, providing more efficient and stable I/O usage. The UNDO subsystem uses the NUMA-aware design and has better multi-core scalability. The UNDO space is allocated and reclaimed in a unified manner, improving the reuse efficiency and storage space usage.
- **Fine-grained resource control**: The Ustore engine provides multi-dimensional transaction monitoring. It monitors transaction running based on the transaction running duration, size of the UNDO space used by a single transaction, and overall UNDO space limit to prevent abnormal and unexpected behaviors. This feature enables database administrators to regulate and restrict the use of database system resources.

Ustore provides stable performance in scenarios where data is frequently updated, enabling service systems to run more stably and adapt to more service scenarios and workloads, especially core financial service scenarios that have higher requirements on performance and stability.

## Usage Guide<a name="section2190298487"></a>

Ustore coexists with the original append update storage engine \(Astore\). Ustore shields the implementation details of the storage layer. The SQL syntax is basically the same as that of the original Astore storage engine. The only difference lies in table creation and index creation.

- **Table creation**

    Ustore contains undo logs. Before creating a table, you need to set  **undo\_zone\_count**  in the  **postgresql.conf**  file. This parameter indicates the number of undo logs. The recommended value is  **16384**, that is,  **undo\_zone\_count=16384**. After the configuration is complete, restart the database.

    \[postgresql.conf\]

    ```
    undo_zone_count=16384
    ```

    - **Method 1: Specify the storage engine type when creating a table.**

    ```
    create table test(id int, name varchar(10)) with (storage_type=ustore);
    ```

    - **Method 2: Specify Ustore by configuring a GUC parameter.**

1. Before starting a database, set  **enable\_default\_ustore\_table**  to  **on**  in  **postgresql.conf**  to specify that Ustore is used when a user creates a table by default.

    \[postgresql.conf\]

    ```
    enable_default_ustore_table=on
    ```

2. Create a table.

    ```
    create table test(id int, name varchar(10));
    ```

- **Index creation**

    The index used by Ustore is UBtree. UBtree is developed for the Ustore storage engine and is the only index type supported by Ustore.

    Taking the following table  **test**  as an example, add an index  **UBtree**  to the  **age**  column of the  **test**  table.

    ```
    openGauss=# \d+  test
                             Table "public.test"
     Column |  Type                 | Modifiers | Storage  | Stats target | Description
    --------+-----------------------+-----------+----------+--------------+-------------
     id     | integer               |           | plain    |              |
     age    | integer               |           | plain    |              |
     name   | character varying(10) |           | extended |              |
    ```

    - **Method 1: If the index type is not specified, a UBtree index is created by default.**

        ```
        openGauss=# create index ubt_idx on test(age);
        ```

        ```
        openGauss=# \d+  test
                                        Table "public.test"
         Column |  Type                 | Modifiers | Storage  | Stats target | Description
        --------+-----------------------+-----------+----------+--------------+-------------
         id     | integer               |           | plain    |              |
         age    | integer               |           | plain    |              |
         name   | character varying(10) |           | extended |              |
        Indexes:
            "ubt_idx" ubtree (age) WITH (storage_type=USTORE) TBALESPACE pg_default
        Has OIDs: no
        Options: orientation=row, storage_type=ustore, compression=no
        ```

    - **Method 2: When creating an index, use the using keyword to set the index type to ubtree.**

        ```
        openGauss=# create index ubt_idx on test using ubtree(age);
        ```

        ```
        openGauss=# \d+  test
                                        Table "public.test"
         Column |  Type                 | Modifiers | Storage  | Stats target | Description
        --------+-----------------------+-----------+----------+--------------+-------------
         id     | integer               |           | plain    |              |
         age    | integer               |           | plain    |              |
         name   | character varying(10) |           | extended |              |
        Indexes:
            "ubt_idx" ubtree (age) WITH (storage_type=USTORE) TBALESPACE pg_default
        Has OIDs: no
        Options: orientation=row, storage_type=ustore, compression=no
        ```
