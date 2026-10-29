# shark - System Information Functions

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:29:39.875Z pushedAt=2026-09-21T08:20:41.614Z -->

This section only contains the system information functions newly added by the shark extension.

## Session Information Functions

- @@FETCH_STATUS

    Description: Returns the status of the last cursor FETCH statement, which can be issued against any cursor currently open on the connection. 0 indicates that FETCH succeeded, and -1 indicates that FETCH failed.

    Return Value Type: int

    Example:

    ```
    select @@FETCH_STATUS;
    ```

- @@ROWCOUNT

    Description: Returns the number of rows affected by the previous statement. If the number of rows exceeds 2 billion, use ROWCOUNT_BIG(). When using JDBC to obtain the number of rows affected by the previous statement, do not use metadata retrieval interfaces to access the database, as this may cause the previous SQL statement to be overwritten.

    Return type: int

    Example:

    ```
    select @@ROWCOUNT;
    ```

- ROWCOUNT_BIG()

    Description: Returns the number of rows affected by the last statement. This function is similar to @@ROWCOUNT, except that the return type of ROWCOUNT_BIG() is bigint.

    Return Value Type: bigint

    Example:

    ```
    select ROWCOUNT_BIG();
    ```

- @@SPID

    Description: Returns the session ID of the current user process.

    Return Value Type: bigint

    Example:

    ```
    select @@SPID;
    ```

- scope_identity()

    Description: Returns the last identity value inserted into an identity column within the same scope.

    Return Value Type: numeric(38, 0)

    Example:

    ```
    openGauss=# CREATE TABLE TZ(Z_id INT IDENTITY PRIMARY KEY, Z_name VARCHAR(20) NOT NULL);
    CREATE TABLE
    openGauss=# INSERT INTO TZ(Z_NAME) VALUES('Lisa');
    INSERT 0 1
    openGauss=# SELECT scope_identity();
    scope_identity 
    ----------------
                1
    (1 row)
    ```

## Object Information Functions

- object_id('[database_name.[schema_name]. | schema_name.]object_name' [, 'object_type'])

    Description: Returns the OID of a database object. Returns NULL if the query permission is not granted or the object does not exist.

    The second parameter object_type supports the following types:

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Property Name</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>S</td>
            <td>System Table</td>
        </tr>
        <tr>
            <td>U</td>
            <td>User Table</td>
        </tr>
        <tr>
            <td>V</td>
            <td>View</td>
        </tr>
        <tr>
            <td>SO</td>
            <td>Sequence</td>
        </tr>
        <tr>
            <td>C</td>
            <td>CHECK constraint</td>
        </tr>
        <tr>
            <td>D</td>
            <td>DEFAULT constraint</td>
        </tr>
        <tr>
            <td>F</td>
            <td>FOREIGN KEY constraint</td>
        </tr>
        <tr>
            <td>PK</td>
            <td>Primary Key Constraint</td>
        </tr>
        <tr>
            <td>UQ</td>
            <td>UNIQUE Constraint</td>
        </tr>
        <tr>
            <td>AF</td>
            <td>Aggregate Function</td>
        </tr>
        <tr>
            <td>FN</td>
            <td>Function</td>
        </tr>
        <tr>
            <td>P</td>
            <td>Stored Procedure</td>
        </tr>
        <tr>
            <td>TR</td>
            <td>Trigger</td>
        </tr>
    </tbody>
</table>

    Return value type: int

    Example:

    ```
    CREATE TABLE sys.students (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT DEFAULT 0,
        grade DECIMAL(5, 2)
    );
    set search_path = 'sys';
    select object_id('students');
    object_id 
    -----------
    16666
    (1 row)

    select object_id('sys.students', 'U');
    object_id 
    -----------
    16666
    (1 row)
    ```

- objectproperty(oid, property)

    Description: Returns the corresponding property result of an object in the plugin framework. Returns NULL if the object type does not match.

    Available property options

    Return value type: int

    **Table 1** Property attribute table

<table aria-label="Table 1" class="table table-sm margin-top-none">
    <thead>
        <tr>
            <th>Property Name</th>
            <th>Object Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>IsDefault</td>
            <td>Any object</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>IsDefaultCnst</td>
            <td>Any object</td>
            <td>Whether it is a DEFAULT constraint. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsDeterministic</td>
            <td>Function</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>IsIndexed</td>
            <td>Table, View</td>
            <td>Table or view that has an index. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsInlineFunction</td>
            <td>Function</td>
            <td>Inline function. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsSysShipped</td>
            <td>Any object</td>
            <td>Objects under the sys schema. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsPrimaryKey</td>
            <td>Any object</td>
            <td>Whether it is a PRIMARY KEY constraint. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsProcedure</td>
            <td>Any object</td>
            <td>Whether it is a stored procedure. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsRule</td>
            <td>Any object</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>IsScalarFunction</td>
            <td>Function</td>
            <td>Whether it is a scalar-valued function. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsSchemaBound</td>
            <td>Function, View</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>IsTable</td>
            <td>Table</td>
            <td>Whether it is a table. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsTableFunction</td>
            <td>Function</td>
            <td>Whether it is a table-valued function. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsTrigger</td>
            <td>Any object</td>
            <td>Whether it is a trigger. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsUserTable</td>
            <td>Table</td>
            <td>Whether it is a user table. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>IsView</td>
            <td>View</td>
            <td>Whether it is a view. 1=True, 0=False</td>
        </tr>
        <tr>
            <td>OwnerId</td>
            <td>Any object</td>
            <td>Returns the OID of the object owner.</td>
        </tr>
        <tr>
            <td>ExeclsQuotedIdentOn</td>
            <td>Function, Stored Procedure, Trigger, View</td>
            <td>Returns 1.</td>
        </tr>
        <tr>
            <td>ExeclsIsAnsiNullsOn</td>
            <td>Function, Stored Procedure, Trigger, View</td>
            <td>Returns 1.</td>
        </tr>
        <tr>
            <td>TableFulltextPopulateStatus</td>
            <td>Table</td>
            <td>Returns 0.</td>
        </tr>
        <tr>
            <td>TableHasVarDecimalStorageFormat</td>
            <td>Table</td>
            <td>Returns 0.</td>
        </tr>
    </tbody>
</table>

    Example:
    where database is the current database

    ```
    CREATE TABLE sys.students (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INT DEFAULT 0,
        grade DECIMAL(5, 2)
    );
    set search_path = 'sys';
    select objectproperty(object_id('students'), 'ownerid') as ownerid;
     ownerid 
    ---------
    10
    (1 row)
    select objectproperty(object_id('sys.students'), 'istable') as ownerid;
     ownerid 
    ---------
    1
    (1 row)
    select objectproperty(object_id('database.sys.students'), 'isview') as ownerid;
     ownerid 
    ---------
    0
    (1 row)
    ```

- databasepropertyex(database, property)

    Description: For the specified database, this function returns the current setting of the specified database option or property.

    Parameter Type:
    - `database` has a data type of nvarchar(128), used to specify the name of the database for which `databasepropertyex` returns the named property information.
    - `property` has a data type of varchar(128) and is used to specify the name of the database property to return.

    Return value type: sql_variant

    Table 2: property attribute table

    <table aria-label="Table 2" class="table table-sm margin-top-none">
        <thead>
            <tr>
                <th>Property Name</th>
                <th>Field Description</th>
                <th>Return Value</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Collation</td>
                <td>Default collation of the database</td>
                <td>Returns the datcollate attribute value of the queried database from pg_database</td>
            </tr>
            <tr>
                <td>ComparisonStyle</td>
                <td>Windows comparison style for collation rules</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>Edition</td>
                <td>Database version or service tier</td>
                <td>Returns Standard</td>
            </tr>
            <tr>
                <td>IsAnsiNullsEnabled</td>
                <td>All comparisons with null values are treated as unknown.</td>
                <td>In openGauss, this is a session-level parameter and returns 1 by default.</td>
            </tr>
            <tr>
                <td>IsAnsiPaddingEnabled</td>
                <td>Strings are padded to the same length before comparison or insertion.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsAnsiWarningsEnabled</td>
                <td>When a standard error condition occurs, an error message or warning message is issued. If a Null value appears in an aggregate function, an error and warning are issued.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsArithmeticAbortEnabled</td>
                <td>If an overflow or division-by-zero error occurs during query execution, the query will be terminated.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsAutoClose</td>
                <td>After the last user exits, the database shuts down completely and releases resources.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsAutoCreateStatistics</td>
                <td>The query optimizer creates single-column statistics as needed to improve query performance.</td>
                <td>Defaults to 1 in openGauss, returns 1.</td>
            </tr>
            <tr>
                <td>IsAutoCreateStatisticsIncremental</td>
                <td>When conditions permit, the created single-column statistics are incremental.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsAutoShrink</td>
                <td>Periodic shrinking of database files</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsAutoUpdateStatistics</td>
                <td>The query optimizer automatically updates potentially outdated statistics.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsClone</td>
                <td>The database is a schema-only and statistic-only copy of a user database created using DBCC CLONEDATABASE.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsCloseCursorsOnCommitEnabled</td>
                <td>Closes all open cursors after a transaction is committed.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsDatabaseSuspendedForSnapshotBackup</td>
                <td>The database is suspended.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsFulltextEnabled</td>
                <td>Supports full-text and semantic search on the database</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsInStandBy</td>
                <td>The database is online in read-only mode, with recovery log support.</td>
                <td>1 for true, 0 for false.</td>
            </tr>
            <tr>
                <td>IsLocalCursorsDefault</td>
                <td>Cursor declarations default to LOCAL.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsMemoryOptimizedElevateToSnapshotEnabled</td>
                <td>When the transaction isolation level is set to read committed, read uncommitted, or lower isolation levels, snapshot isolation is used to access memory-optimized tables.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsMergePublished</td>
                <td>If replication (backup) is installed, allows supporting database table publication for merge replication (backup).</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsNullConcat</td>
                <td>Null concatenation produces Null</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsNumericRoundAbortEnabled</td>
                <td>Precision loss in an expression will cause an error.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsParameterizationForced</td>
                <td>Whether the parameterized database is set to FORCED.</td>
                <td>Returns 0.</td>
            </tr>
            <tr>
                <td>IsQuotedIdentifersEnabled</td>
                <td>Allows the use of double quotation marks</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsPublished</td>
                <td>If replication is installed, supports publishing database tables for snapshot replication or transactional replication</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsRecursiveTriggersEnable</td>
                <td>Recursive triggers enabled</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsSubscribed</td>
                <td>Database subscribed for publishing</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsSyncWithBackup</td>
                <td>The database is a publishing database or a distributed database, and supports restoration without interrupting transaction replication</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsTornPageDetectionEnabled</td>
                <td>Detects incomplete I/O operations caused by power outages or other system failures.</td>
                <td>1 for true, 0 for false.</td>
            </tr>
            <tr>
                <td>IsVerifiedClone</td>
                <td>The database is a schema-only and statistics-only copy of a user database created using DBCC CLONEDATABASE with the WITH VERIFY_CLONEDB option.</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>IsXTPSupported</td>
                <td>Whether the database supports XTP</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>LastGoodCheckDbTime</td>
                <td>Date and time of the last successful DBCC CHECKDB on the specified database</td>
                <td>Returns NULL</td>
            </tr>
            <tr>
                <td>LCID</td>
                <td>Windows locale identifier for the collation</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>MaxSizeInBytes</td>
                <td>Maximum database size (in bytes)</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>Recovery</td>
                <td>Database recovery mode</td>
                <td>Returns NULL</td>
            </tr>
            <tr>
                <td>ServiceObjective</td>
                <td>Describes the database performance level in SQL Database or Azure Synapse Analytics</td>
                <td>Returns NULL</td>
            </tr>
            <tr>
                <td>ServiceObjectiveId</td>
                <td>Service objective ID in SQL Database</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>SQLSortOrder</td>
                <td>Sort ID supported in earlier versions</td>
                <td>Returns 0</td>
            </tr>
            <tr>
                <td>Status</td>
                <td>Database status</td>
                <td>Returns ONLINE</td>
            </tr>
            <tr>
                <td>Updateability</td>
                <td>Indicates whether data can be modified</td>
                <td>1 for true, 0 for false</td>
            </tr>
            <tr>
                <td>UserAccess</td>
                <td>Shows which users can access the database</td>
                <td>Returns NULL</td>
            </tr>
            <tr>
                <td>Version</td>
                <td>Internal version number of the code used to create the database.</td>
                <td>Returns the openGauss version sequence number.</td>
            </tr>
            <tr>
                <td>ReplicaID</td>
                <td>Replica ID of the connected hyperscale database/replica.</td>
                <td>Returns NULL.</td>
            </tr>
        </tbody>
    </table>

    Example:

    ```
    openGauss=# SELECT databasepropertyex('existDB','Collation') AS Collation;
    collation  
    -------------
    zh_CN.UTF-8
    (1 row)
    ```

- suser_name(\[server_user_id\])

    Description: Returns the login identification name of the user.

    Parameter Type:
    - `server_user_id` has the data type oid, and is used to specify the OID corresponding to the login identification name of the user that `suser_name` is to return. When the user does not input any user OID, this function returns the login identification name of the current user by default. If the input is NULL, this function returns NULL.

    Return value type: nvarchar(128)

    Example:

    ```
    openGauss=# SELECT suser_name(10) AS suser_name;
    suser_name 
    ------------
    user_name
    (1 row)
    ```

- suser_sname(\[server_user_sid\])

    Description: Returns the login identification name of a user.

    Parameter type:
    - The data type of `server_user_sid` is varbinary(85), used to specify the OID corresponding to the login identifier name of the user to be returned by `suser_sname`. This function is currently equivalent to `suser_name`.

    Return value type: nvarchar(128)

    Example:

    ```
    openGauss=# SELECT suser_sname(10::varbinary) AS suser_sname;
    suser_sname 
    -------------
    user_name
    (1 row)
    ```

- @@PROCID

    Description: Returns the OID of the current module. The module can be a stored procedure, a user-defined function, or a trigger.

    Return Value Type: oid

    Example:

    ```
    -- Create a stored procedure and call @@PROCID.
    openGauss=# CREATE PROCEDURE test_procid
    openGauss-# AS
    openGauss$# DECLARE
    openGauss$#     ProcID integer;
    openGauss$# BEGIN
    openGauss$#     ProcID = @@PROCID;
    openGauss$#     RAISE INFO 'Stored procedure %', ProcID;
    openGauss$# END;
    openGauss$# /
    CREATE PROCEDURE
    
    -- Call the stored procedure.
    openGauss=# SELECT test_procid();
    INFO:  Stored procedure 49675
    test_procid 
    -------------
    
    (1 row)
    ```