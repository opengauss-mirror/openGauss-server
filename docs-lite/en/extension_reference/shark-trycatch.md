# TRY...CATCH Statement

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-09-13T02:27:30.053Z pushedAt=2026-09-29T02:34:56.694Z -->

shark implements an error handling mechanism similar to exception handling. If an SQL statement in the TRY block encounters an error, the statements in the CATCH block are executed.

## Precautions

- This section describes only the syntax newly added in shark.

## Syntax Format

- BEGIN TRY
    { sql_statement | statement_block }
    END TRY
    BEGIN CATCH
    [ { sql_statement | statement_block } ]
    END CATCH
    [ ; ]

## Parameters

- **sql_statement**

    Any SQL statement except transaction management statements.

- **statement_block**

    Any SQL statement block except transaction management statements.

## Example

```
opengauss=# create table test_3(a int);
opengauss=# begin try;
opengauss=# insert into test_3 values(2);
opengauss=# select 1/0;
ERROR:  division by zero
opengauss=# end try begin catch;
opengauss=# insert into test_3 values(3);
opengauss=# select 1/0;
ERROR:  division by zero
opengauss=# select * from test_3;
ERROR:  current catch block is failed, commands ignored until end of catch block
opengauss=# end catch;
opengauss=# select * from test_3;
 a
---
(0 rows)

```
