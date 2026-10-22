# Managing Transactions

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:13.215Z pushedAt=2026-09-21T06:28:32.406Z -->

A transaction is a user-defined sequence of database operations that are executed as an indivisible unit of work, either all or none. The transaction control commands supported by the shark plugin include starting, setting savepoints, committing, and rolling back transactions.

## Notes

- This section only contains the syntax newly added by shark. For the original openGauss transaction control syntax, only the begin transaction transaction_mode syntax is deleted due to unavoidable conflicts. The conflicting syntax can be replaced by similar syntax such as begin transaction_mode or start transaction transaction_mode.
- PL/pgSQL in D databases does not support the BEGIN TRAN and BEGIN TRANSACTION syntax.

## Syntax

- Starting a transaction

    Use the BEGIN syntax to start a transaction. transaction_name has no actual meaning and is provided only for syntax compatibility.

    ```
    BEGIN { TRAN | TRANSACTION } [ { transaction_name } ];
    ```

- Setting a savepoint

    Use the SAVE syntax to mark the current data state. It allows all commands executed after the savepoint was established to be rolled back, restoring the transaction state to the moment when the savepoint was created.

    Sets a savepoint.

    ```
    SAVE { TRAN | TRANSACTION } { savepoint_name };
    ```

    Rolls back to a savepoint.

    ```
    ROLLBACK { TRAN | TRANSACTION } { savepoint_name };
    ```

- Commit a transaction

    Uses COMMIT to commit a transaction, that is, to commit all operations in the transaction. transaction_name has no actual meaning and is provided only for syntax compatibility.

    ```
    COMMIT [ { TRAN | TRANSACTION } [ transaction_name ] ];
    ```

- Roll back a transaction

    Use ROLLBACK to roll back to a specific savepoint or roll back all operations in the transaction. When the savepoint_name parameter is specified, it rolls back to the corresponding savepoint; otherwise, it rolls back all operations.

    ```
    ROLLBACK { TRAN | TRANSACTION } [ savepoint_name ];
    ```

## Parameter Description

- **TRAN | TRANSACTION**

    An optional keyword in the BEGIN syntax, which has no practical effect.

- **transaction_name**

    Has no practical meaning and is provided only for syntax compatibility.

- **SAVE**

    Sets a savepoint.

- **savepoint_name**

    The name of the savepoint, which can be used later with the ROLLBACK command.

- **COMMIT**

    Commits the current transaction, making all changes of the current transaction visible to other transactions.

- **ROLLBACK**

    Rolls back the current transaction, undoing all operations.

## Examples

```
opengauss=# create table t1 (c1 int);
CREATE TABLE
opengauss=# begin tran;
BEGIN
opengauss=# insert into t1 values(1);
INSERT 0 1
opengauss=# save tran savepoint1;
SAVEPOINT
opengauss=# insert into t1 values(2);
INSERT 0 1
opengauss=# rollback tran savepoint1;
ROLLBACK
opengauss=# commit tran;
COMMIT
opengauss=# select * from t1;
 c1 
----
  1
(1 row)

```