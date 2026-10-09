# Transparent Write Forwarding Within a Transaction on the Standby

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-17T06:50:39.114Z -->

## Availability<a name="section15406143204715"></a>

This feature is introduced since openGauss 5.1.0 and applies only to the resource pool centralized architecture.

## Feature Description <a name="section740615433477"></a>

This feature is an enhancement of the standby transaction write forwarding feature under the traditional primary/standby architecture, adapted for the resource pool centralized architecture. Under the traditional architecture, the standby write forwarding feature forwards the entire transaction to the primary node whenever a transaction is initiated on the standby. With this feature, under the resource pool centralized architecture, when write forwarding is enabled and a transaction is initiated on the standby, read requests within the transaction are executed locally on the standby, while write requests are forwarded to the primary node for execution.

## Customer Value<a name="section13406743164715"></a>

Under the resource pool centralized architecture, the cluster provides the functional effect of supporting concurrent writes on multiple nodes. When data conflicts between concurrent operations on primary and standby nodes are minimal, the overall cluster performance achieves a linear scalability ratio.

## Feature Description<a name="section16406154310471"></a>

This feature depends on the [writable standby feature](https://docs.opengauss.org/en/docs/latest/database_reference/enabling_write_statements_on_standby_nodes.html). Under the resource pool centralized architecture, when the writable standby feature is enabled, for explicit transactions executed on the standby node (that is, SQL statements enclosed by BEGIN and END), the database automatically forwards the write SQL statements involved in the transaction to the primary node, while the read statements in the transaction are still executed locally on the standby node.

## Feature Enhancement<a name="section1340684315478"></a>

This feature is an enhancement of the transaction write forwarding feature on standby nodes under the traditional primary/standby architecture, extended to the resource pool centralized architecture.

## Feature Constraints<a name="section06531946143616"></a>

- Under the resource pool centralized architecture, when the writable standby feature is enabled, the standby forwards write SQL statements that involve modifications within a transaction to the primary, while read statements within the transaction are still executed locally on the standby.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, the standby does not support transactions containing DDL statements or LOCK statements. An error is reported in such cases.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, if a transaction contains subtransactions, all reads within the transaction are also forwarded to the primary.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, all cursor-related operations, including the cursors themselves, are forwarded to the primary.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, COPY-type statements are not forwarded to the primary node for execution. That is, COPY TO commands can be successfully executed on the standby node, while COPY FROM commands will report an error as expected.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, the execution behavior of external tools is not altered by the standby's support for write forwarding. Commands such as gs_dump, gs_dumpall, gs_probackup, and pg_recvlogical will still report errors indicating support or lack thereof according to their original logic.
- Under the resource pool centralized architecture, when the writable standby feature is enabled, the standby node does not support calls to stored procedures, functions, autonomous transactions, or packages outside of an explicit transaction block. To call a stored procedure, function, autonomous transaction, or package, it must be used within an explicit transaction block and invoked using the call xxx syntax. For example:

    ```sql
    openGauss=# begin;
    BEGIN
    openGauss=# call pck1.p1();
    INFO:   rowcount: 1
    INFO:   (2,200,var2,clob2,1234ABD2,text2)
    INFO:   rowcount: 2
     p1
    ----

    1 (row)
    openGauss=# end;
    COMMIT
    ```

## Dependencies<a name="section8406643144716"></a>

This feature depends on the writable standby forwarding feature.

## Principles

The basic principle of this feature is as follows: In the resource pool centralized architecture, after this feature is enabled, when a connection is established to a standby node through gsql or other drivers and a transaction operation is performed on the standby (that is, a transaction is started), the standby internally establishes a connection to the primary node and synchronously sends the transaction start operation to the primary. If the operation executed within the transaction is a read-only SQL statement (that is, a `SELECT` statement), the statement is executed only locally on the standby. If the operation executed within the transaction is a write-type statement (that is, an `IUD (Insert/Update/Delete)` statement), the statement is forwarded to the primary for execution, and the standby is responsible for receiving the result and transparently passing it through to the upper-layer service. If the transaction contains a `DDL`-type operation, an error is reported indicating that execution on the standby side is not supported.

## Usage Guide

To use this feature, you need to configure the GUC parameter `enable_remote_execute = on` on all nodes in the cluster. For detailed configuration methods, see [Parameters for Enabling Write Statements on Standby Nodes](https://docs.opengauss.org/en/docs/latest/database_reference/enabling_write_statements_on_standby_nodes.html). In addition, the `pg_hba.conf` file on the primary side (and on nodes that may become the primary after a primary/standby switchover) must allow the standby to establish internal write forwarding connections as the current service user. You can configure the most restrictive `host replication` rule based on the service user and standby IP, for example, `host replication <business_user> <standby_ip>/32 sha256`. If password-based authentication such as `sha256` is used, ensure that the standby database process can obtain the authentication credentials for the service user to connect to the primary. For example, configure the corresponding host, port, database, user, and password in a `.pgpass` file readable by the database runtime user. Write forwarding does not automatically reuse the password entered by the client when connecting to the standby. This rule only controls connection admission and cannot replace SYSADMIN privileges or the object permissions required by the target SQL statement. Avoid using overly permissive or password-free rules such as `host replication all 0.0.0.0/0 trust`. After the configuration is complete and the database is restarted to make it take effect, connect to the standby via gsql or another driver, execute transaction operations containing write-type statements on the standby, and observe whether the execution on the standby proceeds normally.

## Use Cases

This feature applies to scenarios where, under the resource pool centralized architecture, some write operations need to be distributed to standby nodes in the cluster for execution.