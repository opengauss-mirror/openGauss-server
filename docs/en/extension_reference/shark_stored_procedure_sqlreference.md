# Stored Procedure

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:35.823Z pushedAt=2026-09-21T07:29:05.259Z -->

Business rules and business logic can be stored in openGauss through programs, and such programs are referred to as stored procedures.

A stored procedure is a combination of SQL and PL/SQL. Stored procedures enable the code that executes business rules to be moved from an app to the database. As a result, the code can be stored once and used by multiple programs. Users can invoke the stored procedure repeatedly, thereby reducing the amount of duplicate SQL statements and improving work efficiency.

## Notes<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- This section only contains the syntax newly added by shark. The original openGauss syntax has not been deleted or modified.
- The shark plugin adds a new procedural language, pltsql. The default language for stored procedures created with CREATE PROCEDURE is pltsql.

## Example<a name="zh-cn_topic_0283136560_zh-cn_topic_0237122104_zh-cn_topic_0059778837_scc61c5d3cc3e48c1a1ef323652dda821"></a>

```
openGauss=# create procedure p1
is
begin
null;
end;
/
CREATE PROCEDURE
----Define a stored procedure.
openGauss=# select l.lanname from pg_language l join pg_proc p on l.oid = p.prolang and p.proname = 'p1';
 lanname
---------
 pltsql
(1 row)

```