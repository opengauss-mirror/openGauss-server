# CREATE PROC

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:31:10.991Z pushedAt=2026-09-21T09:50:43.379Z -->

## Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

Creates a new stored procedure.

## Notes<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- This section only contains the syntax newly added by shark. The original openGauss syntax has not been deleted or modified. For the original openGauss CREATE PROCEDURE syntax, see [CREATE PROCEDURE](../sql_reference/create_procedure.md).
- The CREATE PROC method is newly supported for creating stored procedures, and its functionality is consistent with the CREATE PROCEDURE method.

## Syntax<a name="en_topic_0283136578_en_topic_0237122106_en_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

```
CREATE [ OR REPLACE ] { PROCEDURE | PROC } procedure_name
    [ ( {[ argname ] [ argmode ] argtype [ { DEFAULT | := | = } expression ]}[,...]) ]
   { IS | AS } plsql_body 
/
```

## Parameter Description<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **PROC**

    A new method for creating stored procedures via CREATE PROC is introduced, with functionality consistent with that of CREATE PROCEDURE.

## Examples<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
create schema test_proc;
set current_schema to test_proc;

create procedure p1()
is
begin        
RAISE INFO 'call procedure: p1';
end;
/

create proc p2()
is
begin
RAISE INFO 'call procedure: p2';
end;
/

\df p1();
                                           List of functions
  Schema   | Name | Result data type | Argument data types |  Type  | fencedmode | propackage | prokind 
-----------+------+------------------+---------------------+--------+------------+------------+---------
 test_proc | p1   | void             |                     | normal | f          | f          | p
(1 row)

\df p2();
                                           List of functions
  Schema   | Name | Result data type | Argument data types |  Type  | fencedmode | propackage | prokind 
-----------+------+------------------+---------------------+--------+------------+------------+---------
 test_proc | p2   | void             |                     | normal | f          | f          | p
(1 row)

call test_proc.p1();
INFO:  call procedure: p1
 p1
----

(1 row)

call test_proc.p2();
INFO:  call procedure: p2
 p2
----

(1 row)
```

## Related Links<a name="section156744489391"></a>

[CREATE PROCEDURE](../sql_reference/create_procedure.md)