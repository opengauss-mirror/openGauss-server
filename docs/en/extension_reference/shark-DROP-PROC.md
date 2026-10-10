# DROP PROC

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:32:06.717Z pushedAt=2026-09-21T10:03:35.475Z -->

## Function Description<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

Deletes an existing stored procedure.

## Notes<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- This section only contains the syntax newly added in shark. The original openGauss syntax has not been deleted or modified. For the original openGauss DROP PROCEDURE syntax, see [DROP PROCEDURE](https://docs.opengauss.org/en/docs/latest/sql_reference/drop_procedure.html).
- The newly added DROP PROC method for dropping stored procedures is supported, and its functionality is consistent with the DROP PROCEDURE method.

## Syntax<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

```
DROP { PROCEDURE | PROC } [ IF EXISTS ] procedure_name 
[ ( [ {[ argname ] [ argmode ] argtype} [, ...] ] ) [ CASCADE | RESTRICT ] ];
```

## Parameter Description<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **PROC**

    Newly added in database D, the DROP PROC statement deletes stored procedures and is functionally consistent with DROP PROCEDURE.

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

drop proc p1;
drop procedure p2;
```

## Related Links<a name="section156744489391"></a>

[DROP PROCEDURE](https://docs.opengauss.org/en/docs/latest/sql_reference/drop_procedure.html)