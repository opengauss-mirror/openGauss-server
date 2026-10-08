# Operators

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:28:15.005Z pushedAt=2026-09-21T06:56:18.632Z -->

## Operator Description<a name="zh-cn_topic_0283137550_zh-cn_topic_0237122008_zh-cn_topic_0059778242_seea12beab1954749bad838953810aa71"></a>

- The usage of postfix operators has been removed in shark. Previously, the only built-in postfix operator in openGauss was the factorial `!`. To maintain forward compatibility, only the syntax rules related to postfix expressions have been removed from the shark grammar file. Therefore, the records in the system table pg\_operator remain unchanged, and the use of postfix expressions in other D-compatible databases is not affected.
- The '< >' operator is newly supported, meaning that any number of spaces are allowed between the greater-than and less-than symbols, and it still represents the inequality meaning.

## Examples<a name="en_topic_0283137550_en_topic_0237122008_en_topic_0059778242_sf97102106fb1409c84eb71bd5d69dc11"></a>

In a D-compatible database, the following statements are no longer supported and will report a syntax error.

```
opengauss=# SELECT 40 ! AS "40 factorial";
ERROR:  syntax error at or near "AS"
LINE 1: SELECT 40 ! AS "40 factorial";
                    ^


-- test < >
opengauss=# create table test1(id int, name varchar(10));
CREATE TABLE
opengauss=# insert into test1 values(1, 'test1');
INSERT 0 1
opengauss=# insert into test1 values(2, 'test2');
INSERT 0 1
opengauss=# select * from test1 where id <> 3;
 id | name
----+-------
  1 | test1
  2 | test2
(2 rows)

opengauss=# select * from test1 where id < > 3;
 id | name
----+-------
  1 | test1
  2 | test2
(2 rows)

opengauss=# select * from test1 where id <    > 3;
 id | name
----+-------
  1 | test1
  2 | test2
(2 rows)

```

## Related Links<a name="section156744489391"></a>

[Operators](../sql_reference/operators.md)