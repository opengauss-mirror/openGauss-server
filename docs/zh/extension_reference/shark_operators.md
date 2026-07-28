# 操作符

## 操作符说明<a name="zh-cn_topic_0283137550_zh-cn_topic_0237122008_zh-cn_topic_0059778242_seea12beab1954749bad838953810aa71"></a>

- shark中移除了后缀操作符的用法，在此之前 openGauss中唯一的内置后缀操作符是阶乘`!`，为了保持前向兼容性，仅在shark的语法文件中移除了后缀表达式相关的语法规则，因此并没有修改系统表pg\_operator中的记录，也并不影响其他兼容性数据库中后缀表达式的使用。
- 新增支持'< >'操作符，即大于小于符号中间支持包含任意个空格，仍代表不等于的含义。

## 示例<a name="zh-cn_topic_0283137550_zh-cn_topic_0237122008_zh-cn_topic_0059778242_sf97102106fb1409c84eb71bd5d69dc11"></a>

在兼容D库中以下语句将不再支持，而是报告语法错误。

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

## 相关链接<a name="section156744489391"></a>

[操作符](https://docs.opengauss.org/zh/docs/latest/sql_reference/operators.html)
