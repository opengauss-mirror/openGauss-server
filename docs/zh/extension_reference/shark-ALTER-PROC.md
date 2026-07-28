# ALTER PROC

## 功能描述<a name="zh-cn_topic_0283137126_zh-cn_topic_0237122076_zh-cn_topic_0059779051_s2baab5c876044795a12b5949f22d2144"></a>

修改自定义存储过程的属性。

## 注意事项<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s31780559299b4f62bec935a2c4679b84"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的ALTER PROCEDURE语法请参考章节[ALTER PROCEDURE](https://docs.opengauss.org/zh/docs/latest/sql_reference/alter_procedure.html)。
- 新增支持通过ALTER PROC方式修改自定义存储过程的属性，功能和ALTER PROCEDURE方式保持一致。
- ALTER PROCEDURE/PROC COMPILE仅在A库生效，在D库报错不支持。

## 语法格式<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_sa24c1a88574742bcb5427f58f5abb732"></a>

- 修改自定义存储过程的附加参数。

    ```
    ALTER { PROCEDURE | PROC } procedure_name ( [ { [ argname ] [ argmode ] argtype} [, ...] ] )
        action [ ... ] [ RESTRICT ];
    ```

    其中附加参数action子句语法为:

    ```
    {CALLED ON NULL INPUT  | STRICT}
     | {IMMUTABLE | STABLE | VOLATILE}
     | {SHIPPABLE | NOT SHIPPABLE}
     | {NOT FENCED | FENCED}
     | [ NOT ] LEAKPROOF
     | { [ EXTERNAL ] SECURITY INVOKER | [ EXTERNAL ] SECURITY DEFINER }
     | AUTHID { DEFINER | CURRENT_USER }
     | COST execution_cost
     | ROWS result_rows
     | SET configuration_parameter { { TO | = } { value | DEFAULT }| FROM CURRENT}
     | RESET {configuration_parameter | ALL}
     | COMMENT 'text'
    ```

- 修改自定义存储过程的名称。

    ```
    ALTER { PROCEDURE | PROC } proname ( [ { [ argname ] [ argmode ] argtype} [, ...] ] )
        RENAME TO new_name;
    ```

- 修改自定义存储过程的所属者。

    ```
    ALTER { PROCEDURE | PROC } proname ( [ { [ argname ] [ argmode ] argtype} [, ...] ] )
        OWNER TO new_owner;
    ```

- 修改自定义存储过程的模式。

    ```
    ALTER { PROCEDURE | PROC } proname ( [ { [ argname ] [ argmode ] argtype} [, ...] ] )
        SET SCHEMA new_schema;
    ```

- 重编译存储过程。

    ```
    ALTER { PROCEDURE | PROC } procedure_name COMPILE;
    ```

    仅在A库生效，在D库报错不支持。

## 参数说明<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s82e47e35c54c477094dcafdc90e5d85a"></a>

- **PROC**

    新增通过ALTER PROC方式修改自定义存储过程的属性，功能和ALTER PROCEDURE方式保持一致。

## 示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

alter procedure p1() stable;
alter proc p2() stable;

select provolatile from pg_proc where proname = 'p1';
 provolatile 
-------------
 s
(1 row)

select provolatile from pg_proc where proname = 'p2';
 provolatile 
-------------
 s
(1 row)

alter procedure p1() rename to new_p1;
alter proc p2() rename to new_p2;

\df new_p1();
                                            List of functions
  Schema   |  Name  | Result data type | Argument data types |  Type  | fencedmode | propackage | prokind 
-----------+--------+------------------+---------------------+--------+------------+------------+---------
 test_proc | new_p1 | void             |                     | normal | f          | f          | p
(1 row)

\df new_p2();
                                            List of functions
  Schema   |  Name  | Result data type | Argument data types |  Type  | fencedmode | propackage | prokind 
-----------+--------+------------------+---------------------+--------+------------+------------+---------
 test_proc | new_p2 | void             |                     | normal | f          | f          | p
(1 row)

create user test_proc_user with password 'xxxxxxxx';
grant all privileges to test_proc_user;

alter procedure new_p1() owner to test_proc_user;
alter proc new_p2() owner to test_proc_user;

select usename from pg_user a, pg_proc b where a.usesysid = b.proowner and b.proname = 'new_p1';
    usename     
----------------
 test_proc_user
(1 row)

select usename from pg_user a, pg_proc b where a.usesysid = b.proowner and b.proname = 'new_p2';
    usename     
----------------
 test_proc_user
(1 row)

create schema new_schema;

alter procedure new_p1() set schema new_schema;
alter proc new_p2() set schema new_schema;

select nspname from pg_namespace a, pg_proc b where a.oid = b.pronamespace and b.proname = 'new_p1';
  nspname   
------------
 new_schema
(1 row)

select nspname from pg_namespace a, pg_proc b where a.oid = b.pronamespace and b.proname = 'new_p2';
  nspname   
------------
 new_schema
(1 row)

alter procedure new_p1 compile;
ERROR:  This operation is not supported.
alter procedure new_p1() compile;
ERROR:  This operation is not supported.
alter proc new_p2 compile;
ERROR:  This operation is not supported.
alter proc new_p2() compile;
ERROR:  This operation is not supported.

call new_schema.new_p1();
INFO:  call procedure: p1
 new_p1 
--------
 
(1 row)

call new_schema.new_p2();
INFO:  call procedure: p2
 new_p2 
--------
 
(1 row)
```

## 相关链接<a name="section156744489391"></a>

[ALTER PROCEDURE](https://docs.opengauss.org/zh/docs/latest/sql_reference/alter_procedure.html)
