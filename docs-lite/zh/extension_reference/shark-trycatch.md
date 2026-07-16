# TRY...CATCH语句

shark实现了一种与异常处理的类似的错误处理机制，当TRY控制块内的SQL语句出现错误时，会继续执行CATCH控制块内的语句。

## 注意事项

- 本章节只包含shark新增的语法。

## 语法格式

- BEGIN TRY
    { sql_statement | statement_block }
    END TRY
    BEGIN CATCH
    [ { sql_statement | statement_block } ]
    END CATCH
    [ ; ]

## 参数说明

- **sql_statement**

    除事务管理语句外的任意SQL语句。

- **statement_block**

    除事务管理语句外的任意SQL语句块。

## 示例

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
