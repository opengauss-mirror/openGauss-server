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

```sql
openGauss=# create database testd dbcompatibility = 'D';
CREATE DATABASE
openGauss=# \c testd
Non-SSL connection (SSL connection is recommended when requiring high-security)
You are now connected to database "testd" as user "omm".
testd=# create extension shark;
CREATE EXTENSION
testd=# set xact_abort = off;
SET
testd=# create table test_3(a int);
CREATE TABLE
testd=# begin try;
BEGIN TRY
testd=# insert into test_3 values(2);
INSERT 0 1
testd=# select 1/0;
ERROR:  division by zero
testd=# end try begin catch;
END TRY BEGIN CATCH
testd=# insert into test_3 values(3);
NOTICE:  current try block is successfully, commands ignored until end of catch block
testd=# select 1/0;
NOTICE:  current try block is successfully, commands ignored until end of catch block
testd=# select * from test_3;
NOTICE:  current try block is successfully, commands ignored until end of catch block
testd=# end catch;
END CATCH
testd=# select * from test_3;
 a
---
 2
(1 row)

testd=# drop table test_3;
DROP TABLE
testd=# set xact_abort = on;
SET
testd=# create table test_3(a int);
CREATE TABLE
testd=# begin try;
BEGIN TRY
testd=# insert into test_3 values(2);
INSERT 0 1
testd=# select 1/0;
ERROR:  division by zero
testd=# end try begin catch;
END TRY BEGIN CATCH
testd=# insert into test_3 values(3);
INSERT 0 1
testd=# select 1/0;
ERROR:  division by zero
testd=# select * from test_3;
ERROR:  current catch block is failed, commands ignored until end of catch block
testd=# end catch;
ROLLBACK
testd=# select * from test_3;
 a
---
(0 rows)

testd=# drop table test_3;
DROP TABLE
```
