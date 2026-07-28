# 字符类型

相比于原始的openGauss，shark对于字符类型的修改主要为：

1. n\N前缀修饰的字符串，其类型为nvarchar2，原始openGauss为bpchar类型。
2. 针对nvarchar数据类型，没有给出长度时表示长度1，即nvarchar(1)，原始openGauss没有给出长度时表示无约束。
3. 新增支持nvarchar(max)，max代表无约束。

示例：

```
openGauss=# select n'abc';
 nvarchar2
-----------
 abc
(1 row)



openGauss=# create table test1(col1 nvarchar(max), col2 nvarchar(50), col3 nvarchar(1), col4 nvarchar);
CREATE TABLE
openGauss=# \d+ test1
                            Table "public.test1"
 Column |      Type      | Modifiers | Storage  | Stats target | Description
--------+----------------+-----------+----------+--------------+-------------
 col1   | nvarchar2(max) |           | extended |              |
 col2   | nvarchar2(50)  |           | extended |              |
 col3   | nvarchar2(1)   |           | extended |              |
 col4   | nvarchar2(1)   |           | extended |              |
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# create table test2(col1 nvarchar2(max), col2 nvarchar2(50), col3 nvarchar2(1), col4 nvarchar2);
CREATE TABLE
openGauss=# \d+ test2
                            Table "public.test2"
 Column |      Type      | Modifiers | Storage  | Stats target | Description
--------+----------------+-----------+----------+--------------+-------------
 col1   | nvarchar2(max) |           | extended |              |
 col2   | nvarchar2(50)  |           | extended |              |
 col3   | nvarchar2(1)   |           | extended |              |
 col4   | nvarchar2(1)   |           | extended |              |
Has OIDs: no
Options: orientation=row, compression=no

openGauss=# insert into test2 values('abcd', 'abcd', 'a', 'a');
INSERT 0 1
openGauss=# insert into test2(col4) values('abcd');
ERROR:  value too long for type nvarchar2(1)
CONTEXT:  referenced column: col4
```
