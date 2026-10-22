# Character Types

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:27:46.114Z pushedAt=2026-09-21T03:30:11.052Z -->

Compared with the original openGauss, the modifications to character types in shark are primarily as follows:

1. Strings modified by the n\N prefix are of type nvarchar2, whereas in the original openGauss they are of type bpchar.
2. For the nvarchar data type, when no length is specified, it indicates a length of 1, i.e., nvarchar(1); in the original openGauss, when no length is specified, it indicates no constraint.
3. Support for nvarchar(max) is newly added, where max represents no constraint.

Examples:

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