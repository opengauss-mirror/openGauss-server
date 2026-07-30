# INSERT

## 功能描述<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_s86b6c9741c7741d3976c5e358e8d5486"></a>

向表中添加一行或多行数据。

## 注意事项<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_sdd2da7fe44624eb99ee77013ff96c6bd"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的INSERT语法请参考章节[INSERT](https://docs.opengauss.org/zh/docs/latest/sql_reference/insert.html)。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283137542_zh-cn_topic_0237122167_zh-cn_topic_0059778902_se242be9719f44731b261539dbd42d7b9"></a>

```
[ WITH [ RECURSIVE ] with_query [, ...] ]
INSERT [/*+ plan_hint */] [INTO] table_name [partition_clause] [ AS alias ] [table_hint_clause] [ ( column_name [, ...] ) ]
    [ OVERRIDING { SYSTEM | USER } VALUE ]
    { DEFAULT VALUES
    | VALUES {( { expression | DEFAULT } [, ...] ) }[, ...] 
    | query }
    [ ON DUPLICATE KEY UPDATE { NOTHING | { column_name = { expression | DEFAULT } } [, ...] [ WHERE condition ] }]
    [ RETURNING {* | {output_expression [ [ AS ] output_name ] }[, ...]} ];
```

- 其中table\_hint子句table\_hint\_clause为：

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283137651_zh-cn_topic_0237122194_zh-cn_topic_0059778969_sf3e3262b89854b3d829a94054116838d"></a>

- **JOIN**

    JOIN包含 INNER JOIN，LEFT JOIN，RIGHT JOIN，FULL JOIN，CROSS JOIN。
  
- **WITH ( <table_hint> [, ...] )**

    - 不同于SELECT子句，针对单个hint，WITH可选，针对INSERT子句，WITH必选，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持，(hint1)不支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - 针对hint，会打印相关NOTICE信息。

- **OVERRIDING \{ SYSTEM | USER \} VALUE**

    该子句用于插入identity列时的行为控制。

    - `OVERRIDING SYSTEM VALUE`用于覆盖identity列生成的系统值，`OVERRIDING USER VALUE`用于覆盖用户自定义值。
    - 当开启`identity_insert`时可以插入用户值（包括default），否则只有使用`OVERRIDING SYSTEM VALUE`才能插入用户值，而insert overriding user value会忽略用户的值采用序列的值。

## table_hint子句示例

```sql
create table t1(c1 int, c2 int);

insert into t1 values(1, 2);

insert into t1 with (nowait) values(3, 4);
NOTICE:  The nowait option is currently ignored

insert into t1 with (nowait) (c1, c2) values(5, 6);
NOTICE:  The nowait option is currently ignored

insert into t1 with (xlock, nowait) values(7, 8);
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into t1 as table_t1 with (nowait) (c1, c2) values(9, 10);
NOTICE:  The nowait option is currently ignored

-- no into in insert statement
insert t1 values(1, 2);

insert t1 with (nowait) values(3, 4);
NOTICE:  The nowait option is currently ignored

insert t1 with (nowait) (c1, c2) values(5, 6);
NOTICE:  The nowait option is currently ignored

insert t1 with (xlock, nowait) values(7, 8);
NOTICE:  The xlock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert t1 as table_t1 with (nowait, nolock) (c1, c2) values(9, 10);
NOTICE:  The nowait option is currently ignored
NOTICE:  The nolock option is currently ignored

CREATE TABLE partition_table1
(
    WR_RETURNED_DATE_SK       INTEGER,
    WR_RETURNED_TIME_SK       INTEGER
)
PARTITION BY RANGE(WR_RETURNED_DATE_SK)
(
        PARTITION P1 VALUES LESS THAN(2450815),
        PARTITION P2 VALUES LESS THAN(2451179),
        PARTITION P8 VALUES LESS THAN(MAXVALUE)
);

insert into partition_table1 with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition (p1) with (nolock, nowait) values(2450000, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition for (2451176) with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

insert into partition_table1 partition for (2451176) as table1_alias with (nolock, nowait) values(2451176, 1);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
```

## identity_insert 与 OVERRIDING clause 示例
```sql
openGauss=# create extension shark;
CREATE EXTENSION
openGauss=#  CREATE TABLE book(bookId int IDENTITY, bookname NVARCHAR(50), author NVARCHAR(50));
NOTICE:  CREATE TABLE will create implicit sequence "book_bookid_seq_identity" for serial column "book.bookid"
CREATE TABLE
openGauss=# \d+ book
                                               Table "public.book"
  Column  |     Type      |                   Modifiers                   | Storage  | Stats target | Description 
----------+---------------+-----------------------------------------------+----------+--------------+-------------
 bookid   | integer       | not null identity                             | plain    |              | 
 bookname | nvarchar2(50) | character set UTF8 collate utf8mb4_general_ci | extended |              | 
 author   | nvarchar2(50) | character set UTF8 collate utf8mb4_general_ci | extended |              | 
Has OIDs: no
Options: orientation=row, compression=no, collate=1537
Character Set: UTF8
Collate: utf8mb4_general_ci

openGauss=# set identity_insert = off;
SET
openGauss=# INSERT INTO book VALUES (2, 'xxx', 'xxx'); -- error;
ERROR:  INSERT has more expressions than target columns
LINE 1: INSERT INTO book VALUES (2, 'xxx', 'xxx');
                                           ^
openGauss=# INSERT INTO book (bookid, bookname, author) VALUES(11111, 'xxxx', 'xxx'); -- error, turn on identity_insert or use OVERRIDING
ERROR:  cannot insert a non-DEFAULT value into column "bookid"
DETAIL:  Column "bookid" is an identity column defined as "IDENTITY".
HINT:  Use OVERRIDING SYSTEM VALUE to override, Or turn on "identity_insert".
openGauss=# INSERT INTO book (bookid, bookname, author) OVERRIDING SYSTEM VALUE VALUES (33, 'xxx', 'xxx'); -- success; -- 33
INSERT 0 1
openGauss=# INSERT INTO book (bookid, bookname, author) OVERRIDING USER VALUE VALUES (11111, 'xxx', 'xxx'); -- success; -- 1
INSERT 0 1
openGauss=# set identity_insert = on;
SET
openGauss=# INSERT INTO book VALUES (44, 'xxx', 'xxx'); -- error;
ERROR:  INSERT has more expressions than target columns
LINE 1: INSERT INTO book VALUES (44, 'xxx', 'xxx');
                                            ^
openGauss=# INSERT INTO book (bookid, bookname, author) OVERRIDING USER VALUE VALUES (11111, 'xxx', 'xxx'); -- success; -- 2
INSERT 0 1
openGauss=# INSERT INTO book (bookid, bookname, author) OVERRIDING SYSTEM VALUE VALUES (55, 'xxx', 'xxx'); -- success; -- 55
INSERT 0 1
openGauss=# set identity_insert = off;
SET
openGauss=# select * from book order by 1, 2;
 bookid | bookname | author 
--------+----------+--------
      1 | xxx      | xxx
      2 | xxx      | xxx
     33 | xxx      | xxx
     55 | xxx      | xxx
(4 rows)

```

## 相关链接<a name="section156744489391"></a>

[INSERT](https://docs.opengauss.org/zh/docs/latest/sql_reference/insert.html)
