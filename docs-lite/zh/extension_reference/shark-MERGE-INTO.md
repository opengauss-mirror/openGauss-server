# MERGE INTO

## 功能描述<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section11462163155618"></a>

通过MERGE INTO语句，将目标表和源表中数据针对关联条件进行匹配，若关联条件匹配时对目标表进行UPDATE，无法匹配时对目标表执行INSERT。此语法可以很方便地用来合并执行UPDATE和INSERT，避免多次执行。

## 注意事项<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section166351045574"></a>

- 进行MERGE INTO操作的用户需要同时拥有目标表的UPDATE和INSERT权限，以及源表的SELECT权限。
- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的MERGE INTO语法请参考章节[MERGE INTO](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/merge_into.html)。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section10551749579"></a>

```
MERGE [/*+ plan_hint */] INTO table_name [ partition_clause ] [ [ AS ] alias ]
USING { { table_name | view_name } | subquery } [ [ AS ] alias ] [table_hint_clause]
ON ( condition )
[
  WHEN MATCHED THEN
  UPDATE SET { column_name = { expression | subquery | DEFAULT } |
          ( column_name [, ...] ) = ( { expression | subquery | DEFAULT } [, ...] ) } [, ...]
  [ WHERE condition ]
]
[
  WHEN NOT MATCHED THEN
  INSERT [ ( column_name [, ...] ) ]
  [ OVERRIDING { SYSTEM | USER } VALUE ]
  { DEFAULT VALUES | VALUES ( { expression | subquery | DEFAULT } [, ...] ) [, ...] [ WHERE condition ] }
];
NOTICE: 'subquery' in the UPDATE and INSERT clauses are only avaliable in CENTRALIZED mode!
```

- 其中table\_hint子句table\_hint\_clause为：

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section1315653475"></a>

- **WITH ( <table_hint> [, ...] )**

    - 与SELECT子句相同，在未给出WITH时，table_hint仅支持给出一个hint，在给出WITH时，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即(hint1)、WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - 针对hint，会打印相关NOTICE信息。

- **OVERRIDING \{ SYSTEM | USER \} VALUE**

    该子句用于插入identity列时的行为控制。

    - `OVERRIDING SYSTEM VALUE`用于覆盖identity列生成的系统值，`OVERRIDING USER VALUE`用于覆盖用户自定义值。
    - 当开启`identity_insert`时可以插入用户值（包括default），否则只有使用`OVERRIDING SYSTEM VALUE`才能插入用户值，而insert overriding user value会忽略用户的值采用序列的值。

## 示例<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section3650125620712"></a>

- merge into语句基础示例
```sql
-- 创建目标表products和源表newproducts，并插入数据
openGauss=# CREATE TABLE products
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60)
);

openGauss=# INSERT INTO products VALUES (1501, 'vivitar 35mm', 'electrncs');
openGauss=# INSERT INTO products VALUES (1502, 'olympus is50', 'electrncs');
openGauss=# INSERT INTO products VALUES (1600, 'play gym', 'toys');
openGauss=# INSERT INTO products VALUES (1601, 'lamaze', 'toys');
openGauss=# INSERT INTO products VALUES (1666, 'harry potter', 'dvd');

openGauss=# CREATE TABLE newproducts
(
product_id INTEGER,
product_name VARCHAR2(60),
category VARCHAR2(60)
);

openGauss=# INSERT INTO newproducts VALUES (1502, 'olympus camera', 'electrncs');
openGauss=# INSERT INTO newproducts VALUES (1601, 'lamaze', 'toys');
openGauss=# INSERT INTO newproducts VALUES (1666, 'harry potter', 'toys');
openGauss=# INSERT INTO newproducts VALUES (1700, 'wait interface', 'books');

-- 进行MERGE INTO操作
openGauss=# MERGE INTO products p   
USING newproducts np   
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'  
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
MERGE 4

-- 查询更新后的结果
openGauss=# SELECT * FROM products ORDER BY product_id;
 product_id |  product_name  | category  
------------+----------------+-----------
       1501 | vivitar 35mm   | electrncs
       1502 | olympus camera | electrncs
       1600 | play gym       | toys
       1601 | lamaze         | toys
       1666 | harry potter   | toys
       1700 | wait interface | books
(6 rows)

-- 进行MERGE INTO操作
MERGE INTO products p 
USING newproducts np with (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
NOTICE:  The nowait option is currently ignored
MERGE 4

-- 查询更新后的结果
openGauss=# SELECT * FROM products with (nowait) ORDER BY product_id;
NOTICE:  The nowait option is currently ignored
 product_id |  product_name  | category  
------------+----------------+-----------
       1501 | vivitar 35mm   | electrncs
       1502 | olympus camera | electrncs
       1600 | play gym       | toys
       1601 | lamaze         | toys
       1666 | harry potter   | toys
       1700 | wait interface | books
(6 rows)

-- 进行MERGE INTO操作
MERGE INTO products p 
USING newproducts np (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
NOTICE:  The nowait option is currently ignored
MERGE 4

-- 查询更新后的结果
openGauss=# SELECT * FROM products with (nowait) ORDER BY product_id;
NOTICE:  The nowait option is currently ignored
 product_id |  product_name  | category  
------------+----------------+-----------
       1501 | vivitar 35mm   | electrncs
       1502 | olympus camera | electrncs
       1600 | play gym       | toys
       1601 | lamaze         | toys
       1666 | harry potter   | toys
       1700 | wait interface | books
(6 rows)

-- 删除表
openGauss=# DROP TABLE products;
openGauss=# DROP TABLE newproducts;
```

- identity_insert 与 OVERRIDING clause 示例
```sql
openGauss=# create table target_table ( id int identity, name varhcar(50), age int);
NOTICE:  CREATE TABLE will create implicit sequence "target_table_id_seq_identity" for serial column "target_table.id"
ERROR:  type "varhcar" does not exist
LINE 1: create table target_table ( id int identity, name varhcar(50...
                                                          ^
openGauss=# create table target_table ( id int identity, name varchar(50), age int);
NOTICE:  CREATE TABLE will create implicit sequence "target_table_id_seq_identity" for serial column "target_table.id"
CREATE TABLE
openGauss=# create table source_table (id int, name varchar(50), age int);
CREATE TABLE
openGauss=# insert into source_table values (10, 'zliu', 28), (11, 'sqi', 30);
INSERT 0 2
openGauss=# set identity_insert = off;
SET
openGauss=#  merge into target_table AS t using source_table AS s ON t.id = s.id
openGauss-#   when not matched then
openGauss-#       insert (name, age) values (s.name, s.age);
MERGE 2
openGauss=# merge into target_table AS t using source_table AS s ON t.id = s.id
openGauss-#   when not matched then
openGauss-#       insert (id, name, age) values (1, 'error', 21);
ERROR:  cannot insert a non-DEFAULT value into column "id"
DETAIL:  Column "id" is an identity column defined as "IDENTITY".
HINT:  Use OVERRIDING SYSTEM VALUE to override, Or turn on "identity_insert"
openGauss=# merge into target_table AS t using source_table AS s ON t.id = s.id
openGauss-#   when not matched then
openGauss-#       insert (id, name, age) OVERRIDING USER VALUE values (1, 'success', 22); -- 3
MERGE 2
openGauss=# set identity_insert = on;
SET
openGauss=# merge into target_table AS t using source_table AS s ON t.id = s.id
openGauss-#   when not matched then
openGauss-#       insert (id, name, age) values (s.id + 2, 'success', s.age);
MERGE 2
openGauss=# select * from target_table order by 1, 2,3;
 id |  name   | age 
----+---------+-----
  1 | zliu    |  28
  2 | sqi     |  30
  3 | success |  22
  4 | success |  22
 12 | success |  28
 13 | success |  30
(6 rows)

```

## 相关链接<a name="section156744489391"></a>

[MERGE INTO](https://docs.opengauss.org/zh/docs/latest-lite/sql_reference/merge_into.html)
