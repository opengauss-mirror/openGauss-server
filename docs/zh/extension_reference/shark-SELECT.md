# SELECT

## 功能描述<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_s65596fb5f1d44a428e41dd508d2044a7"></a>

SELECT用于从表或视图中取出数据。

SELECT语句就像叠加在数据库表上的过滤器，利用SQL关键字从数据表中过滤出用户需要的数据。

## 注意事项<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_s42c37979749545719ac9114594f45d93"></a>

- 本章节只包含shark新增的语法，原openGauss的语法未做删除和修改。原openGauss的SELECT语法请参考章节[SELECT](https://docs.opengauss.org/zh/docs/latest/sql_reference/select.html)。
- 新增支持TOP子句。
- 新增支持table_hint子句。

## 语法格式<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_sb7329222602d46fe944bf6c300931dd2"></a>

- 查询数据

```
[ WITH [ RECURSIVE ] with_query [, ...] ]
SELECT [/*+ plan_hint */] [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
[ top_clause ]
{ * | {expression [ [ AS ] output_name ]} [, ...] }
[ into_option ]
[ FROM from_item [, ...] ]
[ WHERE condition ]
[ [ START WITH condition ] CONNECT BY [NOCYCLE] condition [ ORDER SIBLINGS BY expression ] ]
[ GROUP BY grouping_element [, ...] ]
[ HAVING condition [, ...] ]
[ WINDOW {window_name AS ( window_definition )} [, ...] ]
[ { UNION | INTERSECT | EXCEPT | MINUS } [ ALL | DISTINCT ] select ]
[ ORDER BY {expression [ [ ASC | DESC | USING operator ] | nlssort_expression_clause ] [ NULLS { FIRST | LAST } ]} [, ...] ]
[ LIMIT { [offset,] count | ALL } ]
[ OFFSET start [ ROW | ROWS ] ]
[ FETCH { FIRST | NEXT } [ count ] [PERCENT] { ROW | ROWS } { ONLY | WITH TIES } ]
[ into_option ]
[ {FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT | WAIT N]} [...] ]
[ into_option ];
```

- 其中TOP子句top\_clause为：

    ```
    TOP (expression) [ PERCENT ] [ WITH TIES ]
    ```

- 其中table\_hint子句table\_hint\_clause为：

    ```
    [ WITH ] ( <table_hint> [, ...] ) 
    ```

## 参数说明<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_sa812f65b8e8c4c638ec7840697222ddc"></a>

- **TOP (expression) [ PERCENT ] [ WITH TIES ]**

    TOP子句限制查询结果集中返回指定的行数或行数的百分比。将 TOP 子句与 ORDER BY 子句一起使用时，结果集被限制为有序的指定行数，否则TOP子句按未定义的顺序返回指定行数。使用PERCENT关键字可以指定返回的行数为查询结果集的百分比。WITH TIES关键字表示返回指定的行数以及结果集有序情况下所有与最后一行相同的值。

    >[!NOTE]说明
    >
    >- 兼容D库中，使用PERCENT关键字时，PERCENT值的范围为[0,100]，超范围报错。
    >- 兼容D库中，指定的百分比数和结果集总数相乘后不为整数时，向上取整到最接近的整数值。指定的行数不为整数时，四舍五入到最接近的整数值。
    >- 兼容D库中，WITH TIES必须和ORDER BY 子句一起使用，否则报错。这一规则适用于TOP子句和FETCH子句。 
    >- 兼容D库中，TOP子句和LIMIT子句以及FETCH子句不能同时使用。

- **[ WITH ] ( <table_hint> [, ...] )**

    - 针对SELECT子句，在未给出WITH时，table_hint仅支持给出一个hint，在给出WITH时，table_hint支持给出一个列表选项，列表通过逗号或者空格分隔，即(hint1)、WITH (hint1)、WITH (hint1, hint2, ...)、WITH (hint1 hint2 ...)均支持。

    - 支持的hint包括NOLOCK、READUNCOMMITTED、UPDLOCK、REPEATABLEREAD、SERIALIZABLE、READCOMMITTED、TABLOCK、TABLOCKX、PAGLOCK、ROWLOCK、NOWAIT、READPAST、XLOCK、SNAPSHOT、NOEXPAND。

    - 当上述hint需要当做标识符，用于列名、变量名等，需要设置d_format_behavior_compat_options = 'enable_table_hint_identifier'，该变量默认值d_format_behavior_compat_options = ''。

    - 所有的hint仅语法支持，无实际含义。

    - table_hint子句位于from_item子句中。

    ```
    {[ ONLY ] table_name [ * ] [ partition_clause ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    [ TABLESAMPLE sampling_method ( argument [, ...] ) [ REPEATABLE ( seed ) ] ] [table_hint_clause]
    [TIMECAPSULE {TIMESTAMP|CSN} expression]
    |( select ) [ AS ] alias [ ( column_alias [, ...] ) ]
    |with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    |function_name ( [ argument [, ...] ] ) [ AS ] alias [ ( column_alias [, ...] | column_definition [, ...] ) ]
    |function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )
    |from_item [ NATURAL ] join_type from_item [ ON join_condition | USING ( join_column [, ...] ) ]
    |rotate_clause
    |notrotate_clause
    |lateral lateral_subquery [ AS ] alias
    |from_item cross apply lateral_subquery [ AS ] alias
    |from_item outer apply lateral_subquery [ AS ] alias}
    ```

    - 对于JOIN场景，每个表均支持单独给出hint。

    - SELECT INTO场景支持相关table_hint语法。

    - 针对hint，会打印相关NOTICE信息。

## TOP子句示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```sql
opengauss=# CREATE TABLE Products(QtyAvailable smallint, UnitPrice money, InventoryValue AS (QtyAvailable * UnitPrice) PERSISTED);
CREATE TABLE
opengauss=# INSERT INTO Products(QtyAvailable, UnitPrice) VALUES (25, 2.00), (10, 1.5), (25, 2.00), (10, 1.5), (10, 1.5);
INSERT 0 5
opengauss=# select * from Products ;
 qtyavailable | unitprice | inventoryvalue 
--------------+-----------+----------------
           25 |     $2.00 |         $50.00
           10 |     $1.50 |         $15.00
           25 |     $2.00 |         $50.00
           10 |     $1.50 |         $15.00
           10 |     $1.50 |         $15.00
(5 rows)

opengauss=# select TOP 4 * from Products ORDER BY qtyavailable;
 qtyavailable | unitprice | inventoryvalue 
--------------+-----------+----------------
           10 |     $1.50 |         $15.00
           10 |     $1.50 |         $15.00
           10 |     $1.50 |         $15.00
           25 |     $2.00 |         $50.00
(4 rows)

opengauss=# select TOP 2 PERCENT * from Products ORDER BY qtyavailable;
 qtyavailable | unitprice | inventoryvalue 
--------------+-----------+----------------
           10 |     $1.50 |         $15.00
(1 row)

opengauss=# select TOP 2 PERCENT WITH TIES * from Products ORDER BY qtyavailable;
 qtyavailable | unitprice | inventoryvalue 
--------------+-----------+----------------
           10 |     $1.50 |         $15.00
           10 |     $1.50 |         $15.00
           10 |     $1.50 |         $15.00
(3 rows)

```

## table_hint子句示例<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

```
create table test_hint(id int);

select * from test_hint t (nolock);
NOTICE:  The nolock option is currently ignored
 id 
----
(0 rows)

select * from test_hint (readuncommitted);
NOTICE:  The readuncommitted option is currently ignored
 id 
----
(0 rows)

select * from test_hint t with (nolock);
NOTICE:  The nolock option is currently ignored
 id 
----
(0 rows)

select * from test_hint t with (nolock, nowait);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
 id 
----
(0 rows)

select * from test_hint with (nolock nowait);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
 id 
----
(0 rows)

--join
create table t1(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t2(col1 int, col2 int, col3 int, col4 int, col5 int);

select * from t1 a (nolock) left join t2 b (nolock) on a.col1 = b.col1;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nolock option is currently ignored
 col1 | col2 | col3 | col4 | col5 | col1 | col2 | col3 | col4 | col5 
------+------+------+------+------+------+------+------+------+------
(0 rows)

select * from t1 with (nolock) left join t2 with (nolock) on t1.col1 = t2.col1;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nolock option is currently ignored
 col1 | col2 | col3 | col4 | col5 | col1 | col2 | col3 | col4 | col5 
------+------+------+------+------+------+------+------+------+------
(0 rows)

select * from t1 a full join t2 b (nolock) on a.col4 = b.col4 where a.col1 > 10 and b.col4 < 100;
NOTICE:  The nolock option is currently ignored
 col1 | col2 | col3 | col4 | col5 | col1 | col2 | col3 | col4 | col5 
------+------+------+------+------+------+------+------+------+------
(0 rows)

-- select into
create table t3(col1 int, col2 int, col3 int, col4 int, col5 int);
create table t4(c1 int, c2 int, c3 int, c4 int, c5 int);

select * into test1 from t3 with (nolock, nowait) where col1 > 10;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

select * into table test2 from t3 with (nolock, nowait) where col1 > 10;
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored

select * into test3 from t3 with (nolock, nowait) cross join t4 with (nolock, nowait);
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
NOTICE:  The nolock option is currently ignored
NOTICE:  The nowait option is currently ignored
```

## 相关链接<a name="section156744489391"></a>

[SELECT](https://docs.opengauss.org/zh/docs/latest/sql_reference/select.html)
