# SELECT

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:34:36.067Z pushedAt=2026-09-22T07:14:37.987Z -->

## Function Description<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_s65596fb5f1d44a428e41dd508d2044a7"></a>

SELECT is used to retrieve data from tables or views.

The SELECT statement acts as a filter overlaid on database tables, utilizing SQL keywords to filter out the data required by the user from the data tables.

## Notes<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_s42c37979749545719ac9114594f45d93"></a>

- This section only contains the syntax newly added by Shark. The original openGauss syntax has not been deleted or modified. For the original openGauss SELECT syntax, see [SELECT](https://docs.opengauss.org/en/docs/latest/sql_reference/select.html).
- The TOP clause is newly supported.
- The table_hint clause is newly supported.

## Syntax Format<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_sb7329222602d46fe944bf6c300931dd2"></a>

- Query data

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

- The TOP clause top_clause is:

    ```
    TOP (expression) [ PERCENT ] [ WITH TIES ]
    ```

- The table_hint clause table_hint_clause is:

    ```
    [ WITH ] ( <table_hint> [, ...] ) 
    ```

## Parameter Description<a name="zh-cn_topic_0283136463_zh-cn_topic_0237122184_zh-cn_topic_0059777449_sa812f65b8e8c4c638ec7840697222ddc"></a>

- **TOP (expression) [ PERCENT ] [ WITH TIES ]**

    The TOP clause limits the number of rows or the percentage of rows returned in the query result set. When the TOP clause is used together with the ORDER BY clause, the result set is limited to the specified number of ordered rows; otherwise, the TOP clause returns the specified number of rows in an undefined order. The PERCENT keyword can be used to specify that the number of rows returned is a percentage of the query result set. The WITH TIES keyword indicates that the specified number of rows is returned, along with all rows that have the same values as the last row when the result set is ordered.

    > [!NOTE]
    >
    >- In D-compatible mode, when the PERCENT keyword is used, the PERCENT value ranges from 0 to 100. Values outside this range will cause an error.
    >- In D-compatible mode, when the product of the specified percentage and the total number of rows in the result set is not an integer, it is rounded up to the nearest integer. When the specified number of rows is not an integer, it is rounded to the nearest integer.
    >- In D-compatible mode, WITH TIES must be used together with the ORDER BY clause; otherwise, an error is reported. This rule applies to both the TOP clause and the FETCH clause.
    >- In D-compatible mode, the TOP clause, the LIMIT clause, and the FETCH clause cannot be used simultaneously.

- **[ WITH ] ( <table_hint> [, ...] )**

    - For the SELECT clause, when WITH is not specified, table_hint supports only one hint; when WITH is specified, table_hint supports a list of options, with the list separated by commas or spaces. That is, (hint1), WITH (hint1), WITH (hint1, hint2, ...), and WITH (hint1 hint2 ...) are all supported.

    - The supported hints include NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, and NOEXPAND.

    - When the above hints need to be used as identifiers, such as column names or variable names, d_format_behavior_compat_options must be set to 'enable_table_hint_identifier'. The default value of this variable is d_format_behavior_compat_options = ''.

    - All hints are supported only syntactically and have no actual semantics.

    - The table_hint clause is located within the from_item clause.

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

    - In JOIN scenarios, hints can be specified separately for each table.

    - The SELECT INTO scenario supports the relevant table_hint syntax.

    - Relevant NOTICE messages are printed for hints.

## TOP Clause Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## table_hint Clause Example<a name="zh-cn_topic_0283136578_zh-cn_topic_0237122106_zh-cn_topic_0059777455_s985289833081489e9d77c485755bd362"></a>

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

## Related Links<a name="section156744489391"></a>

[SELECT](https://docs.opengauss.org/en/docs/latest/sql_reference/select.html)