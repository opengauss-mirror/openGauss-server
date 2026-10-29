# MERGE INTO

<!-- md-trans-meta sourceCommit=unknown translatedAt=2026-07-30T06:35:02.127Z pushedAt=2026-09-22T03:58:42.153Z -->

## Function Description<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section11462163155618"></a>

The MERGE INTO statement matches data in the target table and the source table based on the join condition. If the join condition is matched, the target table is updated; otherwise, the target table is inserted into. This syntax conveniently combines UPDATE and INSERT operations, avoiding multiple executions.

## Precautions<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section166351045574"></a>

- The user performing the MERGE INTO operation must have both the UPDATE and INSERT privileges on the target table, as well as the SELECT privilege on the source table.
- This section only contains the syntax newly added by shark. The original openGauss syntax has not been deleted or modified. For the original openGauss MERGE INTO syntax, see [MERGE INTO](https://docs.opengauss.org/en/docs/latest/sql_reference/merge_into.html).
- The table_hint clause is newly supported.

## Syntax<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section10551749579"></a>

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
  INSERT { DEFAULT VALUES |
  [ ( column_name [, ...] ) ] VALUES ( { expression | subquery | DEFAULT } [, ...] ) [, ...] [ WHERE condition ] }
];
NOTICE: 'subquery' in the UPDATE and INSERT clauses are only avaliable in CENTRALIZED mode!
```

- The table_hint clause table_hint_clause is:

    ```
    WITH ( <table_hint> [, ...] ) 
    ```

## Parameter Description<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section1315653475"></a>

- **WITH ( <table_hint> [, ...] )**

    - Same as the SELECT clause, when WITH is not specified, table_hint supports only one hint; when WITH is specified, table_hint supports a list of options separated by commas or spaces, that is, (hint1), WITH (hint1), WITH (hint1, hint2, ...), and WITH (hint1 hint2 ...) are all supported.

    - Supported hints include NOLOCK, READUNCOMMITTED, UPDLOCK, REPEATABLEREAD, SERIALIZABLE, READCOMMITTED, TABLOCK, TABLOCKX, PAGLOCK, ROWLOCK, NOWAIT, READPAST, XLOCK, SNAPSHOT, and NOEXPAND.

    - When the above hints need to be used as identifiers, such as column names or variable names, set d_format_behavior_compat_options = 'enable_table_hint_identifier'. The default value of this variable is d_format_behavior_compat_options = ''.

    - All hints are supported only syntactically and have no actual semantics.

    - For hints, relevant NOTICE messages will be printed.

## Examples<a name="zh-cn_topic_0283137308_zh-cn_topic_0237122170_section3650125620712"></a>

```
-- Create the target table products and the source table newproducts, and insert data.
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

-- Perform the MERGE INTO operation.
openGauss=# MERGE INTO products p   
USING newproducts np   
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'  
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
MERGE 4

-- Query the updated results.
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

-- Perform the MERGE INTO operation.
MERGE INTO products p 
USING newproducts np with (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
NOTICE:  The nowait option is currently ignored
MERGE 4

-- Query the updated result.
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

-- Perform the MERGE INTO operation.
MERGE INTO products p 
USING newproducts np (nowait) 
ON (p.product_id = np.product_id)   
WHEN MATCHED THEN  
  UPDATE SET p.product_name = np.product_name, p.category = np.category WHERE p.product_name != 'play gym'
WHEN NOT MATCHED THEN  
  INSERT VALUES (np.product_id, np.product_name, np.category) WHERE np.category = 'books';
NOTICE:  The nowait option is currently ignored
MERGE 4

-- Query the updated result.
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

-- Drop the table.
openGauss=# DROP TABLE products;
openGauss=# DROP TABLE newproducts;
```

## Related Links<a name="section156744489391"></a>

[MERGE INTO](https://docs.opengauss.org/en/docs/latest/sql_reference/merge_into.html)