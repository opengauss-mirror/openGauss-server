# INDEX HINTS

## Precautions

Compatibility restrictions:

- This function takes effect only in B-compatible mode.

- Currently, only some syntax and functions of the complete syntax in the MySQL database are supported.

- The syntax takes effect only when this function is used in query statements.

## Function

It specifies a name of an index expected to be used when a specified table is scanned.

- When USE INDEX is used to specify an index, the cost of scanning indexes and that of sequential scanning are considered. An index with a lower cost is used.

- If an index is specified by FORCE INDEX, the index is forcibly used for scanning.

- FORCE INDEX and USE INDEX cannot be used in the same table at the same time.

- Using multiple **index\_hints** is equivalent to writing multiple index names in **index\_list**.

## Syntax

```
tbl_name [ partition_clause ] [ [ AS ] alias ] [ index_hint_list ]

index_hint_list:
    index_hint [ index_hint ]
index_hint:
    USE {INDEX | KEY} ( [ index_list ] )
  | FORCE { INDEX | KEY } ( index_list )
index_list:
    index_name [ , index_name ] ...
```

## Parameter Description

- **index\_list**

  Names of indexes, which are separated by commas (,).

- **tbl\_name**

  Table name.

## Example

```sql
openGauss=# explain (costs off,verbose true  )select * from db_1097149_tb force key (index_1097149_4) where col2= 3 and col4 = 'a';
                        QUERY PLAN                        
----------------------------------------------------------
 Index Scan using index_1097149_4 on public.db_1097149_tb
   Output: col1, col2, col3, col4
   Index Cond: ((db_1097149_tb.col4)::text = 'a'::text)
   Filter: (db_1097149_tb.col2 = 3)
(4 rows)
```
