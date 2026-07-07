# Join Order Hints<a name="EN-US_TOPIC_0245374568"></a>

## Function<a name="en-us_topic_0237121533_section97491741123412"></a>

Theses hints specify the join order and outer/inner tables.

## Syntax<a name="en-us_topic_0237121533_section128191729143517"></a>

- Specify only the join order.

```
leading(join_table_list) 
```

- Specify the join order and outer/inner tables. The outer/inner tables are specified by the outermost parentheses.

```
leading((join_table_list)) 
```

## Parameter Description<a name="en-us_topic_0237121533_section1280444714345"></a>

_join\_table\_list_  specifies the tables to be joined. The values can be table names or table aliases. If a subquery is pulled up, the value can also be the subquery alias. Separate the values with spaces. You can add parentheses to specify the join priorities of tables.

>[!TIP]NOTICE   
>A table name or alias can only be a string without a schema name.  
>An alias \(if any\) is used to represent a table.  

To prevent semantic errors, tables in the list must meet the following requirements:

- The tables must exist in the query or its subquery to be pulled up.
- The table names must be unique in the query or subquery to be pulled up. If they are not, their aliases must be unique.
- A table appears only once in the list.
- An alias \(if any\) is used to represent a table.

For example:

**leading\(t1 t2 t3 t4 t5\)**:  **t1**,  **t2**,  **t3**,  **t4**, and  **t5**  are joined. The join order and outer/inner tables are not specified.

**leading\(\(t1 t2 t3 t4 t5\)\)**:  **t1**,  **t2**,  **t3**,  **t4**, and  **t5**  are joined in sequence. The table on the right is used as the inner table in each join.

**leading\(t1 \(t2 t3 t4\) t5\)**: First,  **t2**,  **t3**, and  **t4**  are joined and the outer/inner tables are not specified. Then, the result is joined with  **t1**  and  **t5**, and the outer/inner tables are not specified.

**leading\(\(t1 \(t2 t3 t4\) t5\)\)**: First,  **t2**,  **t3**, and  **t4**  are joined and the outer/inner tables are not specified. Then, the result is joined with  **t1**, and  **\(t2 t3 t4\)**  is used as the inner table. Finally, the result is joined with  **t5**, and  **t5**  is used as the inner table.

**leading\(\(t1 \(t2 t3\) t4 t5\)\) leading\(\(t3 t2\)\)**: First,  **t2**  and  **t3**  are joined and  **t2**  is used as the inner table. Then, the result is joined with  **t1**, and  **\(t2 t3\)**  is used as the inner table. Finally, the result is joined with  **t4**  and then  **t5**, and the table on the right in each join is used as the inner table.

## Example<a name="en-us_topic_0237121533_section1127715590585"></a>

Hint the query plan in  [Example](plan_hint_optimization_overview.md#en-us_topic_0237121532_section671421102912)  as follows:

```
explain
select /*+ leading((((((store_sales store) promotion) item) customer) ad2) store_returns) leading((store store_sales))*/ i_product_name product_name ...
```

First,  **store\_sales**  and  **store**  are joined and  **store\_sales**  is the inner table. Then, the result is joined with  **promotion**,  **item**,  **customer**,  **ad2**, and  **store\_returns**  in sequence. The optimized plan is as follows:

![](figures/en-us_image_0253030479.png)

For details about the warning at the top of the plan, see  [Hint Errors, Conflicts, and Other Warnings](hint_errors_conflicts_and_other_warnings.md).
