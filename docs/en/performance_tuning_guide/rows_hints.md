# Rows Hints<a name="EN-US_TOPIC_0245374570"></a>

## Function<a name="en-us_topic_0237121535_section290819468377"></a>

These hints specify the number of rows in an intermediate result set. Both absolute values and relative values are supported.

## Syntax<a name="en-us_topic_0237121535_section6280155403"></a>

```
rows(table_list #|+|-|* const)
```

## Parameter Description<a name="en-us_topic_0237121535_section55696776143642"></a>

- **\#**,  **+**,  **-**, and  **\***  are operators used for hinting the estimation.  **\#**  indicates that the original estimation is used without any calculation.  **+**,  **-**, and  **\***  indicate that the original estimation is calculated using these operators. The minimum calculation result is 1.  _table\_list_  specifies the tables to be joined. The values are the same as those of  **[table\_list](join_operation_hints.md)**  in  [Join Operation Hints](join_operation_hints.md).

- _const_  can be any non-negative number and supports scientific notation.

For example:

**rows\(t1 \#5\)**: The result set of  **t1**  is five rows.

**rows\(t1 t2 t3 \*1000\)**: Multiply the result set of joined  **t1**,  **t2**, and  **t3**  by 1000.

## Suggestion<a name="en-us_topic_0237121535_section99281150122819"></a>

- The hint using  **\***  for two tables is recommended. This hint will be triggered if the two tables appear on two sides of a join. For example, if the hint is  **rows\(t1 t2 \* 3\)**, the join result of  **\(t1 t3 t4\)**  and  **\(t2 t5 t6\)**  will be multiplied by 3 because  **t1**  and  **t2**  appear on both sides of the join.
- **rows**  hints can be specified for the result sets of a single table, multiple tables, function tables, and subquery scan tables.

## Example<a name="en-us_topic_0237121535_section1127715590585"></a>

Hint the query plan in  [Example](plan_hint_optimization_overview.md#en-us_topic_0237121532_section671421102912)  as follows:

```
explain
select /*+ rows(store_sales store_returns *50) */ i_product_name product_name ...
```

Multiply the result set of joined  **store\_sales**  and  **store\_returns**  by 50. The optimized plan is as follows:

![](figures/en-us_image_0253036670.png)
