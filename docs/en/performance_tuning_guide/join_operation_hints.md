# Join Operation Hints<a name="EN-US_TOPIC_0245374569"></a>

## Function<a name="en-us_topic_0237121534_section290819468377"></a>

These hints specify the join method, which can be nested loop join, hash join, or merge join.

## Syntax<a name="en-us_topic_0237121534_section3654114133815"></a>

```
[no] nestloop|hashjoin|mergejoin(table_list)
```

## Parameter Description<a name="en-us_topic_0237121534_section35948678143011"></a>

- **no**  indicates that the specified hint will not be used for a join.

- _table\_list_  specifies the tables to be joined. The values are the same as those of  **[join\_table\_list](join_order_hints.md#en-us_topic_0237121533_section1280444714345)**  but contain no parentheses.

For example:

**no nestloop\(t1 t2 t3\)**:  **nestloop**  is not used for joining  **t1**,  **t2**, and  **t3**. The three tables may be joined in either of the two ways: Join  **t2**  and  **t3**, and then  **t1**; join  **t1**  and  **t2**, and then  **t3**. This hint takes effect only for the last join. If necessary, you can hint other joins. For example, you can add  **no nestloop\(t2 t3\)**  to join  **t2**  and  **t3**  first and to forbid the use of  **nestloop**.

## Example<a name="en-us_topic_0237121534_section1127715590585"></a>

Hint the query plan in  [Example](plan_hint_optimization_overview.md#en-us_topic_0237121532_section671421102912)  as follows:

```
explain
select /*+ nestloop(store_sales store_returns item) */ i_product_name product_name ...
```

**nestloop**  is used for the last join between  **store\_sales**,  **store\_returns**, and  **item**. The optimized plan is as follows:

![](figures/en-us_image_0253032870.png)
