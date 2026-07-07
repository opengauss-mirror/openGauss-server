# Optimizing Statistics<a name="EN-US_TOPIC_0245374561"></a>

## Background<a name="en-us_topic_0237121526_en-us_topic_0073253803_en-us_topic_0062578363_section5394576019248"></a>

openGauss generates optimal execution plans based on the cost estimation. Optimizers need to estimate the number of data rows and the cost based on statistics collected using  **ANALYZE**. Therefore, the statistics is vital for the estimation of the number of rows and cost. Global statistics are collected using  **ANALYZE**:  **relpages**  and  **reltuples**  in the  **pg\_class**  table;  **stadistinct**,  **stanullfrac**,  **stanumbersN**,  **stavaluesN**, and  **histogram\_bounds**  in the  **pg\_statistic**  table.

## Example 1: Poor Query Performance Due to the Lack of Statistics<a name="en-us_topic_0237121526_en-us_topic_0073253803_en-us_topic_0062578363_section3078446519518"></a>

In most cases, the lack of statistics about tables or columns involved in the query greatly affects the query performance.

The table structure is as follows:

```
CREATE TABLE LINEITEM
(
L_ORDERKEY         BIGINT        NOT NULL
, L_PARTKEY        BIGINT        NOT NULL
, L_SUPPKEY        BIGINT        NOT NULL
, L_LINENUMBER     BIGINT        NOT NULL
, L_QUANTITY       DECIMAL(15,2) NOT NULL
, L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL
, L_DISCOUNT       DECIMAL(15,2) NOT NULL
, L_TAX            DECIMAL(15,2) NOT NULL
, L_RETURNFLAG     CHAR(1)       NOT NULL
, L_LINESTATUS     CHAR(1)       NOT NULL
, L_SHIPDATE       DATE          NOT NULL
, L_COMMITDATE     DATE          NOT NULL
, L_RECEIPTDATE    DATE          NOT NULL
, L_SHIPINSTRUCT   CHAR(25)      NOT NULL
, L_SHIPMODE       CHAR(10)      NOT NULL
, L_COMMENT        VARCHAR(44)   NOT NULL
) with (orientation = column, COMPRESSION = MIDDLE);

CREATE TABLE ORDERS
(
O_ORDERKEY        BIGINT        NOT NULL
, O_CUSTKEY       BIGINT        NOT NULL
, O_ORDERSTATUS   CHAR(1)       NOT NULL
, O_TOTALPRICE    DECIMAL(15,2) NOT NULL
, O_ORDERDATE     DATE NOT NULL
, O_ORDERPRIORITY CHAR(15)      NOT NULL
, O_CLERK         CHAR(15)      NOT NULL
, O_SHIPPRIORITY  BIGINT        NOT NULL
, O_COMMENT       VARCHAR(79)   NOT NULL
)with (orientation = column, COMPRESSION = MIDDLE);
```

The query statements are as follows:

```
explain verbose select
count(*) as numwait 
from
lineitem l1,
orders 
where
o_orderkey = l1.l_orderkey
and o_orderstatus = 'F'
and l1.l_receiptdate > l1.l_commitdate
and not exists (
select
*
from
lineitem l3
where
l3.l_orderkey = l1.l_orderkey
and l3.l_suppkey <> l1.l_suppkey
and l3.l_receiptdate > l3.l_commitdate
)
order by
numwait desc;
```

If such an issue occurs, you can use the following methods to check whether statistics in tables or columns has been collected using  **ANALYZE**.

1. Execute  **EXPLAIN VERBOSE**  to analyze the execution plan and check the warning information:

    ```
    WARNING:Statistics in some tables or columns(public.lineitem.l_receiptdate, public.lineitem.l_commitdate, public.lineitem.l_orderkey, public.lineitem.l_suppkey, public.orders.o_orderstatus, public.orders.o_orderkey) are not collected.
    HINT:Do analyze for them in order to generate optimized plan.
    ```

2. Check whether the following information exists in the log file in the  **pg\_log**  directory. If it does, the poor query performance was caused by the lack of statistics in some tables or columns.

    ```
    2017-06-14 17:28:30.336 CST 140644024579856 20971684 [BACKEND] LOG:Statistics in some tables or columns(public.lineitem.l_receiptdate, public.lineitem.l_commitdate, public.lineitem.l_orderkey, public.linei
    tem.l_suppkey, public.orders.o_orderstatus, public.orders.o_orderkey) are not collected.
    2017-06-14 17:28:30.336 CST 140644024579856 20971684 [BACKEND] HINT:Do analyze for them in order to generate optimized plan.
    ```

By using any of the preceding methods, you can identify tables or columns whose statistics have not been collected using  **ANALYZE**. You can execute  **ANALYZE**  to warnings or tables and columns recorded in logs to resolve the problem.
