# Reviewing and Modifying a Table Definition<a name="EN-US_TOPIC_0245374549"></a>

## Overview<a name="EN-US_TOPIC_0245374550"></a>

To properly define a table, you must:

1. **Reduce the data volume scanned**  by using the partition pruning mechanism.
2. **Minimize random I/Os**  by using clustering or partial clustering.

The table definition is created during the database design and is reviewed and modified during the SQL statement optimization.

## Selecting a Storage Model<a name="EN-US_TOPIC_0000001119972374"></a>

During database design, some key factors about table design will greatly affect the subsequent query performance of the database. Table design affects data storage as well. A good table design reduces I/O operations and minimizes memory usage, improving the query performance.

Selecting a model for table storage is the first step of table definition. Select a proper storage model for your service based on the following table:

<a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_table39547486"></a>
<table><thead align="left"><tr id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_row59078165"><th class="cellrowborder" valign="top" width="15.65%" id="mcps1.1.3.1.1"><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p20602051"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p20602051"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p20602051"></a><strong id="b167675376287"><a name="b167675376287"></a><a name="b167675376287"></a>Storage Model</strong></p>
</th>
<th class="cellrowborder" valign="top" width="84.35000000000001%" id="mcps1.1.3.1.2"><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p53618895"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p53618895"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p53618895"></a><strong id="b11727483289"><a name="b11727483289"></a><a name="b11727483289"></a>Applicable Scenario</strong></p>
</th>
</tr>
</thead>
<tbody><tr id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_row30816121"><td class="cellrowborder" valign="top" width="15.65%" headers="mcps1.1.3.1.1 "><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p13077833"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p13077833"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p13077833"></a>Row-store</p>
</td>
<td class="cellrowborder" valign="top" width="84.35000000000001%" headers="mcps1.1.3.1.2 "><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p52671525"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p52671525"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p52671525"></a>Point queries (simple index-based queries that only return a few records)</p>
<p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p4281684"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p4281684"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p4281684"></a>Scenarios requiring frequent addition, deletion, and modification operations</p>
</td>
</tr>
<tr id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_row38535158"><td class="cellrowborder" valign="top" width="15.65%" headers="mcps1.1.3.1.1 "><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p34340132"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p34340132"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p34340132"></a>Column-store</p>
</td>
<td class="cellrowborder" valign="top" width="84.35000000000001%" headers="mcps1.1.3.1.2 "><p id="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p30087318"><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p30087318"></a><a name="en-us_topic_0085132191_en-us_topic_0076211991_en-us_topic_0071158045_p30087318"></a>Statistics analysis query, in which operations, such as group and join, are performed many times.</p>
</td>
</tr>
</tbody>
</table>

## Using Partial Cluster Keys \(PCKs\)<a name="EN-US_TOPIC_0000001119812466"></a>

PCK is a column-store technology. It can minimize or maximize sparse indexes to quickly filter base tables. PCKs can be used to specify multiple columns. However, it is recommended that a maximum of two columns be specified. Use the following principles to specify PCKs:

1. The specified PCKs must be restricted by simple expressions in base tables. Such constraints are usually represented by  _col op const_, in which  _col_  indicates the column name,  _op_  indicates operators \(including =, \>, \>=, <=, and <\), and  _const_  indicates constants.
2. Select columns that are frequently selected \(to filter much more undesired data\) in simple expressions.
3. List the most frequently selected columns at the top.
4. List the columns of the enumerated type at the top.

## Using Partitioned Tables<a name="EN-US_TOPIC_0000001119972376"></a>

Partitioned tables split what is logically one table into smaller physical blocks based on a specific scheme. The table based on the logic is called a partitioned table, and each physical block is called a partition. A partitioned table is a logical table and does not store data. Data is actually stored in partitions. A partitioned table has the following advantages over an ordinary table:

1. High query performance: You can specify partitions when querying partitioned tables, improving query efficiency.
2. High availability: If a certain partition in a partitioned table is faulty, data in the other partitions is still available.
3. Easy maintenance: To fix a partitioned table having a faulty partition, you only need to fix the partition.

    Partitioned tables supported by the openGauss database are level-1 and level-2 partitioned tables. Level-1 partitioned tables include range partitioned tables, interval partitioned tables, list partitioned tables, and hash partitioned tables. Level-2 partitioned tables include nine combinations of any two of range partitioned tables, list partitioned tables, and hash partitioned tables.

    - Range partitioned table: Data within a certain range is mapped to each partition. The range is determined by the partition key specified when the partitioned table is created. This partitioning method is most commonly used. The partition key is usually a date. For example, sales data is partitioned by month.
    - Interval partitioned table: a special type of range partitioned tables. Compared with range partitioned tables, interval value definition is added. When no matching partition can be found for an inserted record, a partition can be automatically created based on the interval value.
    - List partitioned table: Key values contained in the data are stored in different partitions, and the data is mapped to each partition in sequence. The key values contained in the partitions are specified when the partitioned table is created.
    - Hash partitioned table: Data is mapped to each partition based on the internal hash algorithm. The number of partitions is specified when the partitioned table is created.
    - Level-2 partitioned table: a partitioned table obtained by randomly combining range partitioning, list partitioning, and hash partitioning. Both level-1 and level-2 partitions can be defined in the preceding three ways.

## Selecting a Data Type<a name="EN-US_TOPIC_0000001166772183"></a>

Efficient data types include the following:

1. **Select data types that facilitate data calculation.**

    Generally, the calculation of integers \(including common comparison calculations, such as =, \>, <, ≥, ≤, and ≠ and  **GROUP BY**\) is more efficient than that of strings and floating point numbers. For example, if you need to perform a point query on a column-store table whose numeric column is used as a filter criterion, the query will take over 10s. If you change the data type from  **NUMERIC**  to  **INT**, the query will be reduced to about 1.8s.

2. **Select data types with a short length.**

    Data types with a short length reduce both the data file size and the memory used for computing, improving the I/O and computing performance. For example, use  **SMALLINT**  instead of  **INT**, and  **INT**  instead of  **BIGINT**.

3. **Use the same data type for a join.**

    You are advised to use the same data type for a join. To join columns with different data types, the database needs to convert them to the same type, which leads to additional performance overheads.
