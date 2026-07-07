# 更新统计信息<a name="ZH-CN_TOPIC_0289900381"></a>

在数据库中，统计信息是规划器生成计划的源数据。没有收集统计信息或者统计信息陈旧往往会造成执行计划严重劣化，从而导致性能问题。

## 背景信息<a name="zh-cn_topic_0283136976_zh-cn_topic_0237121513_zh-cn_topic_0073253809_zh-cn_topic_0040046492_section1822974510313"></a>

ANALYZE语句可收集与数据库中表内容相关的统计信息，统计结果存储在系统表PG\_STATISTIC中。查询优化器会使用这些统计数据，以生成最有效的执行计划。

建议在执行了大批量插入/删除操作后，例行对表或全库执行ANALYZE语句更新统计信息。目前默认收集统计信息的采样比例是30000行（即：guc参数default\_statistics\_target默认设置为100），如果表的总行数超过一定行数（大于1600000），建议设置guc参数default\_statistics\_target为-2，即按2%收集样本估算统计信息。

对于在批处理脚本或者存储过程中生成的中间表，也需要在完成数据生成之后显式的调用ANALYZE。

对于表中多个列有相关性且查询中有同时基于这些列的条件或分组操作的情况，可尝试收集多列统计信息，以便查询优化器可以更准确地估算行数，并生成更有效的执行计划。

## 操作步骤<a name="zh-cn_topic_0283136976_zh-cn_topic_0237121513_zh-cn_topic_0073253809_zh-cn_topic_0040046492_section6024390710327"></a>

使用以下命令更新某个表或者整个database的统计信息。

```
ANALYZE tablename;                        --更新单个表的统计信息
ANALYZE;                                  --更新全库的统计信息
```

使用以下命令进行多列统计信息相关操作。

```
ANALYZE tablename ((column_1, column_2));                       --收集tablename表的column_1、column_2列的多列统计信息

ALTER TABLE tablename ADD STATISTICS ((column_1, column_2));    --添加tablename表的column_1、column_2列的多列统计信息声明
ANALYZE tablename;                                              --收集单列统计信息，并收集已声明的多列统计信息

ALTER TABLE tablename DELETE STATISTICS ((column_1, column_2)); --删除tablename表的column_1、column_2列的多列统计信息或其声明
```

>[!TIP]须知
>
>在使用ALTER TABLE tablename ADD STATISTICS语句添加了多列统计信息声明后，系统并不会立刻收集多列统计信息，而是在下次对该表或全库进行ANALYZE时，进行多列统计信息的收集。
>如果想直接收集多列统计信息，请使用ANALYZE命令进行收集。

>[!NOTE]说明
>
>使用EXPLAIN查看各SQL的执行计划时，如果发现某个表SEQ SCAN的输出中rows=10，rows=10是系统给的默认值，有可能该表没有进行ANALYZE，需要对该表执行ANALYZE。
