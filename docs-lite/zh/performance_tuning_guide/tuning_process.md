# 调优流程<a name="ZH-CN_TOPIC_0289899868"></a>

对慢SQL语句进行分析，通常包括以下步骤：

## 操作步骤<a name="zh-cn_topic_0283137712_zh-cn_topic_0237121512_zh-cn_topic_0073253796_zh-cn_topic_0062520023_section43790015111840"></a>

1. 收集SQL中涉及到的所有表的统计信息。在数据库中，统计信息是规划器生成计划的源数据。没有收集统计信息或者统计信息陈旧往往会造成执行计划严重劣化，从而导致性能问题。从经验数据来看，10%左右性能问题是因为没有收集统计信息。具体请参见[更新统计信息](update_statistics.md)。
2. 通过查看执行计划来查找原因。如果SQL长时间运行未结束，通过EXPLAIN命令查看执行计划，进行初步定位。如果SQL可以运行出来，则推荐使用EXPLAIN ANALYZE或EXPLAIN PERFORMANCE查看执行计划及实际运行情况，以便更精准地定位问题原因。有关执行计划的详细介绍请参见[SQL执行计划介绍](sql_execution_plan_introduction.md)。
3. [审视和修改表定义](reviewing_and_modifying_a_table_definition.md)。
4. 针对EXPLAIN或EXPLAIN PERFORMANCE信息，定位SQL慢的具体原因以及改进措施，具体参见[典型SQL调优点](typical_sql_optimization_methods.md)。
5. 通常情况下，有些SQL语句可以通过查询重写转换成等价的，或特定场景下等价的语句。重写后的语句比原语句更简单，且可以简化某些执行步骤达到提升性能的目的。查询重写方法在各个数据库中基本是通用的。[经验总结：SQL语句改写规则](experience_in_rewriting_sql_statements.md)介绍了几种常用的通过改写SQL进行调优的方法。
