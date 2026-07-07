# SQL调优指南

SQL调优的唯一目的是“资源利用最大化”，即CPU、内存、磁盘IO三种资源利用最大化。所有调优手段都是围绕资源使用开展的。所谓资源利用最大化是指SQL语句尽量高效，节省资源开销，以最小的代价实现最大的效益。比如做典型点查询的时候，可以用seqscan+filter（即读取每一条元组和点查询条件进行匹配）实现，也可以通过indexscan实现，显然indexscan可以以更小的代价实现相同的效果。

根据硬件资源和客户的业务特征确定合理的openGauss部署方案和表定义是数据库在多数情况下满足性能要求的基础。下文的调优说明假设您已根据“软件安装”指引在安装过程中按照合理的openGauss方案完成了安装，且已经根据“开发设计建议”的指引进行了数据库设计。

- **[Query执行流程](query_execution_process.md)**  

- **[SQL执行计划介绍](sql_execution_plan_introduction.md)**  

- **[调优流程](tuning_process.md)**  

- **[更新统计信息](update_statistics.md)**  

- **[审视和修改表定义](reviewing_and_modifying_a_table_definition.md)**  

- **[典型SQL调优点](typical_sql_optimization_methods.md)**  

- **[经验总结：SQL语句改写规则](experience_in_rewriting_sql_statements.md)**  

- **[SQL调优关键参数调整](resetting_key_parameters_during_sql_tuning.md)**  

- **[使用Plan Hint进行调优](plan_hint_optimization_overview.md)**  
