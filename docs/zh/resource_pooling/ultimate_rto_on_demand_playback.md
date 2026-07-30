# 极致RTO按需回放

## 可获得性

本特性自openGauss 5.1.0 版本开始引入。

## 特性简介

- 支撑资源池化部署下数据库主机重启后快速恢复的场景。
- 支撑资源池化部署下备机加速failover，降低RTO。
- 本特性基于现有极致RTO特性演进而来。

## 客户价值

资源池化部署下，备机不再回放主机日志。当主机发生故障后，备机从主机最后一次checkpoint位置开始恢复，恢复过程需要很长时间，数据库不可用，严重影响系统可用性。

开启按需回放极致RTO（Recovery Time Object，恢复时间目标），减少了主机故障后数据库不可用的时间，提高了可用性。

## 特性描述

按需回放极致RTO开关开启后，故障恢复时仅构建恢复所必须的内容，不进行实际回放，之后立即对外提供服务，降低RTO。在对外提供服务后，在后台继续进行日志回放，并由用户需要触发按需回放，保证用户获得数据最新（即结果与未发生故障时前一致）。

按需回放在control文件的Cluster Status项新增两种状态："in on-demand build"、"in on-demand redo"（详见[pg_controldata](https://docs.opengauss.org/zh/docs/latest/tool_and_commandreference/pg_controldata.html)）。开启按需回放功能，发生failover时，新主节点先将控制文件中cluster Status置为"in on-demand build"，进入按需回放的构建阶段，此时集群在构建回放必须信息，不能对外提供服务。构建完成后，会将控制文件中cluster Status置为"in on-demand redo"，进入按需回放的回放阶段，此时可以对外提供服务，同时进行日志回放，直至回放完毕。如果主节点在按需回放两个阶段未能全部完成的情况下退出，作为逃生手段，新主节点会以极致RTO模式进行故障恢复。

## 特性增强

1. 按需回放支持实时构建
   - 本特性自openGauss 6.0.0-RC1版本开始引入。
   - 按需回放支持实时构建，能够在资源池化集群正常运行阶段就构建恢复所必须的内容，进一步降低RTO。
2. 按需回放redo阶段适配部分ddl
   - 本特性自openGauss 6.0.0版本开始引入。
   - 支持在按需回放redo阶段（即failover完成，新主节点已经对外提供服务，但后台还在继续进行日志回放）执行部分DDL语法。
   - 具体而言，本特性支持执行SCHEMA、TABLE、INDEX、VIEW、PROCEDURE的CREATE、DROP、ALTER类型语法，以及DATABASE的ALTER类型语法，其他语法会合理报错。此外本特性支持在按需回放redo阶段执行存储过程，存储过程中包含的语法必须在redo阶段支持语法范围内。
   - 由于本特性需要在执行DDL前检索相关数据库实体的所有未回放日志，并完成回放，可能存在DDL执行时间较长的情况。
3. 实时构建支持流控
   - 本特性自openGauss 7.0.0-RC1版本开始引入，即在资源池化实时构建场景适配recovery_time_target参数。该功能只在资源池化的主节点有效，开启该功能后，备机会定时向主机发送备机实时构建的最新wal日志的lsn，主机根据连续几次接收到的lsn估算RTO，当识别到当前实时构建的构建速度难以满足目标RTO时，会对主机业务做适当限制，以缓解备机实时构建压力，缩短RTO。
   - 资源池化场景recovery_time_target参数只在主机生效，使用本特性需要主机配置recovery_time_target > 0，备机配置ss_ondemand_realtime_build = on，主机就会开始基于备机的xlog构建速度对主机业务进行流控。
   - 本特性在reform期间不生效，即该功能在reform期间会动态关闭，reform结束后会重新生效。
   - 为避免极端场景阻塞业务，资源池化场景recovery_time_target为RTO参考值，主机会基于RTO参考值在识别到备机实时构建速度较慢时适当减缓业务，不会完全阻塞业务，即不能完全保证实际RTO在recovery_time_target配置值以内。

## 特性约束

- 本特性仅支持在资源池化部署下使用。
- 在对外提供服务后，数据库仍在后台进行回放，直到全部内容完成后退出回放，该阶段称为“按需回放阶段”。按需回放阶段仅支持部分类型SQL语法（INSERT/UPDATE/DELETE/SELECT/SET/SHOW）及部分DDL（详见[特性增强](#特性增强)），该状态请使用pg_controldata工具查询。
- 按需回放阶段禁用autovacuum。
- 实时构建能力仅在备机failover场景生效。
- 极致RTO按需回放性能会受磁阵环境影响而波动。
- 本功能会消耗较多的内存空间，仅建议在内存充足且对RTO时间敏感的系统上使用，其余系统建议使用极致RTO回放（详见[极致RTO](https://docs.opengauss.org/zh/docs/latest/database_om_guide/ultimate_rto.html)）。
- 资源池化下回放模式选择，请参考[资源池化高可用系统配置](resource_pooling_ha_system_configuration.md)

## 依赖关系

无。

## 基本原理

1. 极致RTO按需回放
   - 该特性的核心思想是将回放分为build阶段, 和redo阶段。build阶段发生在failover的reform期间, 升主节点只会完成段页式日志和事务日志的回放, 并将其他类型的xlog解析成页面粒度的日志段存入哈希表中。redo阶段发生在reform完成后, PageRedoManager线程会把日志段分发给RedoPageWorker线程进行回放。此时, 如果有业务需要请求某个页面, 会通过从哈希表中检索该页面所有待回放的xlog, 并提前回放。
2. 按需回放支持实时构建
   - 本特性是按需回放的增强特性。为了进一步缩短RTO, 实时构建特性将原本在reform期间进行的读取xlog、解析xlog、构建哈希表等操作改为在集群状态为normal时进行, 等到failover发生时, 回放线程会基于现有的哈希表构建进度继续执行build阶段, 后续流程与按需回放一致。
3. 按需回放redo阶段适配部分ddl
   - 本特性是按需回放的增强特性。该特性主要实现内容是在redo阶段执行DDL时, 从哈希表中筛选出DDL涉及到的表的所有待回放xlog。以对表的DLL操作为例, 如"ALTER TABLE XXX"、"DROP TABLE XXX"时, 在执行DDL操作前, 基于表名和表空间, 查找表的relnode, 从哈希表中检索relnode相同的待回放的xlog并提前完成回放。此外, 需要检查该表是否有其他关联的relation或partition, 包括索引（global索引、local索引）、分区表（一级分区、二级分区）、toast表（每个partition都可能有toast）。如果有, 需要分别获取其relnode, 并哈希表中检索出所有待回放xlog并提前回放。之后执行具体DDL操作时, 就可以视为在非redo阶段执行DDL。
4. 实时构建支持流控
   - 本特性是实时构建的增强特性, 主要用于在备机实时构建速度跟不上主机xlog生成速度时, 适当减缓主机业务, 缓解备机压力。具体而言, 本特性采用DMS消息通信的方式, 主节点配置recovery_time_target > 0时, 会向备机广播消息。备机接收到广播消息后, 会定时通过DMS向主机发送xlog构建进度, 由主机分别计算各节点的预估RTO, 如果预估RTO > recovery_time_target /2, 则会基于高斯分布计算睡眠时间globalSleepTime, 业务提交前每次休眠特定时间, 以减缓业务流量。

## 使用指导

极致RTO按需回放及其增强特性只在资源池化场景生效：

- 极致RTO按需回放，开启参数：ss_enable_ondemand_recovery = on, ss_enable_ondemand_realtime_build = off, recovery_parse_workers > 1, recovery_redo_workers >= 1, 主机进行故障恢复时如果开启以上参数则自动触发。ss_ondemand_recovery_mem_size建议配置为故障恢复时待回放日志量的2倍。
- 极致RTO按需回放实时构建, 开启参数：ss_enable_ondemand_recovery = on, ss_enable_ondemand_realtime_build = on, recovery_parse_workers > 1, recovery_redo_workers >= 1, 集群状态为normal时, 备机如果开启以上参数, 则自动启动按需回放实时构建特性。ss_ondemand_recovery_mem_size建议配置为故障恢复时待回放日志量的2倍。
- 按需回放redo阶段适配部分DDL, 没有参数控制, 按需回放redo阶段自动生效, 已适配语法执行方法与非redo阶段相同, 但是执行时间会增加, 不支持语法会合理报错。
- 实时构建支持流控特性, 需要主机配置recovery_time_target > 0, 备机开启按需回放实时构建特性。开启后主机会根据备机实时构建的进度预估RTO, 进而在预期RTO可能无法达到时, 适当减缓业务执行效率。

## 使用场景

资源池化按需回放特性及其增强特性主要适用于, 资源池化集群中对RTO时间敏感度高, 且系统内存充足的场景, 具体请参考[资源池化高可用系统配置](resource_pooling_ha_system_configuration.md#回放方式选择)
