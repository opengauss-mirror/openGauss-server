# 分支合并

## 一、功能概述

数据分支合并用于比较和合入两个数据分支之间的数据差异，包含 `branch diff`（比较差异）和 `branch merge`（合入变更）两类命令。

根据差异识别和变更应用方式的不同，分支合并分为两种模式：

- **全量分支合并**：默认模式。目标 endpoint 通过 `postgres_fdw` 临时映射源分支业务表，在目标 endpoint 内对共有表做全量行比较和合并。详见 [全量分支合并](./data_branch_full_merge.md)。
- **增量分支合并**：通过 `--incremental-oggit` 启用。基于 WAL 逻辑解码产生的增量元数据，只处理冻结的增量窗口，不执行全量业务表扫描。详见 [增量分支合并](./data_branch_delta_merge.md)。

两种模式共用 `neon_local branch diff/merge` 命令入口，未指定 `--incremental-oggit` 时执行默认的全量分支合并。

## 二、核心差异对比

| 对比项 | 全量分支合并 | 增量分支合并 |
| --- | --- | --- |
| 启用方式 | 默认模式，无需额外参数 | 显式指定 `--incremental-oggit` |
| 差异识别方式 | 通过 `postgres_fdw` 映射源表，全表扫描比较 | 基于两端 WAL 逻辑解码元数据，按增量窗口比较 |
| 前置依赖 | 两端有 running endpoint；目标端可 `CREATE EXTENSION neon` | 两端已启用 Oggit 且 worker 状态为 `active`，所需增量日志完整可用 |
| 处理范围 | 分支全量数据 | 自共同基线或上次成功合并以来的增量窗口 |
| 行身份要求 | 共有表必须存在主键 | 主键、可稳定定位行的 replica identity 唯一索引或 `REPLICA IDENTITY FULL`；无稳定行身份记为 `unsupported` |
| DELETE 传播 | 不删除目标侧独有行 | 传播 source 的显式删除事件 |
| 源独有表 | 可复制表结构和数据到目标，但仅支持部分常见表对象 | 按对象事件和安全分类处理 |
| DDL / TRUNCATE | 不处理已有对象的 DDL，仅复制源独有表的部分对象 | 按 `safe_additive`、`requires_validation`、`semantic`、`destructive`、`unsupported` 分类自动重放或转人工 |
| Sequence | 源独有表复制不支持 sequence 或自增默认值 | 应用时由目标端默认值生成新值，并修复目标端 sequence |
| diff 结果类型 | `source_only`、`target_only`、`conflict`、`schema_mismatch`、`no_primary_key` | `theirs_only`、`ours_only`、`same_change`、`mergeable`、`row_conflict`、`unsupported`，以及对象级 `theirs_object`、`ours_object`、`object_conflict` |
| 冲突处理策略 | `fail`、`ours`、`theirs` | `fail`、`ours`、`theirs`、`manual` |
| 人工干预流程 | 无阻塞恢复流程，按策略一次性处理 | `manual` 冲突或执行错误进入 `blocked`，支持 `conflicts`、`resolve`、`continue`、`abort` |
| 执行与并发控制 | 在目标 endpoint 内单次执行 | 目标端单事务应用；进入 `blocked` 时对 target 开启写屏障，同一 target 不允许新的 active merge |
| 主要开销 | 与数据量正相关，数据量越大耗时越长 | 与增量窗口内的变更量正相关，与表总数据量基本无关 |

## 三、建议使用场景

### 1. 建议使用全量分支合并

- 分支数据量较小或中等，全表比较的耗时和资源可以接受。
- 需要一次性、低频地比较或合入两个分支，不需要持续跟踪变更。
- 只需做差异查看，或只需把源分支新增的普通表复制到目标分支。
- 尚未启用 Oggit，或不希望引入 WAL 逻辑解码和增量元数据维护。
- 共有表有主键，且只需处理插入、更新类变更，不涉及显式删除传播。

### 2. 建议使用增量分支合并

- 表数据量大，全表扫描代价高，但每次实际变更的行数很少。
- 需要频繁、持续地在父子分支之间双向同步变更。
- 需要传播源分支的显式 DELETE 事件。
- 涉及 DDL、TRUNCATE、Sequence 等对象变更，希望按安全分类自动重放或转人工确认。
- 需要人工逐项决议冲突，并支持继续或终止合并的恢复流程。
- 已具备 Oggit 运行条件，能够接受两端启用 Oggit 并维护增量元数据。

### 3. 选择建议

- 优先按数据量和合并频率判断：数据量大且合并频繁时优先考虑增量模式，否则全量模式通常更简单。
- 若需要 DELETE 传播、对象变更自动处理或人工冲突决议，只能选择增量模式。
- 若增量日志不完整、worker 未处于 `active`，或两端不是直接父子 timeline，应回退到全量模式或先修复前置条件。
- 两种模式的 diff 和 merge 窗口相互独立，切换模式后应重新执行 diff 确认差异范围。

## 四、相关文档

- [全量分支合并](./data_branch_full_merge.md)：全量模式的命令、参数、输出、冲突策略、约束限制与完整示例。
- [增量分支合并](./data_branch_delta_merge.md)：增量模式的前置条件与启用方式、增量窗口机制、行身份与冲突、DDL 与 Sequence 处理、人工决议流程与限制说明。
