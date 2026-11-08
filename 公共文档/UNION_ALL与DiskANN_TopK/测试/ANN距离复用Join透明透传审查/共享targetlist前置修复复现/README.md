# UNION ALL 源头列裁剪：共享 targetlist 崩溃复现

## 这是什么问题

这是 `union_all_replace_source_chain()` 的旧问题，**不是本次 ANN 距离复用 Join 透明透传的 P1 问题**。

必要条件是源头投影改写命中一条共享 `TargetEntry` 的 source chain；本用例通过无主键的 `Sort -> Seq Scan` 稳定制造这种形态，**无主键本身不是必要条件**。`make_sort()` 会让 Sort 与下层子计划共享同一份 targetlist；旧的源头投影改写逻辑先修改一份共享的 `TargetEntry`，随后再次按原始 `Var` 查找时找不到该 entry，继续解引用空指针。2026-09-21 已临时还原旧函数并实际捕获这处 SIGSEGV，见下文实测证据。

修复后的逻辑位于 `planner.cpp` 的 `union_all_replace_source_chain()`：

1. 遍历整条 source chain，先完成所有匹配和安全校验。
2. 按 `TargetEntry*` 指针去重，避免 Sort/Scan 共享 entry 被重复处理。
3. 所有校验通过后，再统一把每个唯一 entry 的表达式替换为候选投影。

因此，必须是“先收集并验证，后统一修改”，不能在第一遍校验时边遍历边改写。

## 如何运行

脚本不会删除现有对象，所有表、视图和数据都在事务内创建，正常完成时最后 `ROLLBACK`。但事务不能保护实例免受内核崩溃影响，**旧版本只能在隔离实例运行**。以下命令假定隔离实例已在 55450 启动，不连接现有 55440 主实例：

```bash
cd /home/qiumc/data/code/cpp/openGauss-server
export GAUSSHOME=$PWD/mppdb_temp_install
export PATH=$GAUSSHOME/bin:$PATH
export GCCLIB=/home/qiumc/data/code/cpp/binarylibs/buildtools/gcc10.3/gcc/lib64
export LD_LIBRARY_PATH=$GAUSSHOME/lib:$GCCLIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

gsql -X -h /tmp -p 55450 -U qiumc -d postgres -a -f \
  /home/qiumc/data/code/cpp/公共文档/UNION_ALL与DiskANN_TopK/测试/ANN距离复用Join透明透传审查/共享targetlist前置修复复现/01_无主键完整复现.sql
```

脚本刻意不创建主键和索引，并关闭 Hash Join、打开 Merge Join，以稳定得到：

```text
Merge Anti Join
  -> Sort
       -> Seq Scan
```

向量索引不是此问题的前提；`ann_orderby_distance_reuse` 只用于复现与正式矩阵相同的 SQL 开关组合。

## 预期现象

脚本中的 B 阶段打开 `enable_union_all_required_columns_pushdown`：

- 在未包含共享 targetlist 修复的旧内核上，`EXPLAIN` 或随后执行可能使 backend core。必须只在隔离的开发实例中验证旧版本，不能在生产或共享实例执行。
- 在当前已修复内核上，计划正常返回，结果为：

```text
 row_id
--------
    101
    102
      3
      3
(4 rows)
```

实测输出保存在 `02_当前已修复实例实测.out`。因为旧版本的预期是进程异常退出，不应把旧版本输出伪造为固定 golden；用例的验收重点是旧版本的崩溃触发点，以及修复后无崩溃、结果正确、事务能回滚。

## 2026-09-21 还原旧函数后的实际验证

本次不是仅按代码推断：在源仓当前工作树中，仅临时撤回 `union_all_replace_source_chain()` 的共享 entry 修复，增量编译并安装到独立目录，使用隔离实例 55450 执行本目录的 `01_无主键完整复现.sql`。没有修改或重启原有 55440 实例。

| 版本与阶段 | 实测结果 |
|---|---|
| 旧函数，A：列裁剪 off、距离复用 off | EXPLAIN 正常，结果为 `101,102,3,3` |
| 旧函数，B：列裁剪 on、距离复用仍 off | EXPLAIN 触发 SIGSEGV，gsql 断连并返回退出码 2 |
| 恢复修复函数并重新编译，原样执行 A/B/C | 三阶段计划与查询正常，结果均为 `101,102,3,3`，结果断言通过并完成 ROLLBACK |

旧函数断点捕获的关键值：

```text
第一次：T_Sort    targetlist=0x71b350da32c8 source=0x71b350da3330
第二次：T_SeqScan targetlist=0x71b350da32c8 source=NULL
SIGSEGV: source->expr = (Expr*)copyObject(candidate->expr);
```

两个节点的 targetlist 地址完全相同。第一次修改把共享 entry 的表达式由 `T_Var` 变成 `T_OpExpr`，第二次重新找原 Var 就返回 NULL。崩溃时 `candidate->expr` 仍为 `T_OpExpr`，本次直接故障点是空 `source` 的解引用。

恢复后再次用调试器验证：`chain->length = 2`、`sources->length = 1`；开始统一修改前，共享 entry 仍为 `T_Var`，即先校验和去重、再修改的修复确实执行。

完整证据：

- [旧函数 SQL 输出及断连位置](03_还原旧函数_SIGSEGV实测.out)
- [旧函数 GDB 指针、SIGSEGV 和调用栈](04_还原旧函数_GDB指针与调用栈.txt)
- [恢复修复版后，同一 SQL 的完整计划与结果](05_恢复修复版_同一SQL实测.out)
- [修复版 GDB 去重证据](06_恢复修复版_GDB去重证据.txt)

旧栈中的 `planner.cpp:762` 是临时还原版的行号；源码恢复后行号已改变，不应直接按当前第 762 行判断故障语句。本次保留的是 GDB 捕获记录，不是完整 core 文件。复现的是源仓与所贴迁移代码一致的崩溃机制，并未运行目标 GaussVector 二进制或目标库的原 SQL。

收尾已恢复测试前的源码，重新编译验证修复版，并停止隔离实例。临时产物与构建日志位于 `/home/qiumc/data/code/cpp/.runtime/union_all_shared_tlist_repro.02yvRz/`。

## 与 Join 透明透传迁移的关系

本问题是目标仓迁移 ANN Join 透传时的**前置能力**，但不属于 Join P1 补丁本身。迁移者需要确认目标仓已经包含该修复或语义等价实现，再运行无主键 UNION ALL 矩阵；否则可能把列裁剪的旧 core 误判为 Join 透传问题。

本目录的复现 SQL 不会被主 Join patch 自动应用到目标仓；它是迁移前的诊断用例。目标仓应根据自身 openGauss 回归框架决定是否纳入回归，并为旧版本单独准备隔离实例。
