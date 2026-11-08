# P1-3 侦察笔记：双支路 Task Stream 构造（execStream.cpp 链路事实）

> 2026-08-22 只读侦察结论（P1-1 验证链运行期间）。落码在 P1-2 提交后。

## 已核实的事实

### 1. 计划遍历：无需改 execStream.cpp
`InitStreamFlow`（execStream.cpp:875）按 nodeTag 分派；**default 分支走 lefttree/righttree**
（:1003-1014）→ HybridRecall 的两支路自动被遍历到。显式 case 只为非标准子节点
（Append.appendplans、SubqueryScan.subplan、ModifyTable.plans、BitmapAnd/Or、
RecursiveUnion、FunctionScan 游标等）。设计 §7.8 的担忧不成立，前提是支路放在
标准 lefttree/righttree（P0-1 的选择被再次验证）。

### 2. 单机模式硬约束
InitStream（:605）开头：`#ifndef ENABLE_MULTIPLE_NODES` 下**只允许 local stream**
（否则 ERROR "Single Node should only has local stream operator"）→ task stream 必须
`distriType ∈ {LOCAL_ROUNDROBIN, LOCAL_BROADCAST}`（设计 §5.3 选 ROUNDROBIN ✅）。

### 3. StreamKey 与 plan_node_id
`key = (pstmt->queryId, plan->plan_node_id, cursor_expr_level, cursor_owner_node_id)`
→ 两个 task Stream 的 plan_node_id 必须全树唯一且非零（§7.8 预警落点）。
载具手工赋 id 时须给整棵树统一编号。

### 4. 启动链路（载具/内核要喂的参数）
- `BuildStreamFlow(PlannedStmt*)`：由 `plan->num_streams > 0` 触发 → 建
  StreamNodeGroup + InitStreamFlow 全树扫描 → InitStream 为每个 Stream 建
  consumer/producer 对 + `buildLocalStreamContext`（本地内存通道）。
- Stream 节点需要：`type=STREAM_REDISTRIBUTE`（本地场景载体）、
  `smpDesc={LOCAL_ROUNDROBIN, consumerDop=1, producerDop=1}`、
  `is_task_parallel=true`、唯一 `plan_node_id`、`consumer_nodes`（ExecNodes，
  单机本节点）、`scan.scanrelid`、producer 侧计划 = Stream 的 lefttree。
- `StartUpStreamInParallel(PlannedStmt*, EState*)`（:1356）：ExecutorStart 阶段拉起
  producer 线程（线程池 ApplyStreamThread）——发生在整个计划 Init 完成后
  （设计 §8.1 的时序约束来源）。

### 5. P1-3 载具改造方向
debug 函数在 HybridRecall 与两个支路 Limit 之间各插一个手工 Stream 节点：
```
hybrid->lefttree  = Stream(task_branch=TEXT)   →  Limit → BM25 IndexScan
hybrid->righttree = Stream(task_branch=VECTOR) →  Limit → AnnIndexScan
```
stmt->num_streams = 2；全树 plan_node_id 重编号（Stream 的 consumer 侧 slot/tlist
用 passthrough 形态——Stream 是 Scan 子类，其 tlist 匹配规则同 P0-4 教训）。
consumer 侧 nodeStream 的 ExecStream 会经本地通道收 producer 元组——HybridRecall
的 drain 代码无需改动（它只见 ExecProcNode 流）——**P1 的并行化对融合层透明**，
这是设计 §5.5 的架构保证。

## 已核实的关键细节（补充）

### 6. Producer 计划来源：lefttree 拷贝，无需 plan_statement
`StreamProducer::init`（streamProducer.cpp:209）：
```cpp
m_plan->planTree = (Plan*)copyObject(m_streamNode->scan.plan.lefttree);
m_plan->num_streams = 0;
```
本地流 producer 直接内存拷贝 Stream 的 **lefttree** 作为执行计划（plan_statement
仅分布式序列化需要）；producer 侧 num_streams=0（V1 支路无嵌套流，天然满足）。
→ 载具只须把支路 Limit 计划挂 Stream->scan.plan.lefttree；copyObject 质量由
P0-2 闭环保证（支路是原生节点）。

## 仍待落码时核对
- ExecStream（nodeStream.cpp）consumer 初始化对 tlist/scanrelid 的要求；
- 线程池配额（u_sess->stream_cxt）在单机 local stream 下的默认值。
