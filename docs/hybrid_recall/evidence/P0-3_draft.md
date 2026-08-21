# P0-3 集成草稿：Exec 分发接线（等 P0-2 提交后套用）

> 状态：草稿（2026-08-21）。新文件已就位（不影响在跑构建）：
> - `src/include/executor/node/nodeHybridRecall.h`
> - `src/gausskernel/runtime/executor/nodeHybridRecall.cpp`
>
> 以下 4 处是**跟踪文件编辑**，按 P0-1→P0-2→P0-3 顺序落码。

## 事实基础（已核对）

- 初始化分发：`execProcnode.cpp` `ExecInitNodeByType` switch，`case T_Limit:` :400-401
- 结束分发：`execProcnode.cpp` :1324-1326 `case T_LimitState:` → ExecEndLimit（**State** tag）
- ReScan 分发：`execAmi.cpp` :304-305 `case T_LimitState:` → ExecReScanLimit（**State** tag）
- ExecProcNode 接线：各 ExecInitXxx 内 `state->ps.ExecProcNode = ExecXxx;`
  （ExecInitNode 统一包 ExecProcNodeFirst→Real + instrument 包装，:612-613/:803-805）；
  新 .cpp 已含该行
- Makefile：`src/gausskernel/runtime/executor/Makefile` :42-43 OBJS 行内追加

## 1. execProcnode.cpp — 初始化分发

old:
```cpp
        case T_Limit:
            return (PlanState*)ExecInitLimit((Limit*)node, estate, eflags);
```
new:
```cpp
        case T_Limit:
            return (PlanState*)ExecInitLimit((Limit*)node, estate, eflags);
        case T_HybridRecall:
            return (PlanState*)ExecInitHybridRecall((HybridRecall*)node, estate, eflags);
```

## 2. execProcnode.cpp — 结束分发

old:
```cpp
        case T_LimitState:
            ExecEndLimit((LimitState*)node);
            break;
```
new:
```cpp
        case T_LimitState:
            ExecEndLimit((LimitState*)node);
            break;

        case T_HybridRecallState:
            ExecEndHybridRecall((HybridRecallState*)node);
            break;
```

（套用时核对两处 case 唯一性：初始化 switch 用 Plan tag，结束 switch 用 State tag。）

## 3. execAmi.cpp — ReScan 分发

old:
```cpp
        case T_LimitState:
            ExecReScanLimit((LimitState*)node);
            break;
```
new:
```cpp
        case T_LimitState:
            ExecReScanLimit((LimitState*)node);
            break;

        case T_HybridRecallState:
            ExecReScanHybridRecall((HybridRecallState*)node);
            break;
```

另需在两文件 include 区加 `#include "executor/node/nodeHybridRecall.h"`：
- execProcnode.cpp：紧跟 :94 `#include "executor/node/nodeLimit.h"` 之后
- execAmi.cpp：紧跟 :37 `#include "executor/node/nodeLimit.h"` 之后

## 4. executor/Makefile — OBJS

old:
```make
       nodeLimit.o nodeLockRows.o nodeMemoize.o \
```
new:
```make
       nodeHybridRecall.o nodeLimit.o nodeLockRows.o nodeMemoize.o \
```

## 5. 验证计划（套用后）

1. `make -C src/gausskernel/runtime/executor -sj4`（注意：新文件**不在**旧 .d 里——本就无
   依赖跟踪，Makefile 变更会触发该目录全量编 .o；增量模式对该目录有效）；
2. 脚本 incremental 安装重启；
3. 9 项定向回归 + golden 双跑比对（P0-3 空生命周期无行为面，仍需证明分发接线不破坏
   现有计划执行）；
4. P0-3 判据"EXPLAIN 空计划不崩溃"：无构造入口（同 P0-2 round-trip 偏差），由 P0-4
   手工计划测试回补，此处记录偏差。

## 6. 与设计文档的对应

- §8.1 RejectUnsupportedExecutorFlags → Assert(BACKWARD|MARK)（Assert 级，发布版静默）；
  P0-4 补 ValidateRuntimeOptions 的 ereport 版本；
- §8.7 End 序列：分支 End + candidate_cxt 释放，Stream 共享上下文不越权释放 ✅；
- §8.8 ReScan：V1 仅 phase 复位，参数化 rescan 由 P2 规划期拒绝。
