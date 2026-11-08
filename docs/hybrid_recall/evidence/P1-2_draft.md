# P1-2 草稿：保留 1:1 Task Stream（等 P1-1 提交后套用）

> 状态：草稿（2026-08-22）。全局审计已做（§5.4 的 rg 要求），三处编辑点。

## 审计结论（已核对源码）

| 位置 | 行为 | 处置 |
| --- | --- | --- |
| `createplan.cpp:831`（`#ifndef ENABLE_MULTIPLE_NODES` 内） | **创建侧删除**：`consumerDop==1 && producerDop==1` → `pfree(stream); return subplan;` | 加 `&& !stream->is_task_parallel` |
| `streamplan_utils.cpp:520`（调用侧） | `if (!stream->is_recursive_local)` 才调 `remove_local_plan`（recursive_local 是**保留先例**） | 条件加 `&& !stream->is_task_parallel`（照抄先例） |
| `streamplan_utils.cpp:1467`（`remove_local_plan` 内部） | **清理侧删除**：`STREAM_IS_LOCAL_NODE && consumerDop==1 && producerDop==1` | 再加 `&& !stream->is_task_parallel`（纵深防御） |
| `execStream.cpp` 的 LOCAL GATHER/ROUNDROBIN 断言 | 执行侧标签/断言，非删除 | 无需改（P1-4 复核） |

## 编辑内容

### 1. createplan.cpp:831

old:
```cpp
    /* if number of producer and consumer is 1, means stream node is not needed */
    if (stream->smpDesc.consumerDop == 1 && stream->smpDesc.producerDop == 1) {
        pfree(stream);
        return subplan;
    }
```
new:
```cpp
    /* if number of producer and consumer is 1, means stream node is not needed.
     * [P1-2] Task-parallel 1:1 streams are the concurrency primitive of
     * HybridRecall's two recall branches (design doc §5.4) - never fold. */
    if (stream->smpDesc.consumerDop == 1 && stream->smpDesc.producerDop == 1 &&
        !stream->is_task_parallel) {
        pfree(stream);
        return subplan;
    }
```

### 2. streamplan_utils.cpp:520

old:
```cpp
                if (!stream->is_recursive_local) {
                    removed = remove_local_plan(result_plan, parent, cell, is_left);
```
new:
```cpp
                /* [P1-2] task-parallel 1:1 streams stay, same as recursive_local */
                if (!stream->is_recursive_local && !stream->is_task_parallel) {
                    removed = remove_local_plan(result_plan, parent, cell, is_left);
```

### 3. streamplan_utils.cpp:1467

old:
```cpp
    if (STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType) && stream->smpDesc.consumerDop == 1 &&
        stream->smpDesc.producerDop == 1) {
```
new:
```cpp
    if (STREAM_IS_LOCAL_NODE(stream->smpDesc.distriType) && stream->smpDesc.consumerDop == 1 &&
        stream->smpDesc.producerDop == 1 && !stream->is_task_parallel) {
```

## 验证计划

1. `.cpp-only` → executor/optimizer 目录增量编译 + 脚本 incremental（用户偏好）；
2. 9 项回归 + golden（无行为变化：现有计划 is_task_parallel 恒 false）；
3. P1-3 起才有真实 task stream 可供 EXPLAIN 断言"两 Stream 存在"。
