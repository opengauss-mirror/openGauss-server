# P0-2 草稿：copy/out/read 闭环（等 P0-1 提交后逐块套用）

> 状态：草稿（2026-08-21）。P0-1 full 构建验证期间编写，**尚未落码**。
> 所有 old/new 块按 Edit 工具格式给出；套用后增量编译（仅 .cpp 改动，incremental 有效）。

## 事实基础（已核对）

- copyfuncs.cpp：`_copyLimit` 结束于 :1793；分发 `case T_Limit:` :8493（switch 风格）
- outfuncs.cpp：`_outLimit` :2375（结束边界套用时读）；分发 `case T_Limit:` :7073（switch）
- readfuncs.cpp：`_readLimit` :3808-3830；分发是**字符串匹配**非 tag switch：
  `} else if (MATCH("LIMIT", 5)) {` :7294-7296
- 宏：`WRITE_ENUM_FIELD(fld, type)` ✅；`WRITE_DOUBLE_FIELD` ❌ 不存在 →
  `WRITE_FLOAT_FIELD(fld, "%.17g")`（format 直通 appendStringInfo，17 位有效数字保证往返精确）
- 版本门策略：HybridRecall 是全新节点类型，旧版本二进制不可能产出含它的计划 →
  copy 无 workingVersionNum 门、read 无 IF_EXIST 门（与 diskann 给**已存在**节点加字段的场景不同）。
  混合版本集群防扩散是 P3-6 事项（设计文档 §19.2）。
- equalfuncs.cpp：无任何 Plan 节点 case（连 AnnIndexScan 都没有）→ 本任务结论"无需改动"，
  依据 = `equal()` 不覆盖 Plan 节点。

## 1. copyfuncs.cpp

### 1a. 函数体（插入点：_copyLimit 结尾 `}` 之后、`_copyNestLoopParam` 注释之前）

```cpp
/*
 * _copyHybridRecall
 *
 * [P0-2] HybridRecall fusion parent (hybrid recall design doc §6.2).
 * Branch plans travel in plan.lefttree/righttree and are deep-copied by
 * CopyPlanFields. No workingVersionNum gate: the node type cannot occur
 * in plans produced by older binaries, and V1 is single-node so the plan
 * is never shipped cross-version; mixed-version gating is tracked as a
 * P3-6 requirement (design doc §19.2).
 */
static HybridRecall* _copyHybridRecall(const HybridRecall* from)
{
    HybridRecall* newnode = makeNode(HybridRecall);

    /*
     * copy node superclass fields
     */
    CopyPlanFields((const Plan*)from, (Plan*)newnode);

    /*
     * copy remainder of node
     */
    COPY_SCALAR_FIELD(id_resno);
    COPY_SCALAR_FIELD(text_score_resno);
    COPY_SCALAR_FIELD(vector_distance_resno);
    COPY_NODE_FIELD(final_topk);
    COPY_NODE_FIELD(text_candidates);
    COPY_NODE_FIELD(vector_candidates);
    COPY_SCALAR_FIELD(fusion);
    COPY_SCALAR_FIELD(rrf_k);
    COPY_SCALAR_FIELD(text_weight);
    COPY_SCALAR_FIELD(vector_weight);
    COPY_SCALAR_FIELD(parallel_branches);
    COPY_SCALAR_FIELD(require_parallel);
    COPY_NODE_FIELD(recheck_quals);

    return newnode;
}
```

### 1b. 分发

old:
```cpp
        case T_Limit:
            retval = _copyLimit((Limit*)from);
            break;
```
new:
```cpp
        case T_Limit:
            retval = _copyLimit((Limit*)from);
            break;
        case T_HybridRecall:
            retval = _copyHybridRecall((HybridRecall*)from);
            break;
```

## 2. outfuncs.cpp

### 2a. 函数体（插入点：_outLimit 结尾 `}` :2409 之后、`static void _outNestLoopParam` :2411 之前）

```cpp
/*
 * [P0-2] HybridRecall fusion parent (hybrid recall design doc §6.2).
 * Doubles use %.17g so nodeToString/stringToNode round-trips exactly.
 */
static void _outHybridRecall(StringInfo str, HybridRecall* node)
{
    WRITE_NODE_TYPE("HYBRIDRECALL");

    _outPlanInfo(str, (Plan*)node);

    WRITE_INT_FIELD(id_resno);
    WRITE_INT_FIELD(text_score_resno);
    WRITE_INT_FIELD(vector_distance_resno);
    WRITE_NODE_FIELD(final_topk);
    WRITE_NODE_FIELD(text_candidates);
    WRITE_NODE_FIELD(vector_candidates);
    WRITE_ENUM_FIELD(fusion, HybridFusionKind);
    WRITE_FLOAT_FIELD(rrf_k, "%.17g");
    WRITE_FLOAT_FIELD(text_weight, "%.17g");
    WRITE_FLOAT_FIELD(vector_weight, "%.17g");
    WRITE_BOOL_FIELD(parallel_branches);
    WRITE_BOOL_FIELD(require_parallel);
    WRITE_NODE_FIELD(recheck_quals);
}
```

### 2b. 分发

old:
```cpp
            case T_Limit:
                _outLimit(str, (Limit*)obj);
                break;
```
new:
```cpp
            case T_Limit:
                _outLimit(str, (Limit*)obj);
                break;
            case T_HybridRecall:
                _outHybridRecall(str, (HybridRecall*)obj);
                break;
```

## 3. readfuncs.cpp

### 3a. 函数体（插入点：_readLimit 的 `}` :3830 之后、`static Agg* _readAgg` 之前）

```cpp
/*
 * [P0-2] HybridRecall read-back. All fields are co-born with the node
 * type, so plain READs suffice (no IF_EXIST gating: no older plan can
 * contain a HYBRIDRECALL section).
 */
static HybridRecall* _readHybridRecall(HybridRecall* local_node)
{
    READ_LOCALS_NULL(HybridRecall);
    READ_TEMP_LOCALS();

    // Read Plan
    _readPlan(&local_node->plan);

    READ_INT_FIELD(id_resno);
    READ_INT_FIELD(text_score_resno);
    READ_INT_FIELD(vector_distance_resno);
    READ_NODE_FIELD(final_topk);
    READ_NODE_FIELD(text_candidates);
    READ_NODE_FIELD(vector_candidates);
    READ_ENUM_FIELD(fusion, HybridFusionKind);
    READ_FLOAT_FIELD(rrf_k);
    READ_FLOAT_FIELD(text_weight);
    READ_FLOAT_FIELD(vector_weight);
    READ_BOOL_FIELD(parallel_branches);
    READ_BOOL_FIELD(require_parallel);
    READ_NODE_FIELD(recheck_quals);

    READ_DONE();
}
```

注：宏签名已核对（2026-08-21）：`READ_ENUM_FIELD(fldname, enumtype)` :327、
`READ_FLOAT_FIELD(fldname)` :335——与上述代码一致，直接套用。

### 3b. 分发（字符串匹配风格）

old:
```cpp
    } else if (MATCH("LIMIT", 5)) {
        return_value = _readLimit(NULL);
```
new:
```cpp
    } else if (MATCH("LIMIT", 5)) {
        return_value = _readLimit(NULL);
    } else if (MATCH("HYBRIDRECALL", 12)) {
        return_value = _readHybridRecall(NULL);
```

## 4. 验证计划（套用后）

1. `make -C src/common/backend/nodes -sj4`（.cpp-only，增量有效）；
2. 脚本 incremental 安装重启；
3. 9 项定向回归 + golden 双跑比对 S0 基线（无行为变化）；
4. **round-trip 证明的载体待定**：P0-2 时点无入口构造 HybridRecall 节点（Planner P2 才
   产出）。候选：P0-4 ExecInitNode 落地后经自制计划走 nodeToString→stringToNode；或临时
   debug 函数。当前先以"编译闭环 + 回归不变"收口，round-trip 断言挪到有构造入口的第一个
   任务并回填此处（设计文档 §6.4 要求 round-trip 证明，此处记录偏差与计划）。

## 5. 待用户知悉的偏差

- equalfuncs：核对后确认无需改动（依据见上），非遗漏；
- round-trip：见 §4.4，延后但有明确回收点。
