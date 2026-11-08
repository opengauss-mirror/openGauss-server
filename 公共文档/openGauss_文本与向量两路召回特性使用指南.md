# openGauss 文本与向量两路召回特性使用指南

> 适用对象：会建表、建索引、执行 `SELECT` 的传统数据库用户。  
> 实现范围：BM25 文本召回 + HNSW 向量召回 + RRF 去重融合，支持串行、强制并行和基于代价自动选择。  
> 文档对应开发分支：`feature/hybrid-recall`，当前实现版本以该分支的 SQL 函数、GUC 和回归用例为准。

---

## 1. 先用一句话理解这个特性

传统文本检索擅长找“包含某个词”的文档，向量检索擅长找“语义相似”的文档。两路召回同时执行两种搜索，再将两份排名融合成一份 Top-K 结果。

```text
用户查询
   ├─ BM25 文本索引 ─────────┐
   │                              ├─ RRF 去重融合 ─ Top-K
   └─ HNSW 向量索引 ─────────┘
```

这里的“并行”是指 BM25 分支和 HNSW 分支可以分别由一个 task worker 同时执行，不是把一个 HNSW 索引扫描拆成多个 worker。

### 1.1 它与 gsql、JDBC 和 OGAI 的关系

```text
gsql / JDBC                 客户端：连接数据库、发送 SQL
       ↓
OGAI SQL 函数              服务端入口：校验参数、管理向量化任务、生成查询
       ↓
HybridRecall 内核计划节点   真正执行 BM25 + HNSW + RRF
```

JDBC 和 gsql 不需要任何改造。OGAI 不是外部大模型，而是 openGauss 服务端的 `contrib/ogai` 扩展。

---

## 2. 使用前检查

本文假设 DBA 已经完成包含本特性的 openGauss 编译、安装和重启。先连接到一个测试数据库：

```bash
gsql -d postgres -p 5432
```

`5432` 只是示例，请换成实际端口。然后执行：

```sql
CREATE EXTENSION IF NOT EXISTS ogai;

SELECT extname, extversion
FROM pg_extension
WHERE extname = 'ogai';

SHOW enable_hybrid_recall;
SHOW hybrid_recall_parallel_mode;

\df ogai.hybrid_rrf_marker
\df ogai.hybrid_search_v2
```

正常情况应当看到：

- OGAI 扩展版本为 `1.0`；
- `enable_hybrid_recall` 存在，默认值为 `off`；
- `hybrid_recall_parallel_mode` 存在，默认值为 `auto`；
- 两个 OGAI 函数都存在。

> `enable_hybrid_recall` 默认关闭是为了便于生产回退。使用本特性的会话必须显式执行 `SET enable_hybrid_recall = on`，或由 DBA 按发布策略设置数据库/用户默认值。

### 2.1 如何判断安装的是否为正确版本

```sql
SELECT name, setting
FROM pg_settings
WHERE name IN (
    'enable_hybrid_recall',
    'hybrid_recall_parallel_mode',
    'hybrid_recall_max_final_topk',
    'hybrid_recall_max_candidates_branch',
    'hybrid_recall_max_candidates_total',
    'hybrid_recall_rrf_k',
    'hybrid_recall_max_candidate_memory_kb'
)
ORDER BY name;
```

如果报 `unrecognized configuration parameter`，表示当前连接的数据库进程不是包含本特性的新二进制，或安装后没有重启正确实例。

---

## 3. 10 分钟快速上手：不调用任何外部模型

这一节是最小可运行实验。向量由 SQL 直接生成，因此不需要 Ollama、OpenAI 或 ONNX 模型。

### 3.1 创建演示表

```sql
DROP TABLE IF EXISTS hybrid_demo_docs CASCADE;

CREATE TABLE hybrid_demo_docs (
    id        integer PRIMARY KEY,
    tenant_id integer NOT NULL,
    title     text NOT NULL,
    content   text NOT NULL,
    embedding vector(8) NOT NULL
);
```

对传统数据库用户来说，只多了一个 `vector(8)` 列。`8` 表示向量有 8 个浮点数维度。真实业务通常使用模型的实际维度，例如 384、768 或 1536，表列、存量向量和查询向量的维度必须一致。

### 3.2 插入可重复的测试数据

```sql
INSERT INTO hybrid_demo_docs
SELECT i,
       i % 4,
       'doc-' || lpad(i::text, 3, '0'),
       (ARRAY['database','kernel','vector','memory','network'])[1 + i % 5]
         || ' '
         || repeat(
              (ARRAY['alpha','beta','gamma','delta','epsilon','zeta','eta'])[1 + i % 7],
              1 + i % 3
            )
         || ' record ' || i,
       ('['
         || round(((i      % 11)::numeric / 11), 4)
         || ',' || round(((i *  2 % 13)::numeric / 13), 4)
         || ',' || round(((i *  3 % 17)::numeric / 17), 4)
         || ',' || round(((i *  5 % 19)::numeric / 19), 4)
         || ',' || round(((i *  7 % 23)::numeric / 23), 4)
         || ',' || round(((i * 11 % 29)::numeric / 29), 4)
         || ',' || round(((i * 13 % 31)::numeric / 31), 4)
         || ',' || round(((i * 17 % 37)::numeric / 37), 4)
         || ']')::vector
FROM generate_series(1, 200) AS i;
```

### 3.3 分别创建 BM25 和 HNSW 索引

```sql
CREATE INDEX hybrid_demo_bm25_idx
    ON hybrid_demo_docs USING bm25(content);

CREATE INDEX hybrid_demo_hnsw_idx
    ON hybrid_demo_docs USING hnsw(embedding vector_cosine_ops);

ANALYZE hybrid_demo_docs;
```

两个索引的用途：

| 索引 | 作用 | 本例中的查询值 |
|---|---|---|
| `bm25(content)` | 根据文本相关性取 Top-N | `'database'` |
| `hnsw(embedding vector_cosine_ops)` | 根据余弦距离取 Top-N | 8 维查询向量 |

HNSW 索引的 `vector_cosine_ops` 与内部使用的余弦距离操作符 `<=>` 匹配。本特性的交付和本文只以 HNSW 为准，不使用 DiskANN。

### 3.4 打开特性并执行第一条两路召回

```sql
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = auto;

SELECT id,
       title,
       ogai.hybrid_rrf_marker(
           id,
           content,
           embedding,
           'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{
             "text_candidates": 40,
             "vector_candidates": 40,
             "rrf_k": 60,
             "weights": [0.5, 0.5]
           }'::jsonb
       ) AS hybrid_score
FROM hybrid_demo_docs
WHERE tenant_id = 1
ORDER BY hybrid_score DESC
LIMIT 8;
```

参数按顺序分别表示：

1. `id`：文档的稳定且非空唯一标识；
2. `content`：BM25 文本列；
3. `embedding`：HNSW 向量列；
4. `'database'`：文本查询；
5. `[...]::vector`：查询向量；
6. JSONB：候选数、RRF 常数和两路权重。

`hybrid_rrf_marker` 的名字里虽然有 marker，但用户不要将它理解为逐行计算函数。它是发给优化器的“这是两路召回”标记；优化器会用专用 `Hybrid Recall` 计划节点取代它。

### 3.5 用 EXPLAIN 确认真的走了两路召回

将上一条 `SELECT` 前加上：

```sql
EXPLAIN (VERBOSE, COSTS OFF)
```

在计划中至少应找到：

```text
Hybrid Recall
  Strategy: Serial RRF       或 Parallel RRF
  Final Top-K: 8
  Text Candidates: 40
  Vector Candidates: 40
  RRF k: 60.000
  Text Weight: 0.500
  Vector Weight: 0.500
  -> Index Scan using hybrid_demo_bm25_idx
  -> Ann Index Scan using hybrid_demo_hnsw_idx
```

如果选择了并行路径，还应看到：

```text
Strategy: Parallel RRF
Planned Branch Workers: 2
-> Streaming(type: LOCAL GATHER)
-> Streaming(type: LOCAL GATHER)
```

验收时可执行 `EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING ON)`，再观察 `Actual Branch Workers`、两路候选行数和 `Branch Overlap`。`ANALYZE` 会真正执行查询；对生产大表先用不带 `ANALYZE` 的计划检查。

---

## 4. 串行、并行和自动选择

### 4.1 三种模式

```sql
-- 专用 HybridRecall 仍然启用，但两个分支顺序执行
SET hybrid_recall_parallel_mode = off;

-- 优化器在串行和并行计划中按代价选择；生产推荐
SET hybrid_recall_parallel_mode = auto;

-- 强制两个分支使用两个 task worker；适合功能验收和压测
SET hybrid_recall_parallel_mode = force;
```

`off` 不等于关闭特性：它仍然使用 `Hybrid Recall` 和 RRF，只是两个分支不并发。

### 4.2 生产建议

- 日常业务使用 `auto`，让优化器权衡 worker 启动和索引扫描成本。
- 短查询不一定适合并行，worker 启动开销可能大于节省的时间。
- `force` 不会绕过查询形状、索引、参数上限等正确性检查，也可能因 worker 资源不足而失败。
- 并发用户很多时，并行不一定比串行快，应用真实数据和并发度压测。

### 4.3 紧急回退

当发现线上问题时，可在新会话中关闭专用计划：

```sql
SET enable_hybrid_recall = off;
```

直接调用 marker 的 SQL 在关闭后不能继续执行；应用程序推荐通过第 7 节的 `hybrid_search_v2` 调用，它在默认 `on_unsupported=legacy` 时可改走旧的串行实现。如果已经缓存了 HybridRecall 计划，关闭后应丢弃并重新生成该计划。

---

## 5. RRF 参数怎么填

### 5.1 四个最常用参数

```json
{
  "text_candidates": 100,
  "vector_candidates": 100,
  "rrf_k": 60,
  "weights": [0.5, 0.5]
}
```

| 参数 | 含义 | 入门建议 |
|---|---|---|
| `text_candidates` | BM25 分支取多少条候选 | 从 `100` 开始 |
| `vector_candidates` | HNSW 分支取多少条候选 | 从 `100` 开始 |
| `rrf_k` | 平滑两路排名差异的常数 | 使用默认 `60` |
| `weights` | `[BM25权重, 向量权重]` | 从 `[0.5,0.5]` 开始 |

RRF 不直接相加 BM25 分数和向量距离，而是相加两路的排名贡献：

```text
融合分 = 文本权重 / (rrf_k + 文本名次)
       + 向量权重 / (rrf_k + 向量名次)
```

某条记录如果只在一路命中，只计算该路的贡献。

### 5.2 参数限制

默认安全上限为：

| 项目 | 默认上限 |
|---|---:|
| 最终 `LIMIT K` | 100 |
| 每个分支的候选数 | 1000 |
| 两分支候选数之和 | 2000 |
| 候选融合内存 | 65536 kB |

并且必须满足：

```text
text_candidates   >= LIMIT K
vector_candidates >= LIMIT K
rrf_k             > 0
weights 中两个值  >= 0
```

`weights` 不强制相加等于 1，但为了便于解释和横向对比，建议做归一化。

### 5.3 怎么调参

- 结果太偏关键词：降低第一个权重，例如 `[0.3,0.7]`。
- 结果太偏语义相似，精确词经常漏掉：提高第一个权重，例如 `[0.7,0.3]`。
- 召回率不足：逐步增大两路 candidate，而不是盲目增大最终 `LIMIT`。
- 延迟过高：先减少 candidate，再根据 `EXPLAIN ANALYZE` 判断是 BM25、HNSW 还是融合阶段慢。
- 带高选择性过滤条件时，HNSW 计划可能显示 `Filter Mode: Post-Filter`；可适当增大 `vector_candidates` 和 `hnsw_ef_search`，并以真实召回率验证。

---

## 6. 直接 marker SQL 的必须形状

V1 为了保证语义和并行安全，只识别形状明确的查询。应当保持：

- 顶层是一条只读 `SELECT`；
- `FROM` 中只有一张普通表；
- 只有一个排序键，且为 `ORDER BY hybrid_score DESC`；
- 有有限且大于 0 的 `LIMIT`，没有 `OFFSET`；
- marker 前三个参数是同一张表的普通列；
- 文本列上有 BM25 索引，向量列上有 HNSW 索引；
- `WHERE` 条件可以安全地同时下推到两个分支；
- 不含子查询、聚合、窗口函数、`GROUP BY`、`DISTINCT`、集合操作或行锁。

正确模板：

```sql
SELECT <需要返回的列>,
       ogai.hybrid_rrf_marker(
           <主键列>, <文本列>, <向量列>,
           <查询文本>, <查询向量>, <常量JSONB选项>
       ) AS hybrid_score
FROM <一张普通表>
WHERE <可选的普通过滤条件>
ORDER BY hybrid_score DESC
LIMIT <K>;
```

以下写法在 V1 中不支持：

```sql
-- 不支持：第二个排序键
ORDER BY hybrid_score DESC, id;

-- 不支持：OFFSET
ORDER BY hybrid_score DESC LIMIT 10 OFFSET 10;

-- 不支持：子查询或多表 JOIN
WHERE EXISTS (SELECT 1 ...);
```

如果查询未被优化器识别，marker 不会像普通函数一样计算，而是故意报错：

```text
ERROR: ogai.hybrid_rrf_marker must be executed by HybridRecall plan
```

这个错误通常表示特性未打开、索引缺失，或 SQL 形状不在 V1 支持范围内，不表示 marker 函数本身损坏。

---

## 7. 真实业务推荐入口：`ogai.hybrid_search_v2`

直接 marker 适合快速验证、自己管理 embedding 的系统和底层 SQL 调优。普通应用推荐调用 `hybrid_search_v2`，因为它会：

- 从 OGAI 任务中找到表、主键、文本列和 `ogai_embedding` 列；
- 只生成一次查询向量；
- 校验任务所有者、表权限、列名、过滤值和参数上限；
- 拒绝原始 `WHERE` SQL 拼接，降低 SQL 注入风险；
- 在默认策略下，特性关闭或条件不满足时走 legacy 实现。

### 7.1 已有 OGAI append 向量化任务时

任务必须满足：

- 任务属于当前数据库用户；
- `method = 'append'`；
- `enable_bm25 = true`；
- 源表有 `ogai_embedding` 向量列；
- 文本列有 BM25 索引，`ogai_embedding` 有 HNSW 索引。

调用示例：

```sql
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = auto;

SELECT result_record
FROM ogai.hybrid_search_v2(
    p_task_name    => 'docs_task',
    p_query        => 'openGauss 并行向量检索',
    p_return_cols  => 'id,title,content',
    p_limit        => 10,
    p_where_clause => '',
    p_options      => '{
      "strategy": "parallel_rrf",
      "text_candidates": 100,
      "vector_candidates": 100,
      "rrf_k": 60,
      "weights": [0.5, 0.5],
      "on_worker_shortage": "serial",
      "on_unsupported": "legacy",
      "filters": {"tenant_id": 42}
    }'::jsonb
);
```

每条返回记录是一个 JSONB，包含 `p_return_cols` 中的列和 `hybrid_score`。

注意：

- `p_where_clause` 在 V2 中必须为空字符串；
- 等值过滤必须写在 `p_options.filters` 中；
- filter 值只能是 JSON 标量或 `null`，不支持数组、对象、范围和任意 SQL；
- `p_return_cols` 只能是逗号分隔的真实列名，不能放表达式或子查询；
- `on_unsupported = 'error'` 可用于验收，它会在无法使用 HybridRecall 时直接报出原因；
- `query_vector` 选项可以直接传入应用已经生成的向量，从而不调用 OGAI embedding provider。

直接传入查询向量的示例：

```sql
SELECT result_record
FROM ogai.hybrid_search_v2(
    'docs_task',
    'openGauss 并行向量检索',
    'id,title',
    10,
    '',
    '{
      "text_candidates": 100,
      "vector_candidates": 100,
      "rrf_k": 60,
      "weights": [0.5, 0.5],
      "query_vector": [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],
      "filters": {"tenant_id": 42}
    }'::jsonb
);
```

`query_vector` 的维度和生成方法必须与存量向量一致。不能用模型 A 建库，用模型 B 生成查询向量。

### 7.2 从普通文本表创建 OGAI 任务

如果数据表还没有 embedding，可以让 OGAI 调用 embedding provider。先确认 provider 的模型名、完整 HTTP API URL、API key 和向量维度。以 Ollama 模板为例：

```sql
INSERT INTO ogai.model_sources(
    model_key,
    model_name,
    model_provider,
    url,
    description,
    api_key,
    owner_name
)
VALUES (
    'my_embedding',
    '<Ollama中的实际模型名>',
    'ollama',
    'http://127.0.0.1:11434/api/embed',
    'local embedding model',
    NULL,
    CURRENT_USER
);
```

`url` 要填可以直接接收 embedding POST 请求的完整地址，不是只填服务根地址。不同 provider 的请求和响应格式不同，必须选择与实际服务兼容的 `model_provider`。

假设原表为：

```sql
CREATE TABLE documents (
    id        bigint PRIMARY KEY,
    tenant_id integer NOT NULL,
    title     text NOT NULL,
    content   text NOT NULL
);
```

创建同步 append 任务：

```sql
SELECT *
FROM ogai.ai_vectorize(
    p_task_name         => 'docs_task',
    p_task_type         => 'sync',
    p_index_type        => 'cosine',
    p_embed_model       => 'my_embedding',
    p_src_schema        => 'public',
    p_src_table         => 'documents',
    p_src_col           => 'content',
    p_primary_key       => 'id',
    p_table_method      => 'append',
    p_dim               => <模型的实际向量维度>,
    p_max_chunk_size    => 1000,
    p_max_chunk_overlap => 200,
    p_enable_bm25       => true
);
```

`<...>` 是需要替换的占位符，不能原样执行。成功后 OGAI 会在 append 模式中：

1. 向源表增加 `ogai_embedding vector(dim)` 列；
2. 为存量行生成 embedding；
3. 在文本列上创建 BM25 索引；
4. 在 `ogai_embedding` 上创建 HNSW 索引；
5. 记录 `docs_task` 任务元数据。

用下列 SQL 检查：

```sql
SELECT task_name, type, index_type, src_schema, src_table,
       src_col, primary_key, method, dim, enable_bm25
FROM ogai.vectorize_tasks
WHERE task_name = 'docs_task';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename = 'documents'
ORDER BY indexname;

SELECT count(*) AS rows_without_embedding
FROM documents
WHERE ogai_embedding IS NULL;
```

在当前 OGAI 实现中，向量化数据的 `UPDATE` 有限制；修改已向量化文本时要按当前 OGAI 约束使用 `DELETE + INSERT`，或由管理员重建向量化任务。不要在生产环境中直接手工修改 `ogai.vectorize_tasks` 元数据。

---

## 8. Oracle 是如何使用两路召回的

本节以 Oracle AI Database 26ai 的官方接口为例。Oracle 不向用户暴露类似 `hybrid_rrf_marker` 的优化器标记，而是先建立一个统一的 Hybrid Vector Index，再调用 `DBMS_HYBRID_VECTOR` 数据库包。

### 8.1 Oracle 先把表、文本列和向量化方式写入索引

假设 Oracle 表中的文档内容列为 `content`，最小建索引示例为：

```sql
CREATE HYBRID VECTOR INDEX docs_hybrid_idx
ON documents(content)
PARAMETERS ('MODEL doc_embed_model VECTOR_IDXTYPE HNSW');
```

这一条 DDL 已经把下列信息绑定到 `docs_hybrid_idx` 的索引元数据中：

- 基础表是 `documents`；
- 文本来自 `content`；
- 使用 `doc_embed_model` 生成 embedding；
- 向量索引类型是 HNSW；
- 同一个混合索引内同时维护 Oracle Text 数据结构和向量数据结构。

如果需要控制分块、重叠、embedding provider、距离类型和 HNSW 等更多参数，Oracle 先创建 vectorizer preference，再将它绑到混合索引：

```sql
BEGIN
    DBMS_VECTOR_CHAIN.CREATE_PREFERENCE(
        'docs_vectorizer_pref',
        DBMS_VECTOR_CHAIN.VECTORIZER,
        JSON('{
          "vector_idxtype": "hnsw",
          "model": "doc_embed_model",
          "by": "words",
          "max": 100,
          "overlap": 10,
          "split": "recursively"
        }')
    );
END;
/

CREATE HYBRID VECTOR INDEX docs_hybrid_idx
ON documents(content)
PARAMETERS ('VECTORIZER docs_vectorizer_pref');
```

上述 `PARALLEL n` 等 Oracle 建索引选项主要控制索引创建资源，不能等同于本特性的 `hybrid_recall_parallel_mode`。Oracle 官方 SQL API 没有要求用户在查询中指定“两分支使用两个 worker”。

### 8.2 Oracle 的查询写法

最简单的用法是只给索引名和一份查询文本：

```sql
SELECT JSON_SERIALIZE(
         DBMS_HYBRID_VECTOR.SEARCH(
           JSON('{
             "hybrid_index_name": "docs_hybrid_idx",
             "search_text": "openGauss parallel database",
             "search_scorer": "RRF",
             "return": {
               "values": ["rowid", "score", "vector_score", "text_score"],
               "topN": 10
             }
           }')
         ) RETURNING CLOB PRETTY
       )
FROM dual;
```

Oracle 会把同一份 `search_text`：

1. 转换成 Oracle Text `CONTAINS` 查询，完成关键词召回；
2. 使用建索引时配置的同一个 embedding 模型生成查询向量；
3. 在混合索引内部的向量数据上执行 `VECTOR_DISTANCE` 查询；
4. 按 `RRF`、`RSF` 或 `WRRF` 融合排名，返回 `topN`。

如果文本查询和语义查询不同，可以分开传入：

```sql
SELECT DBMS_HYBRID_VECTOR.SEARCH(
         JSON('{
           "hybrid_index_name": "docs_hybrid_idx",
           "search_fusion": "UNION",
           "search_scorer": "RRF",
           "vector": {
             "search_text": "parallel database execution",
             "search_mode": "DOCUMENT",
             "result_max": 100
           },
           "text": {
             "contains": "openGauss AND parallel",
             "result_max": 100
           },
           "return": {
             "values": ["rowid", "score", "vector_rank", "text_rank"],
             "topN": 10
           }
         }')
       )
FROM dual;
```

如果应用已经有查询向量，则使用 `vector.search_vector`，不需要让 Oracle 再从 `search_text` 生成向量。该向量仍必须与建索引时使用的 embedding 模型兼容。

Oracle 26ai 还提供了返回关系型行的 pipeline 函数：

```sql
SELECT doc_rowid,
       score,
       vector_score,
       text_score,
       vector_rank,
       text_rank
FROM DBMS_HYBRID_VECTOR.SEARCHPIPELINE(
       JSON('{
         "hybrid_index_name": "docs_hybrid_idx",
         "search_text": "openGauss parallel database"
       }')
     );
```

### 8.3 Oracle 为什么不需要 marker 里的六个字段

我们的底层 marker 是：

```sql
ogai.hybrid_rrf_marker(
    id,
    text_col,
    vec_col,
    q_text,
    q_vec,
    opts
)
```

Oracle 并不是不需要这些信息，而是将信息分别放到了混合索引元数据和 `SEARCH` JSON 中：

| 我们 marker 的参数 | Oracle 的信息来源 | Oracle 查询时为什么不再传列名 |
|---|---|---|
| `id` | 基础表 `ROWID`、混合索引内部 `DOC_ROWID` | Oracle 用 `ROWID` 维护文档与基础表的映射，结果可用 `ROWID` 回表获取业务主键 |
| `text_col` | `CREATE HYBRID VECTOR INDEX ... ON table(text_column)` | 文本列已在建索引时绑定 |
| `vec_col` | Hybrid Vector Index 内部的 chunk embedding | Oracle 由 vectorizer 生成并管理内部向量，普通查询不需要指定基表向量列 |
| `q_text` | `search_text` 或 `text.contains` | 仍在每次 `SEARCH` 时传入，只是放在 JSON 中 |
| `q_vec` | 同一 vectorizer 将 `search_text` 转为向量，或 `vector.search_vector` | 可自动生成，也可显式传入，并没有消失 |
| `opts` | `search_fusion`、`search_scorer`、`vector/text.result_max`、权重、rank penalty、`return.topN` 等 | 仍在 `SEARCH` JSON 中传入，Oracle 还能根据 `topN` 和搜索模式推导默认 candidate 上限 |

Oracle 的混合索引内部会保留类似下列映射：

```text
DOC_ROWID            基础表文档的 ROWID
DOC_CHUNK_ID         文档分块编号
DOC_CHUNK_TEXT       分块文本
DOC_CHUNK_EMBEDDING  分块向量
```

因此 Oracle 只需要 `hybrid_index_name` 就能从数据字典找到表、文本数据、向量数据、文档标识和 vectorizer。

我们的当前实现不同：

```text
普通表.content   ─ 独立 BM25 索引
普通表.embedding ─ 独立 HNSW 索引
```

openGauss 目前没有一个类似 Oracle `HYBRID VECTOR INDEX` 的统一 catalog 对象，也没有新增混合搜索 SQL 语法。优化器面对两个互不关联的普通索引，所以 marker 必须显式告诉它：

- 用哪个列对两路候选去重；
- 哪个文本列对应 BM25；
- 哪个向量列对应 HNSW；
- 两路各自的查询值；
- 候选上限和 RRF 融合参数。

所以根本差异是：

> Oracle 把“哪张表、哪个文本列、哪份内部向量和如何回表”预先封装到了统一混合索引中；我们使用两个独立索引，因此需要 marker 在每条查询中建立它们之间的语义关系。

### 8.4 Oracle 接口与我们的层次对应

| 层次 | Oracle | openGauss 当前实现 |
|---|---|---|
| 客户端 | JDBC、SQL*Plus | JDBC、gsql |
| 高层搜索 API | `DBMS_HYBRID_VECTOR.SEARCH` | 可选的 `ogai.hybrid_search_v2` |
| 用户可见的优化器标记 | 无 | `ogai.hybrid_rrf_marker` |
| 索引组织 | 一个 `HYBRID VECTOR INDEX` 封装文本与向量 | 独立 BM25 + 独立 HNSW |
| 文档标识 | 内部 `ROWID/DOC_ROWID` | marker 的 `id` 列 |
| 融合配置 | `SEARCH` JSON | marker 的 `opts` JSONB |
| 执行层 | Oracle 内部混合检索实现 | `HybridRecall` Plan/Executor |

Oracle 没有公开要求用户传递的 marker，不等于其内部没有等价的语义识别和计划构造机制。可以确定的是，Oracle 将它们隐藏在混合索引和 `DBMS_HYBRID_VECTOR` 实现内部，用户只操作稳定的高层 API。

Oracle 官方参考：

- [CREATE HYBRID VECTOR INDEX](https://docs.oracle.com/en/database/oracle/oracle-database/26/ccref/create-hybrid-vector-index.html)
- [DBMS_HYBRID_VECTOR.SEARCH](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/search.html)
- [DBMS_HYBRID_VECTOR.SEARCHPIPELINE](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/searchpipeline.html)
- [Query Hybrid Vector Indexes End-to-End Example](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/query-hybrid-vector-indexes-end-end-example.html)
- [Hybrid Vector Index `$VECTORS` dictionary view](https://docs.oracle.com/en/database/oracle/oracle-database/26/vecse/indexnamevectors.html)

---

## 9. JDBC 调用示例

JDBC 不需要了解 marker 和内部计划，应用程序可以像调用普通查询一样调用 `hybrid_search_v2`：

```java
String sql =
    "SELECT result_record::text " +
    "FROM ogai.hybrid_search_v2(?, ?, ?, ?, '', ?::jsonb)";

try (Connection conn = dataSource.getConnection()) {
    try (Statement st = conn.createStatement()) {
        st.execute("SET enable_hybrid_recall = on");
        st.execute("SET hybrid_recall_parallel_mode = auto");
    }

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
        ps.setString(1, "docs_task");
        ps.setString(2, "openGauss 并行向量检索");
        ps.setString(3, "id,title,content");
        ps.setInt(4, 10);
        ps.setString(5,
            "{\"text_candidates\":100," +
            "\"vector_candidates\":100," +
            "\"rrf_k\":60," +
            "\"weights\":[0.5,0.5]," +
            "\"filters\":{\"tenant_id\":42}}");

        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String resultJson = rs.getString(1);
                System.out.println(resultJson);
            }
        }
    }
}
```

实际项目中建议由连接池的连接初始化 SQL 或 DBA 的用户/数据库级配置统一设置 GUC，避免业务代码漏设。不要将用户输入拼到 SQL 字符串中，继续使用 `PreparedStatement` 绑定参数。

---

## 10. 常见问题

### 10.1 `extension "ogai" is not available`

原因：`ogai.control`、`ogai--1.0.sql` 或 `ogai.so` 没有安装到当前 openGauss 实例对应的目录。

处理：由 DBA 使用同一份源码和同一个 `GAUSSHOME` 重新编译、安装 `contrib/ogai`，再连接正确实例。

### 10.2 `unrecognized configuration parameter "enable_hybrid_recall"`

当前运行的是旧内核，或者安装了新文件但没有重启到新二进制。检查服务端实际执行文件、`GAUSSHOME`、数据目录和端口。

### 10.3 `hybrid_rrf_marker must be executed by HybridRecall plan`

按顺序检查：

1. `SHOW enable_hybrid_recall;` 是否为 `on`；
2. 文本列是否有 BM25 索引；
3. 向量列是否有 HNSW 索引；
4. SQL 是否只查一张表、只有 marker 一个排序键、有 `LIMIT`且没有 `OFFSET`；
5. SQL 是否包含 JOIN、子查询、聚合、窗口或 `DISTINCT`；
6. marker 的前三个参数是否为同表的直接列。

### 10.4 `branch candidate caps must be >= final_topk`

例如 `LIMIT 50`，但 `text_candidates=20`。把两路 candidate 都设为至少 50。

### 10.5 查询向量维度错误

`vector(768)` 列必须接收 768 维查询向量。确认 embedding 模型、表列声明、存量向量和查询向量使用相同维度。

### 10.6 `task not found or not owned by current user`

`hybrid_search_v2` 只允许任务所有者调用自己的任务。使用创建任务的用户连接，并确保该用户对源表有 `SELECT` 权限。

### 10.7 `does not accept raw p_where_clause SQL`

V2 故意禁止原始 SQL 过滤字符串。把：

```text
tenant_id = 42
```

改为 options 中的：

```json
{"filters":{"tenant_id":42}}
```

### 10.8 小表测试没有变快

这很正常。小表和小 candidate 的 worker 启动成本可能大于并行收益。功能正确性用小表验证，性能收益应在与生产相近的数据量、候选数、过滤选择率和并发度下测试。

---

## 11. 验收清单

一个环境可以开始使用本特性，至少应满足下列条件：

- [ ] `CREATE EXTENSION ogai` 成功，两个混合检索函数存在。
- [ ] `SHOW enable_hybrid_recall` 不报错，测试会话已设为 `on`。
- [ ] 目标文本列有 BM25 索引。
- [ ] 目标向量列有 HNSW 索引，opclass 与距离类型一致。
- [ ] `EXPLAIN` 中有 `Hybrid Recall`、BM25 `Index Scan` 和 HNSW `Ann Index Scan`。
- [ ] `parallel_mode=off` 时显示 `Serial RRF`。
- [ ] `parallel_mode=force` 时显示 `Parallel RRF`、`Planned Branch Workers: 2` 和两个 `LOCAL GATHER`。
- [ ] 相同查询的串行和并行结果的 ID、顺序和融合分数一致。
- [ ] 在真实数据上完成召回质量和性能对比，而不是只验证计划形状。
- [ ] 应用使用参数绑定，没有拼接用户输入 SQL。
- [ ] 已演练 `SET enable_hybrid_recall=off` 回退策略。

### 11.1 独立原生回归用例

源码树已提供一个可独立调度的回归用例：

```text
src/test/regress/sql/hybrid_recall.sql
src/test/regress/expected/hybrid_recall.out
src/test/regress/parallel_schedule.hybrid_recall
```

在已正确 configure 的源码树中执行：

```bash
make -C src/test/regress hybrid_recallcheck
```

该用例同时看护：

- 串行计划；
- 强制并行计划；
- 代价模式选择串行和并行；
- BM25 和 HNSW 索引路径；
- 过滤条件下推；
- 子查询、`OFFSET`、两个排序键和缺失 HNSW 索引时不生成专用计划；
- 串行、强制并行和 auto 两种选择的有序结果与融合分数一致。

---

## 12. 常用配置速查

| GUC | 默认值 | 用途 |
|---|---:|---|
| `enable_hybrid_recall` | `off` | 特性总开关和紧急回退开关 |
| `hybrid_recall_parallel_mode` | `auto` | `off / auto / force` |
| `hybrid_recall_max_final_topk` | `100` | 最终 Top-K 上限 |
| `hybrid_recall_max_candidates_branch` | `1000` | 单分支候选上限 |
| `hybrid_recall_max_candidates_total` | `2000` | 两分支候选总上限 |
| `hybrid_recall_rrf_k` | `60` | marker 未显式给出 `rrf_k` 时的默认值 |
| `hybrid_recall_max_candidate_memory_kb` | `65536` | 候选去重、payload 和 Top-K 堆的内存上限 |

查看当前值：

```sql
SHOW enable_hybrid_recall;
SHOW hybrid_recall_parallel_mode;
SHOW hybrid_recall_max_final_topk;
SHOW hybrid_recall_max_candidates_branch;
SHOW hybrid_recall_max_candidates_total;
SHOW hybrid_recall_rrf_k;
SHOW hybrid_recall_max_candidate_memory_kb;
```

除了会话级 `SET`，不建议初学者直接改全局上限。调高 candidate 和内存上限会放大单查询的 CPU、内存、worker 和索引 I/O 消耗，应由 DBA 基于并发压测决定。

---

## 13. 最小心智模型

对于刚上手的用户，只需记住下面六句话：

1. 一张表需要主键、TEXT 列和同模型生成的 VECTOR 列。
2. TEXT 列建 BM25，VECTOR 列建 HNSW。
3. 先执行 `SET enable_hybrid_recall=on`。
4. 业务优先调 `ogai.hybrid_search_v2`，底层验证可直接用 marker SQL。
5. 用 `EXPLAIN` 确认同时出现 `Hybrid Recall`、BM25 Index Scan 和 HNSW Ann Index Scan。
6. 生产使用 `auto`，验收使用 `force`，发现问题用总开关回退。
