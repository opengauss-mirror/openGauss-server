-- ---------------------------------------------------------------------------
-- hybrid_recall_baseline.sql
-- S0-4/S0-5: 确定性两路召回基线（HybridRecall 项目 golden 种子）
--
-- 目的：
--   1. 固定 seed 语义（无随机数，全部由 i 的模运算派生）→ 重复执行结果一致；
--   2. BM25 与 ANN(HNSW) 两路各自的 Top-K（含 tenant 过滤）；
--   3. 手写 RRF 融合 = 未来内核串行 HybridRecall 必须对齐的 golden 语义；
--   4. EXPLAIN (ANALYZE) 证明两路均走索引有序 Top-K 扫描（非全表 Sort）。
--
-- 运行：gsql -X -d postgres -p 55440 -f src/test/regress/sql/hybrid_recall_baseline.sql
-- 判定：连续两次执行输出逐字节一致（分数已 round 到 4 位规避浮点打印噪声）。
-- ---------------------------------------------------------------------------

SET client_min_messages = warning;
DROP TABLE IF EXISTS hybrid_recall_docs CASCADE;

CREATE TABLE hybrid_recall_docs (
    id        int PRIMARY KEY,
    tenant_id int NOT NULL,
    title     text NOT NULL,
    content   text NOT NULL,
    embedding vector(8) NOT NULL
);

-- 500 行确定性数据：
--   content  = 主题词(i%5) x 重复(1..3) + 副词(i%7)  → BM25 分数有区分度且可手算
--   embedding= 8 个互异质数模运算分量              → 余弦距离有确定性分布
INSERT INTO hybrid_recall_docs
SELECT i,
       i % 4,
       'doc-' || lpad(i::text, 3, '0'),
       (ARRAY['database','kernel','vector','memory','network'])[1 + i % 5]
         || ' ' || repeat((ARRAY['alpha','beta','gamma','delta','epsilon','zeta','eta'])[1 + i % 7], 1 + i % 3)
         || ' record ' || i,
       ('['  || round((( i       % 11)::numeric / 11), 4)
         || ',' || round((( i *  2  % 13)::numeric / 13), 4)
         || ',' || round((( i *  3  % 17)::numeric / 17), 4)
         || ',' || round((( i *  5  % 19)::numeric / 19), 4)
         || ',' || round((( i *  7  % 23)::numeric / 23), 4)
         || ',' || round((( i * 11  % 29)::numeric / 29), 4)
         || ',' || round((( i * 13  % 31)::numeric / 31), 4)
         || ',' || round((( i * 17  % 37)::numeric / 37), 4)
       || ']')::vector
FROM generate_series(1, 500) AS i;

CREATE INDEX hrd_bm25_idx ON hybrid_recall_docs USING bm25 (content);
CREATE INDEX hrd_hnsw_idx ON hybrid_recall_docs USING hnsw (embedding vector_cosine_ops);

ANALYZE hybrid_recall_docs;

-- ===========================================================================
-- 查询参数（固定）
-- ===========================================================================
-- 文本查询：'database'；租户过滤：tenant_id = 1；候选：各 100；final K：10
-- 向量查询：固定 8 维字面量

-- ---------------------------------------------------------------------------
-- A. 文本支路 Top-K（BM25 索引有序扫描）
-- ---------------------------------------------------------------------------
EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF)
SELECT id, round((content <&> 'database')::numeric, 4) AS text_score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY content <&> 'database' DESC
LIMIT 100;

-- ---------------------------------------------------------------------------
-- B. 向量支路 Top-K（HNSW AnnIndexScan）
-- ---------------------------------------------------------------------------
EXplain (ANALYZE ON, VERBOSE ON, COSTS OFF)
SELECT id, round((embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')::numeric, 4) AS vector_distance
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'
LIMIT 100;

-- B2. 证明 ANN 索引路径可达：500 行小表上默认计划选 SeqScan+Sort（成本模型合理），
--     与 ogai.hybrid_search 同法关闭 seqscan 后应得 AnnIndexScan（近似 Top-K）。
--     该段为证据记录，不进入字节级 golden（ANN 近似语义另行说明）。
SET enable_seqscan = off;
EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF)
SELECT id, round((embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')::numeric, 4) AS vector_distance
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'
LIMIT 10;
RESET enable_seqscan;

\echo ===GOLDEN_START===
-- 注：BM25 打分函数有执行期守卫（bm25scan.cpp bm25_scores_text：无 BM25 索引扫描
-- 即 ERROR），且窗口函数形态下规划器可能选 Sort 路径。故 C/D 段与 ogai.hybrid_search
-- 同法关闭 seqscan，强制两支路走各自生产索引（BM25 / HNSW）——与未来内核算子一致。
SET enable_seqscan = off;
-- ---------------------------------------------------------------------------
-- C. 手写串行 RRF 融合（golden 语义）
--    hybrid = 0.5/(60+text_rank) + 0.5/(60+vector_rank)，rrf_k=60
--    tie-break: hybrid DESC, source_mask DESC, best_rank ASC, id ASC
-- ---------------------------------------------------------------------------
WITH text_path AS (
    SELECT id, round((content <&> 'database')::numeric, 4) AS text_score,
           row_number() OVER (ORDER BY content <&> 'database' DESC) AS text_rank
    FROM hybrid_recall_docs
    WHERE tenant_id = 1
    ORDER BY content <&> 'database' DESC
    LIMIT 100
),
vector_path AS (
    SELECT id, round((embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]')::numeric, 4) AS vector_distance,
           row_number() OVER (ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS vector_rank
    FROM hybrid_recall_docs
    WHERE tenant_id = 1
    ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'
    LIMIT 100
),
fused AS (
    SELECT coalesce(t.id, v.id) AS id,
           (CASE WHEN t.id IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN v.id IS NOT NULL THEN 2 ELSE 0 END) AS source_mask,
           coalesce(t.text_rank, 0)   AS text_rank,
           coalesce(v.vector_rank, 0) AS vector_rank,
           t.text_score,
           v.vector_distance,
           /* 设计文档 §3.4 标准语义：未命中的支路不贡献分数。
            * 初版 golden 曾用 coalesce(rank,0)，等价于给未命中支路
            * 加 weight/rrf_k 常量偏置，会改变单路与双路候选间的排序，
            * 与内核实现（P0-4）对照时暴露，此处修正为 hit-only。 */
           round( CASE WHEN t.id IS NOT NULL THEN 0.5 / (60 + t.text_rank) ELSE 0 END
                + CASE WHEN v.id IS NOT NULL THEN 0.5 / (60 + v.vector_rank) ELSE 0 END
                , 8) AS hybrid_score
    FROM text_path t
    FULL JOIN vector_path v ON v.id = t.id
)
SELECT id, source_mask, text_rank, vector_rank, text_score, vector_distance, hybrid_score
FROM fused
ORDER BY hybrid_score DESC,
         source_mask DESC,
         least(coalesce(nullif(text_rank, 0), 2147483647),
               coalesce(nullif(vector_rank, 0), 2147483647)) ASC,
         id ASC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- D. 统计断言（供自动核对）
-- ---------------------------------------------------------------------------
WITH tp AS (
    SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
      ORDER BY content <&> 'database' DESC LIMIT 100
), vp AS (
    SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
      ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]' LIMIT 100
)
SELECT (SELECT count(*) FROM tp) AS text_rows,
       (SELECT count(*) FROM vp) AS vector_rows,
       (SELECT count(*) FROM tp JOIN vp USING (id)) AS both_rows,
       (SELECT count(*) FROM tp WHERE id IN (SELECT id FROM vp)) AS overlap;
RESET enable_seqscan;
\echo ===GOLDEN_END===
