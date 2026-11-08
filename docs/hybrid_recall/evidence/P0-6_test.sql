-- ---------------------------------------------------------------------------
-- P0-6_test.sql：Top-K/tie-break/NULL/NaN 边界（设计文档 §17.1 F08/F09/F10）
-- 前置：P0-4_test.sql 已执行（函数已创建）；需 enable_seqscan=off。
-- 期望逐段写在注释；断言失败表现为输出不符或缺失 ERROR。
-- ---------------------------------------------------------------------------

SET enable_hybrid_recall = on;
SET enable_seqscan = off;

-- T1 / F08 前半：向量权重 0 → 双命中候选与纯文本候选同分（只由文本 rank 决定），
--       纯向量候选全为 0 分沉底。验证权重归零语义 + mask=1/3 混排正确
-- 期望：按文本 rank 1..10 输出（1/(60+t) 递减），分数全部来自文本支路
SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 1.0, 0.0);

-- T2 / F08 后半：双权重 0 → 全体同分 0，tie-break 从 source_mask 开始
-- 期望：top10 全部 source_mask=3（双命中优先），best_rank ASC
SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.0, 0.0);

-- T3 / F09：text 主键（by-ref datumCopy + 文本 hash/eq/lt 语义）
-- 需 text 返回类型的函数变体；本数据各 rank 唯一无并列，故与 int 版同序
DROP FUNCTION IF EXISTS hybrid_recall_debug_text(text,text,int,int,int,int,int,int,float8,float8,float8);
CREATE FUNCTION hybrid_recall_debug_text(text,text,int,int,int,int,int,int,float8,float8,float8)
RETURNS TABLE(doc_id text, source_mask int, text_rank int, vector_rank int, hybrid_score float8)
AS 'hybrid_recall_test.so', 'hybrid_recall_debug_execute' LANGUAGE C VOLATILE;
SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_text(
  'SELECT id::text, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id::text, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);

-- T4 / F10 前半：NULL 主键必须在运行期拒绝（V1 稳定非空主键约束）
DROP TABLE IF EXISTS hybrid_recall_null_docs;
CREATE TABLE hybrid_recall_null_docs AS
  SELECT id, tenant_id, title, content, embedding FROM hybrid_recall_docs WHERE id <= 30;
UPDATE hybrid_recall_null_docs SET id = NULL WHERE id = 5;
CREATE INDEX hrn_bm25_idx ON hybrid_recall_null_docs USING bm25 (content);
-- 期望：ERROR: HybridRecall doc key must not be NULL
SELECT count(*) FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_null_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_null_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);
DROP TABLE hybrid_recall_null_docs;

-- T5 / F10 后半：NaN 支路分数不得污染 hybrid_score
-- NaN 只注入 SELECT 输出值，ORDER BY 保持裸索引序表达式（计划仍为
-- Limit→IndexScan；注意 CASE 进 ORDER BY 会退化为 Sort 路径，其手工树
-- 的 65001-var 投影产出 NULL——载具约束，见 P0-4 记录）。
-- 期望：融合正常输出 10 行、hybrid_score 无 NaN（rank 驱动分数，
--       NaN 原始分只作诊断字段）
SELECT doc_id, source_mask, text_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, CASE id WHEN 5 THEN ''NaN''::float8 ELSE (content <&> ''database'')::float8 END FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);

RESET enable_seqscan;
