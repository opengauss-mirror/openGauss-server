-- ---------------------------------------------------------------------------
-- P0-5_test.sql：HybridRecall 边界用例（对照设计文档 §17.1 F02/F04/F05/F06/F07
-- 与 §7.5 资源校验、§8.2 支路上限）
-- 前置：先执行 P0-4_test.sql（LOAD + CREATE FUNCTION 已完成）
-- 期望值均可用 SQL 手算对照，逐段断言写在注释；失败表现为输出不符或缺失 ERROR。
-- ---------------------------------------------------------------------------

SET enable_hybrid_recall = on;
SET enable_seqscan = off;

-- F04：文本支路零结果（tenant 99 无行）→ 仅向量路候选，source_mask 必为 2
-- 期望：10 行，全部 mask=2、text_rank=0
SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 99 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);

-- F05：两路均零结果 → 0 行
SELECT count(*) AS f05_rows
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 99 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 99 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);

-- F06：final K 达到上限（并集 25+100-21=104 > K=100=caps）
-- 期望：返回 min(K, 去重并集)=100 行，且含 mask=3
SELECT count(*) AS f06_rows, max(source_mask) AS f06_maxmask
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 100, 100, 100, 60.0, 0.5, 0.5);

-- F07：不等权重 0.7/0.3 → 排名应符合公式（与等权版对比应出现排序变化）
SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.7, 0.3);

-- §7.5 资源校验：branch cap < final_topk → 必须报错
-- 期望：ERROR: HybridRecall branch candidate caps must be >= final_topk
SELECT count(*) FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 50, 100, 10, 60.0, 0.5, 0.5);

-- §8.2 有界候选：branch SQL 产出超过 cap（SQL LIMIT 100，cap 5）→ 必须报错
-- 期望：ERROR: HybridRecall text branch exceeded its candidate cap 5
SELECT count(*) FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 5, 5, 5, 60.0, 0.5, 0.5);

RESET enable_seqscan;
