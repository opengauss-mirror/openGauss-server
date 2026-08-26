-- P2-1 marker 识别白名单验证（SET client_min_messages=debug1 后运行）
SET enable_hybrid_recall = on;
-- 正向：应输出 "hybrid_recall P2-1 recognize: ... bm25=1 ann=1"
EXPLAIN (COSTS OFF) SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector, '{}'::jsonb) DESC LIMIT 10;
-- 负向×3（无 LIMIT / 非 marker 排序 / 普通查询）：均不得出现 recognize 行
EXPLAIN (COSTS OFF) SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector, '{}'::jsonb) DESC;
EXPLAIN (COSTS OFF) SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY content <&> 'database' DESC LIMIT 10;
EXPLAIN (COSTS OFF) SELECT id FROM hybrid_recall_docs ORDER BY id LIMIT 10;
