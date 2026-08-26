-- ---------------------------------------------------------------------------
-- P0-4_test.sql：内核串行 HybridRecall vs golden 手写 RRF 对照
--
-- 载具：hybrid_recall_test.so（docs/hybrid_recall/p04_test_module/，build.sh
--   会部署到 $GAUSSHOME/lib/postgresql/proc_srclib/）。
--   openGauss 无 LOAD；C 函数库走 proc_srclib；CREATE 会固化拷贝到 pg_plugin，
--   更新 .so 后必须 DROP+CREATE。
-- 参数与 golden 完全一致：tenant=1、文本 'database'、固定 8 维向量、
--   各 100 候选、final K=10、rrf_k=60、权重 0.5/0.5。
-- 必须 SET enable_seqscan=off：①与 golden C 段同样走 BM25/HNSW 索引序对齐 rank；
--   ②手工树下 Sort 节点的 65001-var 投影产出 NULL 列（见 P0-4 记录）。
-- 期望输出与 S0-5 golden（hit-only 修正版）逐行一致。
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS hybrid_recall_debug_execute(text,text,int,int,int,int,int,int,float8,float8,float8);
CREATE FUNCTION hybrid_recall_debug_execute(text,text,int,int,int,int,int,int,float8,float8,float8)
RETURNS TABLE(doc_id int, source_mask int, text_rank int, vector_rank int, hybrid_score float8)
AS 'hybrid_recall_test.so', 'hybrid_recall_debug_execute'
LANGUAGE C VOLATILE;

SET enable_hybrid_recall = on;
SET enable_seqscan = off;

SELECT doc_id, source_mask, text_rank, vector_rank, round(hybrid_score, 8) AS hybrid_score
FROM hybrid_recall_debug_execute(
  'SELECT id, (content <&> ''database'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY content <&> ''database'' DESC LIMIT 100',
  'SELECT id, (embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8 FROM hybrid_recall_docs WHERE tenant_id = 1 ORDER BY embedding <=> ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
  1, 2, 2, 10, 100, 100, 60.0, 0.5, 0.5);

RESET enable_seqscan;
