-- P2-2/P2-3: planner-built BM25 + ANN branches and payload contract.
-- Prerequisite: S0 hybrid_recall_docs data/indexes and ogai marker function.
\set ON_ERROR_STOP on

SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = force;
SHOW enable_seqscan;

-- Plan contract: Hybrid Recall, two LOCAL GATHER task streams, branch Limit,
-- BM25 Index Scan and Ann Index Scan.  The caller's enable_seqscan remains on.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id, tenant_id, title, content,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 3;

-- Native planner path must match the S0 hand-written RRF golden Top-10
-- recorded in BASELINE.md/evidence.  Keep this as a top-level query: V1
-- deliberately rejects injection below CTE/subquery/window upper plans.
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

-- Multi-payload, duplicate/reordered by-reference payload, and an unrelated
-- float8 zero.  Both content columns must be nonempty/equal; zero must remain
-- zero while score is the fused RRF value.
SELECT content AS content_first, id, content AS content_again, 0::float8 AS zero,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 3;

-- The doc key is still carried internally when it is not in the SELECT list.
SELECT content,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 3;

-- V1 negative: a marker below an upper query level must not inject task
-- streams (design test P06).  EXPLAIN shows the ordinary subquery plan.
EXPLAIN (COSTS OFF)
SELECT count(*) AS nested_rows
FROM (
  SELECT id, content,
         ogai.hybrid_rrf_marker(
           id, content, embedding, 'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{}'::jsonb) AS score
  FROM hybrid_recall_docs
  WHERE tenant_id = 1
  ORDER BY score DESC
  LIMIT 10
) s;

-- Cached plan executes twice: ExecInit must rebuild positional mapping from
-- immutable original-column metadata on every execution.
PREPARE hr_p23_payload AS
SELECT id, tenant_id, title, content,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 3;
EXECUTE hr_p23_payload;
EXECUTE hr_p23_payload;
DEALLOCATE PREPARE hr_p23_payload;

-- Whitelist negatives: ASC, a second sort key, missing/NULL LIMIT, and OFFSET
-- must not acquire HybridRecall semantics.  EXPLAIN should show ordinary
-- Sort/SeqScan.
EXPLAIN (COSTS OFF)
SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  '{}'::jsonb) ASC
LIMIT 3;

EXPLAIN (COSTS OFF)
SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  '{}'::jsonb) DESC, id
LIMIT 3;

EXPLAIN (COSTS OFF)
SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  '{}'::jsonb) DESC;

EXPLAIN (COSTS OFF)
SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  '{}'::jsonb) DESC
LIMIT NULL;

EXPLAIN (COSTS OFF)
SELECT id FROM hybrid_recall_docs WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  '{}'::jsonb) DESC
LIMIT 3 OFFSET 1;

SHOW enable_seqscan;
RESET hybrid_recall_parallel_mode;
