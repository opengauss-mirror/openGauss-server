-- P2-4 / P07: identical common-filter pushdown plus fusion-layer recheck.
-- Prerequisite: S0 hybrid_recall_docs data/indexes and ogai marker function.
\set ON_ERROR_STOP on
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = force;

\echo === P2-4 PLAN: HIDDEN FILTER PAYLOAD ON BOTH BRANCHES ===
SHOW enable_seqscan;

-- tenant_id/title are deliberately absent from SELECT.  EXPLAIN VERBOSE must
-- still show them in both branch outputs, with the same Filter on BM25 and ANN.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1 AND title LIKE 'doc-2%'
ORDER BY score DESC
LIMIT 5;

-- Capture native output and an independently expressed serial RRF baseline
-- in a byte-comparable form.  The runner performs a second external diff so
-- a mismatch cannot be hidden by gsql's shell-command exit handling.
\pset format unaligned
\pset tuples_only on
\o /tmp/hybrid_recall_p24_native.txt
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1 AND title LIKE 'doc-2%'
ORDER BY score DESC
LIMIT 5;
\o

SET enable_seqscan = off;
\o /tmp/hybrid_recall_p24_serial.txt
WITH text_path AS (
    SELECT id,
           row_number() OVER (ORDER BY content <&> 'database' DESC) AS text_rank
    FROM hybrid_recall_docs
    WHERE tenant_id = 1 AND title LIKE 'doc-2%'
    ORDER BY content <&> 'database' DESC
    LIMIT 100
),
vector_path AS (
    SELECT id,
           row_number() OVER (
             ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]') AS vector_rank
    FROM hybrid_recall_docs
    WHERE tenant_id = 1 AND title LIKE 'doc-2%'
    ORDER BY embedding <=> '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'
    LIMIT 100
),
fused AS (
    SELECT coalesce(t.id, v.id) AS id,
           (CASE WHEN t.id IS NOT NULL THEN 1 ELSE 0 END
              + CASE WHEN v.id IS NOT NULL THEN 2 ELSE 0 END) AS source_mask,
           coalesce(t.text_rank, 0) AS text_rank,
           coalesce(v.vector_rank, 0) AS vector_rank,
           (CASE WHEN t.id IS NOT NULL THEN 0.5 / (60 + t.text_rank) ELSE 0 END
              + CASE WHEN v.id IS NOT NULL THEN 0.5 / (60 + v.vector_rank) ELSE 0 END)::float8 AS score
    FROM text_path t
    FULL JOIN vector_path v ON v.id = t.id
)
SELECT id, score
FROM fused
ORDER BY score DESC,
         source_mask DESC,
         least(coalesce(nullif(text_rank, 0), 2147483647),
               coalesce(nullif(vector_rank, 0), 2147483647)),
         id
LIMIT 5;
\o
RESET enable_seqscan;

\echo === P2-4 NATIVE FILTERED TOP-5 ===
\! cat /tmp/hybrid_recall_p24_native.txt
\echo === P2-4 SERIAL FILTERED TOP-5 ===
\! cat /tmp/hybrid_recall_p24_serial.txt
\! diff -u /tmp/hybrid_recall_p24_serial.txt /tmp/hybrid_recall_p24_native.txt && echo P2_4_SERIAL_MATCH=ok
\pset format aligned
\pset tuples_only off

\echo === P2-4 CANDIDATE COUNT BELOW FINAL K ===
-- One qualifying row and zero qualifying rows must return their real counts;
-- neither may leak a row rejected by the public filter.
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1 AND title = 'doc-245'
ORDER BY score DESC
LIMIT 10;

SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 99
ORDER BY score DESC
LIMIT 10;

\echo === P2-4 CACHED PLAN RECHECK MAPPING ===
PREPARE hr_p24_hidden_filter AS
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1 AND title LIKE 'doc-2%'
ORDER BY score DESC
LIMIT 5;
EXECUTE hr_p24_hidden_filter;
EXECUTE hr_p24_hidden_filter;
DEALLOCATE PREPARE hr_p24_hidden_filter;

\echo === P2-4 UNSAFE FILTERS MUST NOT PARALLELIZE ===
-- EXPLAIN only: the reserved marker intentionally errors if an unsupported
-- ordinary plan is executed.  Each plan below must be Sort + SeqScan, never
-- Hybrid Recall or LOCAL GATHER.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id
FROM hybrid_recall_docs
WHERE tenant_id = floor(random() * 4)::int
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector, '{}'::jsonb) DESC
LIMIT 3;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id
FROM hybrid_recall_docs
WHERE tenant_id = (SELECT min(tenant_id) FROM hybrid_recall_docs)
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, 'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector, '{}'::jsonb) DESC
LIMIT 3;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT id
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY ogai.hybrid_rrf_marker(
  id, content, embedding, random()::text,
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector, '{}'::jsonb) DESC
LIMIT 3;

SHOW enable_seqscan;
RESET hybrid_recall_parallel_mode;
