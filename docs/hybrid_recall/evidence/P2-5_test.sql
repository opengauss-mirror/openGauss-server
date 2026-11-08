-- P2-5: serial/parallel HybridRecall path selection.
-- Prerequisite: S0 hybrid_recall_docs data/indexes and ogai marker function.
\set ON_ERROR_STOP on

SET enable_hybrid_recall = on;
\echo === P2-5 GUC DEFAULT ===
SHOW hybrid_recall_parallel_mode;

\echo === P2-5 FORCE SERIAL PLAN ===
SET hybrid_recall_parallel_mode = off;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

\pset format unaligned
\pset tuples_only on
\o /tmp/hybrid_recall_p25_serial.txt
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;
\o

\echo === P2-5 FORCE PARALLEL PLAN ===
SET hybrid_recall_parallel_mode = force;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

\o /tmp/hybrid_recall_p25_parallel.txt
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;
\o

\echo === P2-5 SERIAL/PARALLEL RESULT EQUALITY ===
\! diff -u /tmp/hybrid_recall_p25_serial.txt /tmp/hybrid_recall_p25_parallel.txt && echo P2_5_SERIAL_PARALLEL_MATCH=ok

\echo === P2-5 AUTO PLAN ===
SET hybrid_recall_parallel_mode = auto;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, 'database',
         '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
         '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 10;

RESET hybrid_recall_parallel_mode;
\pset format aligned
\pset tuples_only off
