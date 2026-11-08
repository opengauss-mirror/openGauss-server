-- P3-4: EXPLAIN, non-sensitive diagnostic logging and WLM instrumentation.
-- Prerequisite: P3-1 fixture in hybrid_recall_p31_20260826.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET search_path = ogai, public;
SET enable_hybrid_recall = on;
SET statement_timeout = '30s';

\echo === P3-4 DEFAULT LOG SWITCH: EXPECT OFF ===
SHOW hybrid_recall_log_summary;
SHOW use_workload_manager;

\echo === P3-4 SERIAL EXPLAIN ANALYZE WITH OPERATOR WLM ===
SET resource_track_level = operator;
SET hybrid_recall_parallel_mode = off;
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING ON)
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;

\echo === P3-4 PARALLEL JSON EXPLAIN: EXPECT TWO WORKERS AND POSITIVE OVERLAP ===
SET hybrid_recall_parallel_mode = force;
EXPLAIN (ANALYZE, VERBOSE, COSTS OFF, TIMING ON, FORMAT JSON)
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;

\echo === P3-4 OPT-IN SUMMARY LOG: SERVER LOG MUST NOT CONTAIN TOKEN ===
SET hybrid_recall_log_summary = on;
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3 /* p34_sensitive_sql_token */;
RESET hybrid_recall_log_summary;

RESET hybrid_recall_parallel_mode;
RESET resource_track_level;
RESET statement_timeout;
RESET enable_hybrid_recall;
RESET search_path;
