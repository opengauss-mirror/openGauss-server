-- P3-3: configurable resource ceilings and execution-time revalidation.
-- Prerequisite: P3-1 fixture in hybrid_recall_p31_20260826.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET search_path = ogai, public;
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = off;

\echo === P3-3 DEFAULT RESOURCE GUCS ===
SHOW hybrid_recall_max_final_topk;
SHOW hybrid_recall_max_candidates_branch;
SHOW hybrid_recall_max_candidates_total;
SHOW hybrid_recall_rrf_k;
SHOW hybrid_recall_max_candidate_memory_kb;

\echo === P3-3 GUC DEFAULT RRF-K: EXPECT DIFFERENT FROM EXPLICIT 60 ===
SET hybrid_recall_rrf_k = 10;
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{"rrf_k":60}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;
RESET hybrid_recall_rrf_k;

\echo === P3-3 WRAPPER PREFLIGHT LIMITS: EXPECT THREE ERRORS ===
SET hybrid_recall_max_final_topk = 2;
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,title', 3, '',
    '{"query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_final_topk;

SET hybrid_recall_max_candidates_branch = 50;
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,title', 3, '',
    '{"text_candidates":51,"vector_candidates":50,
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_candidates_branch;

SET hybrid_recall_max_candidates_total = 150;
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,title', 3, '',
    '{"text_candidates":80,"vector_candidates":80,
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_candidates_total;

\echo === P3-3 DIRECT MARKER PLANNER LIMIT: EXPECT ERROR ===
SET hybrid_recall_max_final_topk = 2;
\set ON_ERROR_STOP off
EXPLAIN (COSTS OFF)
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;
\set ON_ERROR_STOP on
RESET hybrid_recall_max_final_topk;

\echo === P3-3 CACHED GENERIC PLAN REVALIDATION: EXPECT THREE ERRORS ===
SET plan_cache_mode = force_generic_plan;
PREPARE p33_cached(text, vector, bigint) AS
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, $1, $2,
    '{"text_candidates":100,"vector_candidates":100,"rrf_k":60}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT $3;
EXECUTE p33_cached(
    'database', '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR, 3::BIGINT);

SET hybrid_recall_max_final_topk = 2;
\set ON_ERROR_STOP off
EXECUTE p33_cached(
    'database', '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR, 3::BIGINT);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_final_topk;

SET hybrid_recall_max_candidates_branch = 99;
\set ON_ERROR_STOP off
EXECUTE p33_cached(
    'database', '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR, 3::BIGINT);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_candidates_branch;

SET hybrid_recall_max_candidates_total = 199;
\set ON_ERROR_STOP off
EXECUTE p33_cached(
    'database', '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR, 3::BIGINT);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_candidates_total;
DEALLOCATE PREPARE p33_cached;
RESET plan_cache_mode;

\echo === P3-3 CANDIDATE MEMORY LIMIT: EXPECT ERROR AND LIVE INSTANCE ===
ALTER TABLE public.p31_docs ADD COLUMN p33_pad TEXT;
UPDATE public.p31_docs SET p33_pad = repeat('x', 2048);
ANALYZE public.p31_docs;
SET hybrid_recall_max_candidate_memory_kb = 64;
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,p33_pad', 10, '',
    '{"text_candidates":500,"vector_candidates":500,
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);
\set ON_ERROR_STOP on
RESET hybrid_recall_max_candidate_memory_kb;
ALTER TABLE public.p31_docs DROP COLUMN p33_pad;
SELECT 1 AS instance_alive_after_memory_error;

RESET hybrid_recall_parallel_mode;
RESET enable_hybrid_recall;
RESET search_path;
