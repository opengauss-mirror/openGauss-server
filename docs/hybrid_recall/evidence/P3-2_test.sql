-- P3-2: feature switch, legacy fallback and old JSONB contract compatibility.
-- Prerequisite: P3-1 fixture in hybrid_recall_p31_20260826.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET search_path = ogai, public;
SET ogai.hybrid_search_ratio = 0.5;

\echo === P3-2 DEFAULT FEATURE SWITCH: EXPECT off ===
SHOW enable_hybrid_recall;

\echo === P3-2 SWITCH OFF DIRECT MARKER: EXPECT NO HYBRID RECALL PATH ===
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;

\echo === P3-2 OLD API VS V2 LEGACY FALLBACK: EXPECT IDENTICAL JSONB ===
SET enable_hybrid_recall = off;
CREATE TEMP TABLE p32_old(seq BIGINT, result_record JSONB);
CREATE TEMP TABLE p32_fallback(seq BIGINT, result_record JSONB);

INSERT INTO p32_old(seq, result_record)
SELECT row_number() OVER (), result_record
FROM ogai.hybrid_search_legacy_core(
        'p31_docs_task', 'database', 'id,title', 3, 'tenant_id = 1',
        '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR);

INSERT INTO p32_fallback(seq, result_record)
SELECT row_number() OVER (), result_record
FROM ogai.hybrid_search_v2(
        'p31_docs_task', 'database', 'id,title', 3, '',
        '{"on_unsupported":"legacy","filters":{"tenant_id":1},
          "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);

SELECT count(*) AS legacy_contract_mismatch_count
FROM (
    SELECT COALESCE(o.seq, f.seq) AS seq,
           o.result_record AS old_row,
           f.result_record AS fallback_row
    FROM p32_old o
    FULL OUTER JOIN p32_fallback f ON f.seq = o.seq
) s
WHERE old_row IS DISTINCT FROM fallback_row;
SELECT * FROM p32_fallback ORDER BY seq;

\echo === P3-2 MISSING ANN INDEX: EXPECT LEGACY FALLBACK AND ZERO MISMATCH ===
SET enable_hybrid_recall = on;
DROP INDEX public.p31_hnsw_idx;
TRUNCATE p32_fallback;
INSERT INTO p32_fallback(seq, result_record)
SELECT row_number() OVER (), result_record
FROM ogai.hybrid_search_v2(
        'p31_docs_task', 'database', 'id,title', 3, '',
        '{"on_unsupported":"legacy","filters":{"tenant_id":1},
          "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::JSONB);
SELECT count(*) AS missing_index_fallback_mismatch_count
FROM (
    SELECT COALESCE(o.seq, f.seq) AS seq,
           o.result_record AS old_row,
           f.result_record AS fallback_row
    FROM p32_old o
    FULL OUTER JOIN p32_fallback f ON f.seq = o.seq
) s
WHERE old_row IS DISTINCT FROM fallback_row;
CREATE INDEX p31_hnsw_idx
ON public.p31_docs USING hnsw (ogai_embedding vector_cosine_ops);
ANALYZE public.p31_docs;

\echo === P3-2 SWITCH ON: EXPECT HYBRID RECALL PLAN ===
EXPLAIN (VERBOSE, COSTS OFF)
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;

\echo === P3-2 CACHED PLAN AFTER SWITCH OFF: EXPECT SAFE REJECTION ===
SET plan_cache_mode = force_generic_plan;
PREPARE p32_cached(text, vector) AS
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, $1, $2, '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;
EXECUTE p32_cached(
    'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR);
SET enable_hybrid_recall = off;
\set ON_ERROR_STOP off
EXECUTE p32_cached(
    'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR);
\set ON_ERROR_STOP on
DEALLOCATE PREPARE p32_cached;
RESET plan_cache_mode;

\echo === P3-2 SWITCH OFF + ERROR POLICY: EXPECT EXPLICIT ERROR ===
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,title', 3, '',
    '{"on_unsupported":"error","filters":{"tenant_id":1}}'::JSONB);
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,title', 3, '',
    '{"on_unsupported":"invalid","filters":{"tenant_id":1}}'::JSONB);
\set ON_ERROR_STOP on

\echo === P3-2 LEGACY CORE UNAFFECTED BY SWITCH: EXPECT ZERO MISMATCH ===
TRUNCATE p32_fallback;
SET enable_hybrid_recall = on;
INSERT INTO p32_fallback(seq, result_record)
SELECT row_number() OVER (), result_record
FROM ogai.hybrid_search_legacy_core(
        'p31_docs_task', 'database', 'id,title', 3, 'tenant_id = 1',
        '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR);
SELECT count(*) AS old_api_switch_mismatch_count
FROM (
    SELECT COALESCE(o.seq, f.seq) AS seq,
           o.result_record AS off_row,
           f.result_record AS on_row
    FROM p32_old o
    FULL OUTER JOIN p32_fallback f ON f.seq = o.seq
) s
WHERE off_row IS DISTINCT FROM on_row;

-- The original five-argument public entry point remains present and is now a
-- thin wrapper over the byte-compatible legacy core.
SELECT count(*) AS old_public_signature_count
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'ogai' AND p.proname = 'hybrid_search'
  AND p.pronargs = 5;

RESET enable_hybrid_recall;
RESET enable_seqscan;
RESET enable_indexscan;
RESET search_path;
