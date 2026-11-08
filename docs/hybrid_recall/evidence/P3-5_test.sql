-- P3-5: debug/cassert runtime stability, true ExecutorRewind rescan loop,
-- alternating parallel parameters and statement-timeout cleanup.
-- Prerequisite: P3-1 fixture in hybrid_recall_p31_20260826 and the test-only
-- hybrid_recall_p35_test.so deployed by p04_test_module/build.sh.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = notice;
SET search_path = ogai, public;
SET enable_hybrid_recall = on;
SET statement_timeout = '30s';

DROP FUNCTION IF EXISTS hybrid_recall_debug_rescan(
    text,text,int,int,int,int,int,int,float8,float8,float8,int);
CREATE FUNCTION hybrid_recall_debug_rescan(
    text,text,int,int,int,int,int,int,float8,float8,float8,int)
RETURNS BIGINT
AS 'hybrid_recall_p35_test.so', 'hybrid_recall_debug_rescan'
LANGUAGE C VOLATILE;

DROP FUNCTION IF EXISTS hybrid_recall_debug_parallel(
    text,text,int,int,int,int,int,int,float8,float8,float8,int);
CREATE FUNCTION hybrid_recall_debug_parallel(
    text,text,int,int,int,int,int,int,float8,float8,float8,int)
RETURNS TABLE(
    doc_id INTEGER,
    source_mask INTEGER,
    text_rank INTEGER,
    vector_rank INTEGER,
    hybrid_score FLOAT8)
AS 'hybrid_recall_p35_test.so', 'hybrid_recall_debug_parallel'
LANGUAGE C VOLATILE;

\echo === P3-5 TRUE EXECUTOR RESCAN: EXPECT 300 ROWS ===
SELECT hybrid_recall_debug_rescan(
    'SELECT id, (content <&> ''database'')::float8
       FROM p31_docs WHERE tenant_id=1
       ORDER BY content <&> ''database'' DESC LIMIT 100',
    'SELECT id, (ogai_embedding <=>
       ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8
       FROM p31_docs WHERE tenant_id=1
       ORDER BY ogai_embedding <=>
       ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
    1, 2, 2, 3, 100, 100, 60.0, 0.5, 0.5, 100) AS rescan_rows;

\echo === P3-5 100 PARALLEL EXECUTIONS WITH ALTERNATING OPTIONS ===
SET hybrid_recall_parallel_mode = force;
DO $p35$
DECLARE
    i INTEGER;
    first_id INTEGER;
    expected_id INTEGER;
    total_rows INTEGER := 0;
    text_only JSONB := $json${
      "text_candidates":100,
      "vector_candidates":100,
      "rrf_k":10,
      "weights":[1,0],
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],
      "filters":{"tenant_id":1}
    }$json$::JSONB;
    balanced JSONB := $json${
      "text_candidates":100,
      "vector_candidates":100,
      "rrf_k":60,
      "weights":[0.5,0.5],
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],
      "filters":{"tenant_id":1}
    }$json$::JSONB;
BEGIN
    FOR i IN 1..100 LOOP
        expected_id := CASE WHEN i % 2 = 1 THEN 5 ELSE 245 END;
        SELECT (result_record->>'id')::INTEGER
          INTO first_id
          FROM ogai.hybrid_search_v2(
              'p31_docs_task', 'database', 'id,title', 3, '',
              CASE WHEN i % 2 = 1 THEN text_only ELSE balanced END)
         LIMIT 1;
        IF first_id <> expected_id THEN
            RAISE EXCEPTION 'iteration % expected first id %, got %',
                i, expected_id, first_id;
        END IF;
        total_rows := total_rows + 3;
    END LOOP;
    RAISE NOTICE 'P35_PARALLEL_EXECUTIONS=100 TOTAL_ROWS=%', total_rows;
END
$p35$;

\echo === P3-5 STATEMENT TIMEOUT: EXPECT ERROR THEN LIVE QUERY ===
SET statement_timeout = '50ms';
\set ON_ERROR_STOP off
SELECT count(*)
FROM hybrid_recall_debug_parallel(
    'SELECT id, (content <&> ''database'')::float8
       FROM p31_docs WHERE tenant_id=1
       ORDER BY content <&> ''database'' DESC LIMIT 100',
    'SELECT id, (ogai_embedding <=>
       ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'')::float8
       FROM p31_docs WHERE tenant_id=1
       ORDER BY ogai_embedding <=>
       ''[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'' LIMIT 100',
    1, 2, 2, 3, 100, 100, 60.0, 0.5, 0.5, 2000);
\set ON_ERROR_STOP on
RESET statement_timeout;

SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::VECTOR,
    '{}'::JSONB) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC LIMIT 3;
SELECT 1 AS instance_alive_after_timeout;

DROP FUNCTION hybrid_recall_debug_rescan(
    text,text,int,int,int,int,int,int,float8,float8,float8,int);
DROP FUNCTION hybrid_recall_debug_parallel(
    text,text,int,int,int,int,int,int,float8,float8,float8,int);
RESET hybrid_recall_parallel_mode;
RESET enable_hybrid_recall;
RESET search_path;
