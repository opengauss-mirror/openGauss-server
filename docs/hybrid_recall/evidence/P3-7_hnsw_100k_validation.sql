-- P3-7 HNSW-only 100k validation for the final Release binary.
-- Covers result equivalence, an empty text branch, the representative plan,
-- and statement-timeout cleanup after both local Stream workers have started.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET search_path = ogai, public;
SET enable_hybrid_recall = on;
SET hnsw_ef_search = 1000;

DELETE FROM ogai.vectorize_tasks WHERE task_name = 'p37_hnsw_100k_task';
INSERT INTO ogai.vectorize_tasks(
    task_name, type, index_type, model_key, src_schema, src_table,
    src_col, primary_key, method, dim, enable_bm25, owner_name)
VALUES (
    'p37_hnsw_100k_task', 'sync', 'hnsw', 'p37_stub', 'public',
    'p37_hnsw_100k', 'content', 'id', 'append', 8, true, CURRENT_USER);

\echo === P37_FINAL_RELEASE_SERIAL_PARALLEL_EQUALITY ===
SET hybrid_recall_parallel_mode = off;
CREATE TEMP TABLE p37_serial AS
SELECT row_number() OVER () AS position, result_record::text AS result_record
  FROM ogai.hybrid_search_v2(
      'p37_hnsw_100k_task', 'record', 'id,title', 10, '',
      '{"text_candidates":1000,"vector_candidates":1000,
        "rrf_k":60,"weights":[0.5,0.5],
        "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
        "filters":{"tenant_id":0}}'::jsonb);

SET hybrid_recall_parallel_mode = force;
CREATE TEMP TABLE p37_parallel AS
SELECT row_number() OVER () AS position, result_record::text AS result_record
  FROM ogai.hybrid_search_v2(
      'p37_hnsw_100k_task', 'record', 'id,title', 10, '',
      '{"text_candidates":1000,"vector_candidates":1000,
        "rrf_k":60,"weights":[0.5,0.5],
        "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
        "filters":{"tenant_id":0}}'::jsonb);

SELECT (SELECT count(*) FROM p37_serial) AS serial_rows,
       (SELECT count(*) FROM p37_parallel) AS parallel_rows,
       count(*) AS mismatched_rows
  FROM p37_serial s
  FULL JOIN p37_parallel p USING (position)
 WHERE coalesce(s.result_record, '') <> coalesce(p.result_record, '');

\echo === P37_EMPTY_TEXT_BRANCH_EQUALITY ===
SET hybrid_recall_parallel_mode = off;
CREATE TEMP TABLE p37_empty_text_serial AS
SELECT row_number() OVER () AS position, result_record::text AS result_record
  FROM ogai.hybrid_search_v2(
      'p37_hnsw_100k_task', 'p37_token_that_does_not_exist',
      'id,title', 10, '',
      '{"text_candidates":100,"vector_candidates":100,
        "rrf_k":60,"weights":[0.5,0.5],
        "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
        "filters":{"tenant_id":0}}'::jsonb);

SET hybrid_recall_parallel_mode = force;
CREATE TEMP TABLE p37_empty_text_parallel AS
SELECT row_number() OVER () AS position, result_record::text AS result_record
  FROM ogai.hybrid_search_v2(
      'p37_hnsw_100k_task', 'p37_token_that_does_not_exist',
      'id,title', 10, '',
      '{"text_candidates":100,"vector_candidates":100,
        "rrf_k":60,"weights":[0.5,0.5],
        "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
        "filters":{"tenant_id":0}}'::jsonb);

SELECT (SELECT count(*) FROM p37_empty_text_serial) AS serial_rows,
       (SELECT count(*) FROM p37_empty_text_parallel) AS parallel_rows,
       count(*) AS mismatched_rows
  FROM p37_empty_text_serial s
  FULL JOIN p37_empty_text_parallel p USING (position)
 WHERE coalesce(s.result_record, '') <> coalesce(p.result_record, '');

\echo === P37_REPRESENTATIVE_PARALLEL_PLAN ===
EXPLAIN (ANALYZE, COSTS OFF, TIMING ON)
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, ogai_embedding, 'record',
           '[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50]'::vector,
           '{"text_candidates":1000,"vector_candidates":1000,
             "rrf_k":60,"weights":[0.5,0.5]}'::jsonb) AS score
  FROM public.p37_hnsw_100k
 WHERE tenant_id = 0
 ORDER BY score DESC
 LIMIT 10;

\echo === P37_STATEMENT_TIMEOUT_THEN_LIVE_QUERY ===
SET statement_timeout = '50ms';
\set ON_ERROR_STOP off
SELECT count(*)
  FROM ogai.hybrid_search_v2(
      'p37_hnsw_100k_task', 'record', 'id,title', 10, '',
      '{"text_candidates":1000,"vector_candidates":1000,
        "rrf_k":60,"weights":[0.5,0.5],
        "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
        "filters":{"tenant_id":0}}'::jsonb);
\set ON_ERROR_STOP on
RESET statement_timeout;
SELECT 1 AS instance_alive_after_timeout;

RESET hybrid_recall_parallel_mode;
RESET hnsw_ef_search;
RESET enable_hybrid_recall;
RESET search_path;
