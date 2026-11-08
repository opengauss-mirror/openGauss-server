-- P3-1: OGAI hybrid_search_v2 API, options, ownership and injection safety.
-- Run in the dedicated hybrid_recall_p31_20260826 database after installing
-- the current contrib/ogai extension files.
\set ON_ERROR_STOP on
\timing on

SET client_min_messages = warning;
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = force;
SET search_path = ogai, public;

DROP ROLE IF EXISTS p31_intruder_20260826;
DROP TABLE IF EXISTS public.p31_docs CASCADE;
CREATE TABLE public.p31_docs (
    id integer PRIMARY KEY,
    tenant_id integer NOT NULL,
    title text NOT NULL,
    content text NOT NULL,
    ogai_embedding vector(8) NOT NULL
);

INSERT INTO public.p31_docs
SELECT i,
       i % 4,
       'doc-' || lpad(i::text, 3, '0'),
       (ARRAY['database','kernel','vector','memory','network'])[1 + i % 5]
         || ' ' || repeat((ARRAY['alpha','beta','gamma','delta','epsilon','zeta','eta'])[1 + i % 7], 1 + i % 3)
         || ' record ' || i,
       ('['  || round((( i       % 11)::numeric / 11), 4)
         || ',' || round((( i *  2  % 13)::numeric / 13), 4)
         || ',' || round((( i *  3  % 17)::numeric / 17), 4)
         || ',' || round((( i *  5  % 19)::numeric / 19), 4)
         || ',' || round((( i *  7  % 23)::numeric / 23), 4)
         || ',' || round((( i * 11  % 29)::numeric / 29), 4)
         || ',' || round((( i * 13  % 31)::numeric / 31), 4)
         || ',' || round((( i * 17  % 37)::numeric / 37), 4)
       || ']')::vector
FROM generate_series(1, 500) AS i;

CREATE INDEX p31_bm25_idx ON public.p31_docs USING bm25 (content);
CREATE INDEX p31_hnsw_idx ON public.p31_docs USING hnsw (ogai_embedding vector_cosine_ops);
ANALYZE public.p31_docs;

DELETE FROM ogai.vectorize_tasks WHERE task_name = 'p31_docs_task';
INSERT INTO ogai.vectorize_tasks(
    task_name, type, index_type, model_key, src_schema, src_table,
    src_col, primary_key, method, dim, enable_bm25, owner_name)
VALUES (
    'p31_docs_task', 'sync', 'hnsw', 'p31_stub', 'public', 'p31_docs',
    'content', 'id', 'append', 8, true, CURRENT_USER);

\echo === P3-1 OPTIONS: WEIGHTS=[1,0], FILTER tenant_id=1 ===
SELECT result_record
FROM ogai.hybrid_search_v2(
    'p31_docs_task',
    'database',
    'id,title',
    3,
    '',
    '{
      "strategy":"parallel_rrf",
      "text_candidates":100,
      "vector_candidates":100,
      "rrf_k":10,
      "weights":[1,0],
      "on_worker_shortage":"serial",
      "query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],
      "filters":{"tenant_id":1}
    }'::jsonb);

\echo === P3-1 DIRECT MARKER OPTIONS: EXPECT 5,25,45 ===
SELECT id,
       ogai.hybrid_rrf_marker(
           id, content, ogai_embedding,
           'database',
           '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
           '{"text_candidates":100,"vector_candidates":100,
             "rrf_k":10,"weights":[1,0]}'::jsonb) AS score
FROM public.p31_docs
WHERE tenant_id = 1
ORDER BY score DESC
LIMIT 3;

\echo === P3-1 OWNERSHIP/PERMISSION TESTS: EXPECT TWO ERRORS ===
CREATE ROLE p31_intruder_20260826 IDENTIFIED BY 'P31_Test_only#2026';
\set ON_ERROR_STOP off
SET ROLE p31_intruder_20260826 PASSWORD 'P31_Test_only#2026';
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 3, '',
    '{"query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::jsonb);
RESET ROLE;
\set ON_ERROR_STOP on

UPDATE ogai.vectorize_tasks
SET owner_name = 'p31_intruder_20260826'
WHERE task_name = 'p31_docs_task';
\set ON_ERROR_STOP off
SET ROLE p31_intruder_20260826 PASSWORD 'P31_Test_only#2026';
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 3, '',
    '{"query_vector":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]}'::jsonb);
RESET ROLE;
\set ON_ERROR_STOP on
UPDATE ogai.vectorize_tasks
SET owner_name = CURRENT_USER
WHERE task_name = 'p31_docs_task';
DROP ROLE p31_intruder_20260826;

\echo === P3-1 NEGATIVE API/INJECTION TESTS: EXPECT SEVEN ERRORS ===
\set ON_ERROR_STOP off
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 0, '', '{}'::jsonb);
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 3,
    '1=1; DROP TABLE public.p31_docs; --', '{}'::jsonb);
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id,(SELECT current_user)', 3, '', '{}'::jsonb);
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 3, '',
    '{"filters":{"tenant_id) = 1; DROP TABLE public.p31_docs; --":1}}'::jsonb);
SELECT * FROM ogai.hybrid_search_v2(
    'p31_docs_task', 'database', 'id', 3, '',
    '{"unknown_option":1}'::jsonb);
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
    '{"text_candidates":2,"vector_candidates":100}'::jsonb) AS score
FROM public.p31_docs
ORDER BY score DESC LIMIT 3;
SELECT id, ogai.hybrid_rrf_marker(
    id, content, ogai_embedding, 'database',
    '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
    '{"text_candidates":1e100,"vector_candidates":100}'::jsonb) AS score
FROM public.p31_docs
ORDER BY score DESC LIMIT 3;
\set ON_ERROR_STOP on

SELECT count(*) AS injection_target_still_exists
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relname = 'p31_docs';

RESET search_path;
RESET hybrid_recall_parallel_mode;
RESET enable_hybrid_recall;
