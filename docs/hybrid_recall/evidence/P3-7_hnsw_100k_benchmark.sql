-- P3-7 HNSW-only 100k benchmark.
-- Warm both strategies, then alternate serial/parallel execution in one
-- session so cache temperature and transient host load affect both fairly.
-- Sixty measured pairs make P95 less sensitive to a single WSL scheduling
-- outlier than the earlier 30-pair calibration run.
\set ON_ERROR_STOP on
\timing off

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

CREATE TEMP TABLE p37_hnsw_samples (
    run_no integer NOT NULL,
    mode text NOT NULL,
    elapsed_ms numeric NOT NULL,
    result_ids text NOT NULL
);

DO $$
DECLARE
    i integer;
    started_at timestamptz;
    ids text;
    elapsed numeric;
    query_sql text := $query$
        SELECT string_agg(result_record::text, '|' ORDER BY result_record::text)
          FROM ogai.hybrid_search_v2(
              'p37_hnsw_100k_task',
              'record',
              'id,title',
              10,
              '',
              '{"text_candidates":1000,"vector_candidates":1000,
                "rrf_k":60,"weights":[0.5,0.5],
                "query_vector":[0.01,0.20,0.35,0.40,0.50,0.50,0.50,0.50],
                "filters":{"tenant_id":0}}'::jsonb)
    $query$;
BEGIN
    -- Five warm-up pairs are excluded from the statistics.
    FOR i IN 1..5 LOOP
        PERFORM set_config('hybrid_recall_parallel_mode', 'off', false);
        EXECUTE query_sql INTO ids;
        PERFORM set_config('hybrid_recall_parallel_mode', 'force', false);
        EXECUTE query_sql INTO ids;
    END LOOP;

    -- Alternate order per round to remove a fixed first/second advantage.
    FOR i IN 1..60 LOOP
        IF i % 2 = 1 THEN
            PERFORM set_config('hybrid_recall_parallel_mode', 'off', false);
            started_at := clock_timestamp();
            EXECUTE query_sql INTO ids;
            elapsed := extract(epoch FROM clock_timestamp() - started_at) * 1000;
            INSERT INTO p37_hnsw_samples VALUES (i, 'serial', elapsed, ids);

            PERFORM set_config('hybrid_recall_parallel_mode', 'force', false);
            started_at := clock_timestamp();
            EXECUTE query_sql INTO ids;
            elapsed := extract(epoch FROM clock_timestamp() - started_at) * 1000;
            INSERT INTO p37_hnsw_samples VALUES (i, 'parallel', elapsed, ids);
        ELSE
            PERFORM set_config('hybrid_recall_parallel_mode', 'force', false);
            started_at := clock_timestamp();
            EXECUTE query_sql INTO ids;
            elapsed := extract(epoch FROM clock_timestamp() - started_at) * 1000;
            INSERT INTO p37_hnsw_samples VALUES (i, 'parallel', elapsed, ids);

            PERFORM set_config('hybrid_recall_parallel_mode', 'off', false);
            started_at := clock_timestamp();
            EXECUTE query_sql INTO ids;
            elapsed := extract(epoch FROM clock_timestamp() - started_at) * 1000;
            INSERT INTO p37_hnsw_samples VALUES (i, 'serial', elapsed, ids);
        END IF;
    END LOOP;
END
$$;

\echo === P37_HNSW_100K_RESULT_EQUALITY ===
SELECT count(*) AS samples,
       count(DISTINCT result_ids) AS distinct_result_sets,
       min(result_ids) AS result_ids
  FROM p37_hnsw_samples;

\echo === P37_HNSW_100K_LATENCY_MS ===
SELECT mode,
       count(*) AS runs,
       round(avg(elapsed_ms), 3) AS mean_ms,
       round(min(elapsed_ms), 3) AS min_ms,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ms)::numeric, 3) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms)::numeric, 3) AS p95_ms,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ms)::numeric, 3) AS p99_ms,
       round(max(elapsed_ms), 3) AS max_ms
  FROM p37_hnsw_samples
 GROUP BY mode
 ORDER BY mode;

\echo === P37_HNSW_100K_IMPROVEMENT ===
WITH stats AS (
    SELECT mode,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ms) AS p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms) AS p95,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ms) AS p99
      FROM p37_hnsw_samples
     GROUP BY mode
), pivoted AS (
    SELECT max(CASE WHEN mode = 'serial' THEN p50 END) AS serial_p50,
           max(CASE WHEN mode = 'parallel' THEN p50 END) AS parallel_p50,
           max(CASE WHEN mode = 'serial' THEN p95 END) AS serial_p95,
           max(CASE WHEN mode = 'parallel' THEN p95 END) AS parallel_p95,
           max(CASE WHEN mode = 'serial' THEN p99 END) AS serial_p99,
           max(CASE WHEN mode = 'parallel' THEN p99 END) AS parallel_p99
      FROM stats
)
SELECT round((1 - parallel_p50 / serial_p50) * 100, 2) AS p50_reduction_pct,
       round((1 - parallel_p95 / serial_p95) * 100, 2) AS p95_reduction_pct,
       round((1 - parallel_p99 / serial_p99) * 100, 2) AS p99_reduction_pct
  FROM pivoted;

\echo === P37_HNSW_100K_PAIRED_ROUNDS ===
WITH pairs AS (
    SELECT run_no,
           max(CASE WHEN mode = 'serial' THEN elapsed_ms END) AS serial_ms,
           max(CASE WHEN mode = 'parallel' THEN elapsed_ms END) AS parallel_ms
      FROM p37_hnsw_samples
     GROUP BY run_no
)
SELECT run_no,
       round(serial_ms, 3) AS serial_ms,
       round(parallel_ms, 3) AS parallel_ms,
       round((1 - parallel_ms / serial_ms) * 100, 2) AS reduction_pct
  FROM pairs
 ORDER BY run_no;

\echo === P37_HNSW_100K_PAIRED_SUMMARY ===
WITH pairs AS (
    SELECT run_no,
           max(CASE WHEN mode = 'serial' THEN elapsed_ms END) AS serial_ms,
           max(CASE WHEN mode = 'parallel' THEN elapsed_ms END) AS parallel_ms
      FROM p37_hnsw_samples
     GROUP BY run_no
), reductions AS (
    SELECT serial_ms,
           parallel_ms,
           (1 - parallel_ms / serial_ms) * 100 AS reduction_pct
      FROM pairs
)
SELECT count(*) AS pairs,
       count(*) FILTER (WHERE parallel_ms < serial_ms) AS parallel_wins,
       round(avg(serial_ms - parallel_ms), 3) AS mean_saved_ms,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY reduction_pct)::numeric, 2)
           AS paired_median_reduction_pct,
       round(percentile_cont(0.05) WITHIN GROUP (ORDER BY reduction_pct)::numeric, 2)
           AS paired_p05_reduction_pct
  FROM reductions;
