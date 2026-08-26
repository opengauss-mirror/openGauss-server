-- P2-7: prepared statement parameters, custom plans and generic plans.
-- All four external Params (query text/vector, public filter and final K)
-- must remain live through both nested index branches and HybridRecall.
\set ON_ERROR_STOP on
\timing on

SET statement_timeout = '30s';
SET enable_hybrid_recall = on;
SET hybrid_recall_parallel_mode = force;

PREPARE hr_p27(text, vector, integer, bigint) AS
SELECT id,
       ogai.hybrid_rrf_marker(
         id, content, embedding, $1, $2, '{}'::jsonb) AS score
FROM hybrid_recall_docs
WHERE tenant_id = $3
ORDER BY score DESC
LIMIT $4;

\echo === P2-7 FORCE CUSTOM PLAN ===
SET plan_cache_mode = force_custom_plan;
EXPLAIN (VERBOSE, COSTS OFF)
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 3::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 3::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 1::bigint);
EXECUTE hr_p27(
  'alpha',
  '[0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1]'::vector,
  2, 3::bigint);

\echo === P2-7 FORCE GENERIC PLAN ===
SET plan_cache_mode = force_generic_plan;
EXPLAIN (VERBOSE, COSTS OFF)
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 3::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 3::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 1::bigint);
EXECUTE hr_p27(
  'alpha',
  '[0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1]'::vector,
  2, 3::bigint);

\echo === P2-7 INVALID RUNTIME LIMITS: EXPECT THREE ERRORS ===
\set ON_ERROR_STOP off
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 0::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, 101::bigint);
EXECUTE hr_p27(
  'database',
  '[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]'::vector,
  1, NULL::bigint);
\set ON_ERROR_STOP on

DEALLOCATE PREPARE hr_p27;
RESET plan_cache_mode;
RESET hybrid_recall_parallel_mode;
