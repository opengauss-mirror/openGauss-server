DROP AGGREGATE IF EXISTS pg_catalog.corr_s(float8, float8, text);
DROP AGGREGATE IF EXISTS pg_catalog.corr_s(float8, float8);
DROP AGGREGATE IF EXISTS pg_catalog.corr_k(float8, float8, text);
DROP AGGREGATE IF EXISTS pg_catalog.corr_k(float8, float8);
DROP FUNCTION IF EXISTS pg_catalog.corr_sk_trans_fn(internal, float8, float8, text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.corr_sk_trans_fn_no3(internal, float8, float8) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.corr_s_final_fn(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.corr_k_final_fn(internal) CASCADE;