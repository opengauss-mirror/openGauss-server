SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5563;
CREATE OR REPLACE FUNCTION pg_catalog.corr_s_final_fn(internal)
RETURNS float8 LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE AS 'corr_s_final_fn';
COMMENT ON FUNCTION pg_catalog.corr_s_final_fn(internal) IS 'Final function for corr_s to return double precision';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5564;
CREATE OR REPLACE FUNCTION pg_catalog.corr_k_final_fn(internal)
RETURNS float8 LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE AS 'corr_k_final_fn';
COMMENT ON FUNCTION pg_catalog.corr_k_final_fn(internal) IS 'Final function for corr_k to return double precision';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5565;
CREATE OR REPLACE FUNCTION pg_catalog.corr_sk_trans_fn(internal, float8, float8, text)
RETURNS internal LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE AS 'corr_sk_trans_fn';
COMMENT ON FUNCTION pg_catalog.corr_sk_trans_fn(internal, float8, float8, text) IS 'Transition function for corr_s/corr_k with 3rd arg';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5568;
CREATE OR REPLACE FUNCTION pg_catalog.corr_sk_trans_fn_no3(internal, float8, float8)
RETURNS internal LANGUAGE INTERNAL IMMUTABLE NOT SHIPPABLE AS 'corr_sk_trans_fn_no3';
COMMENT ON FUNCTION pg_catalog.corr_sk_trans_fn_no3(internal, float8, float8) IS 'Transition function for corr_s/corr_k without 3rd arg';

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5561;
CREATE AGGREGATE pg_catalog.corr_s(float8, float8, text) (SFUNC=corr_sk_trans_fn, STYPE= internal, finalfunc = corr_s_final_fn);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5566;
CREATE AGGREGATE pg_catalog.corr_s(float8, float8) (SFUNC=corr_sk_trans_fn_no3, STYPE= internal, finalfunc = corr_s_final_fn);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5562;
CREATE AGGREGATE pg_catalog.corr_k(float8, float8, text) (SFUNC=corr_sk_trans_fn, STYPE= internal, finalfunc = corr_k_final_fn);

SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5567;
CREATE AGGREGATE pg_catalog.corr_k(float8, float8) (SFUNC=corr_sk_trans_fn_no3, STYPE= internal, finalfunc = corr_k_final_fn);