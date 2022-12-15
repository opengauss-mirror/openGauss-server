/* Add built-in function mot_jit_detail */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6205;
CREATE OR REPLACE FUNCTION pg_catalog.mot_jit_detail(
OUT proc_oid oid,
OUT query text,
OUT namespace text,
OUT jittable_status text,
OUT valid_status text,
OUT last_updated timestamptz,
OUT plan_type text,
OUT codegen_time int8,
OUT verify_time int8,
OUT finalize_time int8,
OUT compile_time int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL ROWS 1000 STABLE NOT FENCED NOT SHIPPABLE as 'mot_jit_detail';

/* Add built-in function mot_jit_profile */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6206;
CREATE OR REPLACE FUNCTION pg_catalog.mot_jit_profile(
OUT proc_oid oid,
OUT id int4,
OUT parent_id int4,
OUT query text,
OUT namespace text,
OUT weight float4,
OUT total int8,
OUT self int8,
OUT child_gross int8,
OUT child_net int8,
OUT def_vars int8,
OUT init_vars int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL ROWS 1000 STABLE NOT FENCED NOT SHIPPABLE as 'mot_jit_profile';

