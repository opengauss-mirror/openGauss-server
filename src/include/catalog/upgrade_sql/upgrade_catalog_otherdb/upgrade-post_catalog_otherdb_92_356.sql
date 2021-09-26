/* Add built-in function mot_session_memory_detail */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6200;
CREATE OR REPLACE FUNCTION pg_catalog.mot_session_memory_detail(
OUT sessid text,
OUT total_size int8,
OUT free_size int8,
OUT used_size int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL ROWS 100 STABLE NOT FENCED NOT SHIPPABLE as 'mot_session_memory_detail';

/* Add built-in function mot_global_memory_detail */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6201;
CREATE OR REPLACE FUNCTION pg_catalog.mot_global_memory_detail(
OUT numa_node int4,
OUT reserved_size int8,
OUT used_size int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL ROWS 100 STABLE NOT FENCED NOT SHIPPABLE as 'mot_global_memory_detail';

/* Add built-in function mot_local_memory_detail */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6202;
CREATE OR REPLACE FUNCTION pg_catalog.mot_local_memory_detail(
OUT numa_node int4,
OUT reserved_size int8,
OUT used_size int8)
RETURNS SETOF RECORD LANGUAGE INTERNAL ROWS 100 STABLE NOT FENCED NOT SHIPPABLE as 'mot_local_memory_detail';

