--upgrade TABLE
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 3483, 3484, 0, 0;
CREATE TABLE IF NOT EXISTS pg_catalog.pg_proc_ext
(
 proc_oid oid not null,
 parallel_cursor_seq int2 not null,
 parallel_cursor_strategy int2 not null,
 parallel_cursor_partkey text[] not null
)WITH(oids=false) TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3488;
CREATE UNIQUE INDEX pg_proc_ext_proc_oid_index ON pg_catalog.pg_proc_ext USING BTREE(proc_oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
GRANT SELECT ON pg_catalog.pg_event_trigger TO PUBLIC;
