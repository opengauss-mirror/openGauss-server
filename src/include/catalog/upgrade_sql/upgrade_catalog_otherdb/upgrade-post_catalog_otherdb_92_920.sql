DROP VIEW IF EXISTS pg_catalog.pg_publication_tables;
CREATE VIEW pg_catalog.pg_publication_tables AS
    SELECT
        gpt.pubname AS pubname,
        N.nspname AS schemaname,
        C.relname AS tablename
    FROM (SELECT
         P.pubname,
         pg_catalog.pg_get_publication_tables(P.pubname) relid
         FROM pg_publication P) gpt,
         pg_class C
         JOIN pg_namespace N ON (N.oid = C.relnamespace)
    WHERE C.oid = gpt.relid;

-- create query_node_reform_info_from_dms
DROP FUNCTION IF EXISTS pg_catalog.query_node_reform_info_from_dms() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2869;
CREATE FUNCTION pg_catalog.query_node_reform_info_from_dms
(
    int4,
    out name text,
    out description text
)
RETURNS SETOF record LANGUAGE INTERNAL as 'query_node_reform_info_from_dms';

-- create query_all_drc_info
DROP FUNCTION IF EXISTS pg_catalog.query_all_drc_info() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2870;
CREATE FUNCTION pg_catalog.query_all_drc_info
(
    int4,
    out RESOURCE_ID text,
    out MASTER_ID int4,
    out COPY_INSTS int8,
    out CLAIMED_OWNER int4,
    out LOCK_MODE int4,
    out LAST_EDP int4,
    out TYPE int4,
    out IN_RECOVERY char,
    out COPY_PROMOTE int4,
    out PART_ID int4,
    out EDP_MAP int8,
    out LSN int8,
    out LEN int4,
    out RECOVERY_SKIP int4,
    out RECYCLING char,
    out CONVERTING_INST_ID int4,
    out CONVERTING_CURR_MODE int4,
    out CONVERTING_REQ_MODE int4
)
RETURNS SETOF record LANGUAGE INTERNAL as 'query_all_drc_info';