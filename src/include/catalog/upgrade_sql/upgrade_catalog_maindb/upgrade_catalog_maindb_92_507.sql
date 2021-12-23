-- --------------------------------------------------------------
-- upgrade pg_catalog.pg_buffercache_pages
-- --------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.pg_buffercache_pages() CASCADE;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4130;
CREATE OR REPLACE FUNCTION pg_catalog.pg_buffercache_pages
(out bufferid pg_catalog.int4,
out relfilenode pg_catalog.oid,
out bucketid pg_catalog.int4,
out storage_type pg_catalog.int2,
out reltablespace pg_catalog.oid,
out reldatabase pg_catalog.oid,
out relforknumber pg_catalog.int2,
out relblocknumber pg_catalog.int8,
out isdirty pg_catalog.bool,
out isvalid pg_catalog.bool,
out usage_count pg_catalog.int2,
out pinning_backends pg_catalog.int4)
RETURNS SETOF record LANGUAGE INTERNAL STABLE STRICT as 'pg_buffercache_pages';DROP TYPE IF EXISTS pg_catalog.gs_job_attribute;
DROP TABLE IF EXISTS pg_catalog.gs_job_attribute;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG,false,true,9031,9031,0,0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_job_attribute
(
    job_name            text NOCOMPRESS,
    attribute_name      text NOCOMPRESS,
    attribute_value     text NOCOMPRESS
) WITH OIDS TABLESPACE pg_default;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4457;
CREATE UNIQUE INDEX gs_job_attribute_name_index ON pg_catalog.gs_job_attribute USING BTREE(job_name text_ops, attribute_name text_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4456;
CREATE UNIQUE INDEX gs_job_attribute_oid_index ON pg_catalog.gs_job_attribute USING BTREE(oid oid_ops);

DROP TYPE IF EXISTS pg_catalog.gs_job_argument;
DROP TABLE IF EXISTS pg_catalog.gs_job_argument;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG,false,true,9036,9036,0,0;
CREATE TABLE IF NOT EXISTS pg_catalog.gs_job_argument
(
    argument_position   integer NOCOMPRESS    not null,
    argument_type       name    NOCOMPRESS    not null,
    job_name            text NOCOMPRESS,
    argument_name       text NOCOMPRESS,
    argument_value      text NOCOMPRESS,
    default_value       text NOCOMPRESS
) WITH OIDS TABLESPACE pg_default;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4458;
CREATE UNIQUE INDEX gs_job_argument_oid_index ON pg_catalog.gs_job_argument USING BTREE(oid oid_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4459;
CREATE UNIQUE INDEX gs_job_argument_name_index ON pg_catalog.gs_job_argument USING BTREE(job_name text_ops, argument_name text_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 4460;
CREATE UNIQUE INDEX gs_job_argument_position_index ON pg_catalog.gs_job_argument USING BTREE(job_name text_ops, argument_position int4_ops);


SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.gs_job_attribute TO PUBLIC;
GRANT SELECT ON TABLE pg_catalog.gs_job_argument TO PUBLIC;do $$DECLARE ans boolean;
BEGIN
    for ans in select case when count(*)=1 then true else false end as ans  from (select nspname from pg_namespace where nspname='dbe_pldeveloper' limit 1)
    LOOP
        if ans = true then
            DROP TABLE IF EXISTS dbe_pldeveloper.gs_source;
            DROP TABLE IF EXISTS dbe_pldeveloper.gs_errors;
        end if;
        exit;
    END LOOP;
END$$;

DROP SCHEMA IF EXISTS dbe_pldeveloper;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_NAMESPACE, 4993;
CREATE SCHEMA DBE_PLDEVELOPER;
CREATE TABLE IF NOT EXISTS DBE_PLDEVELOPER.gs_source
(
    id oid,
    owner bigint,
    nspid oid,
    name name,
    type text,
    status boolean,
    src text
);
CREATE INDEX DBE_PLDEVELOPER.gs_source_id_idx ON DBE_PLDEVELOPER.gs_source USING btree(id);
CREATE INDEX DBE_PLDEVELOPER.gs_source_idx ON DBE_PLDEVELOPER.gs_source USING btree(owner, nspid, name, type);
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE DBE_PLDEVELOPER.gs_source TO PUBLIC;

CREATE TABLE IF NOT EXISTS DBE_PLDEVELOPER.gs_errors
(
    id oid,
    owner bigint,
    nspid oid,
    name name,
    type text,
    line int,
    src text
);
CREATE INDEX DBE_PLDEVELOPER.gs_errors_id_idx ON DBE_PLDEVELOPER.gs_source USING btree(id);
CREATE INDEX DBE_PLDEVELOPER.gs_errors_idx ON DBE_PLDEVELOPER.gs_errors USING btree(owner, nspid, name);
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE DBE_PLDEVELOPER.gs_errors TO PUBLIC;

DROP INDEX IF EXISTS pg_catalog.pg_proc_proname_args_nsp_new_index;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 9378;
CREATE INDEX pg_catalog.pg_proc_proname_args_nsp_new_index on pg_catalog.pg_proc USING BTREE(proname name_ops, proargtypes oidvector_ops, pronamespace oid_ops);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
-- ----------------------------------------------------------------
-- upgrade pg_catalog.pg_conversion 
-- ----------------------------------------------------------------
UPDATE pg_catalog.pg_conversion SET conforencoding=36 WHERE conname like 'gb18030_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=36 WHERE conname like '%_to_gb18030';

UPDATE pg_catalog.pg_conversion SET conforencoding=37 WHERE conname like 'sjis_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=37 WHERE conname like '%_to_sjis';

UPDATE pg_catalog.pg_conversion SET conforencoding=38 WHERE conname like 'big5_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=38 WHERE conname like '%_to_big5';

UPDATE pg_catalog.pg_conversion SET conforencoding=39 WHERE conname like 'uhc_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=39 WHERE conname like '%_to_uhc';
CREATE FUNCTION pg_catalog.insert_pg_authid_temp(default_role_name text) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'INSERT INTO pg_catalog.pg_authid VALUES (''' || default_role_name || ''', false, true, false, false, false, false, false, false, false, -1, null, null, null, ''default_pool'', false, 0, null, ''n'', 0, null, null, null, false, false, false)';
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

CREATE FUNCTION pg_catalog.insert_pg_shdepend_temp(default_role_id oid) RETURNS void
AS $$
DECLARE
    query_str text;
BEGIN
    query_str := 'INSERT INTO pg_catalog.pg_shdepend VALUES (0,0,0,0, 1260,' || default_role_id || ', ''p'')';
    EXECUTE query_str;
    return;
END;
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1056;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_directory_create');
SELECT pg_catalog.insert_pg_shdepend_temp(1056);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1059;
SELECT pg_catalog.insert_pg_authid_temp('gs_role_directory_drop');
SELECT pg_catalog.insert_pg_shdepend_temp(1059);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DROP FUNCTION pg_catalog.insert_pg_authid_temp();
DROP FUNCTION pg_catalog.insert_pg_shdepend_temp();
-- ----------------------------------------------------------------
-- upgrade pg_catalog.pg_collation 
-- ----------------------------------------------------------------
insert into pg_catalog.pg_collation values ('zh_CN', 11, 10, 36, 'zh_CN.gb18030', 'zh_CN.gb18030'), ('zh_CN.gb18030', 11, 10, 36, 'zh_CN.gb18030', 'zh_CN.gb18030');-- adding system table pg_publication

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 6130, 6141, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_publication
(
    pubname name NOCOMPRESS,
    pubowner oid NOCOMPRESS,
    puballtables bool NOCOMPRESS,
    pubinsert bool NOCOMPRESS,
    pubupdate bool NOCOMPRESS,
    pubdelete bool NOCOMPRESS
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6120;
CREATE UNIQUE INDEX pg_publication_oid_index ON pg_catalog.pg_publication USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6121;
CREATE UNIQUE INDEX pg_publication_pubname_index ON pg_catalog.pg_publication USING BTREE(pubname);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_publication TO PUBLIC;

-- adding system table pg_publication_rel

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 6132, 6142, 0, 0;

CREATE TABLE IF NOT EXISTS pg_catalog.pg_publication_rel
(
    prpubid oid NOCOMPRESS NOT NULL,
    prrelid oid NOCOMPRESS NOT NULL
) WITH OIDS;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6122;
CREATE UNIQUE INDEX pg_publication_rel_oid_index ON pg_catalog.pg_publication_rel USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 6123;
CREATE UNIQUE INDEX pg_publication_rel_map_index ON pg_catalog.pg_publication_rel USING BTREE(prrelid, prpubid);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_publication_rel TO PUBLIC;SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4768;
CREATE OR REPLACE FUNCTION pg_catalog.gs_read_block_from_remote
(  int4,
   int4,
   int4,
   int2, 
   int2,
   int4,
   xid,
   int4,
   xid,
   boolean)
RETURNS SETOF record LANGUAGE INTERNAL ROWS 1 STRICT as 'gs_read_block_from_remote_compress';
-- pg_read_binary_file_blocks()
--
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8413;
CREATE FUNCTION pg_catalog.pg_read_binary_file_blocks(IN inputpath text, IN startblocknum int8, IN count int8,
                                           OUT path text,
                                           OUT blocknum int4,
                                           OUT len int4,
                                           OUT data bytea)
    AS 'pg_read_binary_file_blocks' LANGUAGE INTERNAL IMMUTABLE STRICT;