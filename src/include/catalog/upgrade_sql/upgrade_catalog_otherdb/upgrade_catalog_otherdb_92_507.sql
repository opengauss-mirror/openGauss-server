DROP TYPE IF EXISTS pg_catalog.gs_job_attribute;
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

-- adding system table pg_publication

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

GRANT SELECT ON TABLE pg_catalog.pg_publication_rel TO PUBLIC;
