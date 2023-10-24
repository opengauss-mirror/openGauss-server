declare
    dependencies_exist int:=0;
    dependencies_obj_exist int:=0;
begin
    select count(*) into dependencies_exist from pg_catalog.pg_class where oid = 7111;
    if dependencies_exist = 0 then
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 7111, 7112, 0, 0;
        DROP INDEX IF EXISTS pg_catalog.gs_dependencies_name_index;
        DROP INDEX IF EXISTS pg_catalog.gs_dependencies_refoid_index;
        DROP TYPE IF EXISTS pg_catalog.gs_dependencies;
        CREATE TABLE IF NOT EXISTS pg_catalog.gs_dependencies(
          schemaname name NOCOMPRESS NOT NULL,
          packagename name NOCOMPRESS NOT NULL,
          refobjpos int NOT NULL,
          refobjoid oid NOT NULL,
          objectname text
        );
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8004;
        CREATE INDEX pg_catalog.gs_dependencies_name_index ON pg_catalog.gs_dependencies USING BTREE(schemaname name_ops, packagename name_ops, refobjpos int4_ops);
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8006;
        CREATE INDEX pg_catalog.gs_dependencies_refoid_index ON pg_catalog.gs_dependencies USING BTREE(refobjoid oid_ops);
        GRANT SELECT ON TABLE pg_catalog.gs_dependencies TO PUBLIC;
    end if;
    select count(*) into dependencies_obj_exist from pg_catalog.pg_class where oid = 7169;
    if dependencies_obj_exist = 0 then
        DROP INDEX IF EXISTS pg_catalog.gs_dependencies_obj_oid_index;
        DROP INDEX IF EXISTS pg_catalog.gs_dependencies_obj_name_index;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 7169, 7170, 0, 0;
        DROP TYPE IF EXISTS pg_catalog.gs_dependencies_obj;
        CREATE TABLE IF NOT EXISTS pg_catalog.gs_dependencies_obj(
         schemaname name NOCOMPRESS NOT NULL,
         packagename name NOCOMPRESS NOT NULL,
         type int,
         name text,
         objnode pg_node_tree
        ) WITH OIDS;
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8007;
        CREATE UNIQUE INDEX pg_catalog.gs_dependencies_obj_oid_index ON pg_catalog.gs_dependencies_obj USING BTREE(oid oid_ops);
        SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 8008;
        CREATE INDEX pg_catalog.gs_dependencies_obj_name_index ON pg_catalog.gs_dependencies_obj USING BTREE(schemaname name_ops, packagename name_ops, type int4_ops);
        GRANT SELECT ON TABLE pg_catalog.gs_dependencies_obj TO PUBLIC;
    end if;
end;
/

DROP TYPE IF EXISTS pg_catalog.undefined;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 4408, 0, u;
CREATE TYPE pg_catalog.undefined;

DROP FUNCTION IF EXISTS pg_catalog.undefinedin(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5704;
CREATE FUNCTION pg_catalog.undefinedin(cstring) RETURNS undefined LANGUAGE INTERNAL IMMUTABLE STRICT as 'undefinedin';

DROP FUNCTION IF EXISTS pg_catalog.undefinedout(undefined) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5707;
CREATE FUNCTION pg_catalog.undefinedout(undefined) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'undefinedout';

DROP FUNCTION IF EXISTS pg_catalog.undefinedrecv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5710;
CREATE FUNCTION pg_catalog.undefinedrecv(internal) RETURNS undefined LANGUAGE INTERNAL IMMUTABLE STRICT as 'undefinedrecv';

DROP FUNCTION IF EXISTS pg_catalog.undefinedsend(undefined) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5709;
CREATE FUNCTION pg_catalog.undefinedsend(undefined) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'undefinedsend';
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

CREATE TYPE pg_catalog.undefined(
    INPUT=undefinedin,
    OUTPUT=undefinedout,
    RECEIVE=undefinedrecv,
    SEND=undefinedsend,
    PASSEDBYVALUE=false,
    INTERNALLENGTH=-2,
    CATEGORY='W',
    PREFERRED=false,
    ALIGNMENT=char,
    STORAGE=plain
);
COMMENT ON TYPE pg_catalog.undefined IS 'undefined objects at PLSQL compilation time';
