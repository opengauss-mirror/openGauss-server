DROP INDEX IF EXISTS pg_catalog.gs_dependencies_name_index;
DROP INDEX IF EXISTS pg_catalog.gs_dependencies_refoid_index;
DROP INDEX IF EXISTS pg_catalog.gs_dependencies_obj_oid_index;
DROP INDEX IF EXISTS pg_catalog.gs_dependencies_obj_name_index;
DROP TABLE IF EXISTS pg_catalog.gs_dependencies;
DROP TABLE IF EXISTS pg_catalog.gs_dependencies_obj;

DECLARE
    cnt int;
BEGIN
    select count(*) into cnt from pg_type where oid = 4408;
    if cnt = 1 then
            DROP FUNCTION IF EXISTS pg_catalog.undefinedin(cstring) CASCADE;
            DROP FUNCTION IF EXISTS pg_catalog.undefinedout(undefined) CASCADE;
            DROP FUNCTION IF EXISTS pg_catalog.undefinedrecv(internal) CASCADE;
            DROP FUNCTION IF EXISTS pg_catalog.undefinedsend(undefined) CASCADE;
    end if;
END;
DROP TYPE IF EXISTS pg_catalog.undefined CASCADE;