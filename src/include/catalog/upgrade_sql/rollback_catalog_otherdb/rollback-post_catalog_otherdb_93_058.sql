DECLARE
cnt int;
BEGIN
select count(*) into cnt from pg_am where amname = 'diskann';
if cnt = 1 then
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_l2_ops USING diskann CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_l2_ops USING diskann CASCADE; 
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_ip_ops USING diskann CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_ip_ops USING diskann CASCADE;
        DROP OPERATOR FAMILY IF EXISTS pg_catalog.vector_cosine_ops USING diskann CASCADE;
        DROP OPERATOR CLASS IF EXISTS pg_catalog.vector_cosine_ops USING diskann CASCADE;
end if;
END;
    
DROP ACCESS METHOD IF EXISTS diskann CASCADE;

DROP FUNCTION IF EXISTS pg_catalog.diskannbuild(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannbuildempty(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskanninsert(internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannbulkdelete(internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannvacuumcleanup(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskanncostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannoptions(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannvalidate(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannbeginscan(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannrescan(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskanngettuple(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannendscan(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.diskannhandler(internal) CASCADE;