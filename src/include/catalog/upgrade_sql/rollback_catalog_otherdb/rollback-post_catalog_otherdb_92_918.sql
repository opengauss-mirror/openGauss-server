DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'jsonb' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_insert(jsonb, text[], jsonb, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_set(jsonb, text[], jsonb, boolean) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, int) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.jsonb_delete(jsonb, text[]) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, integer) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, text) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(jsonb, text[]) CASCADE;
    end if;
END$$;