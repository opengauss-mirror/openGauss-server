DO $$
DECLARE
cnt int;
BEGIN
    select count(*) into cnt from pg_type where typname = 'int16';
    if cnt = 1 then
        DROP FUNCTION IF EXISTS pg_catalog.last_insert_id() CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.last_insert_id(int16) CASCADE;
    end if;
END$$;

-- delete auto_increment
DO $$
DECLARE
  stmt text;
  cursor r is SELECT ns.nspname as nmspc, cl.relname as tbnm, ab.attname as atnm
              from pg_catalog.pg_attrdef ad join pg_catalog.pg_class cl on ad.adrelid = cl.oid
                    join pg_catalog.pg_namespace ns on cl.relnamespace = ns.oid
                    join pg_catalog.pg_attribute ab on ad.adrelid = ab.attrelid and ad.adnum = ab.attnum
              where ad.adsrc = 'AUTO_INCREMENT';
  res r%rowtype;
BEGIN
	for res in r loop
	  stmt := 'ALTER TABLE ' || res.nmspc || '.' || res.tbnm || ' DROP COLUMN ' || res.atnm;
    execute immediate stmt;
	end loop;
END$$;
