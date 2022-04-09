-- drop prefix key index
DO $$
DECLARE
  stmt text;
  cursor r is SELECT ns.nspname as nmspc, cl.relname as idxname
              from pg_catalog.pg_index idx
                    join pg_catalog.pg_class cl on idx.indexrelid  = cl.oid
                    join pg_catalog.pg_namespace ns on cl.relnamespace = ns.oid
              where pg_catalog.strpos(idx.indexprs, '{PREFIXKEY') > 0;
  res r%rowtype;
BEGIN
	for res in r loop
	  stmt := 'DROP INDEX ' || res.nmspc || '.' || res.idxname;
    execute immediate stmt;
	end loop;
END$$;
