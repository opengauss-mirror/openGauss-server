CREATE OR REPLACE FUNCTION pg_catalog.Delete_pg_collation_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_description where objoid in (1327, 1328, 1800, 1801) and classoid = 3456;
  END LOOP;
  return;
END;
$$ LANGUAGE 'plpgsql';
 
SELECT pg_catalog.Delete_pg_collation_temp();
DROP FUNCTION pg_catalog.Delete_pg_collation_temp();
