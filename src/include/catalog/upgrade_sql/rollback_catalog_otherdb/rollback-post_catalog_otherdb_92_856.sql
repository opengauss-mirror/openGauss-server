--------------------------------------------------------------
-- delete pg_catalog.pg_collation 
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.Delete_pg_collation_temp()
RETURNS void
AS $$
DECLARE
row_name record;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_collation where collname in ('gbk_chinese_ci', 'gbk_bin', 'gb18030_chinese_ci', 'gb18030_bin');
  END LOOP;
return;
END;
$$ LANGUAGE 'plpgsql';
 
SELECT pg_catalog.Delete_pg_collation_temp();
DROP FUNCTION pg_catalog.Delete_pg_collation_temp();