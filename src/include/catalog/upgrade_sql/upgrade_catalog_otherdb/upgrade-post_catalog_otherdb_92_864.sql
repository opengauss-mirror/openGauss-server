CREATE OR REPLACE FUNCTION pg_catalog.update_pg_description(IN colloid integer, IN colldesc text)
RETURNS void
AS $$
DECLARE
row_name record;
query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
    delete from pg_catalog.pg_description where objoid = colloid and classoid = 3456;
    insert into pg_catalog.pg_description values(colloid, 3456, 0, colldesc);
  END LOOP;
  return;
END;
$$ LANGUAGE 'plpgsql';

select pg_catalog.update_pg_description(1327, 'gbk_chinese_ci collation');
select pg_catalog.update_pg_description(1328, 'gbk_bin collation');
select pg_catalog.update_pg_description(1800, 'gb18030_chinese_ci collation');
select pg_catalog.update_pg_description(1801, 'gb18030_bin collation');

DROP FUNCTION pg_catalog.update_pg_description;
