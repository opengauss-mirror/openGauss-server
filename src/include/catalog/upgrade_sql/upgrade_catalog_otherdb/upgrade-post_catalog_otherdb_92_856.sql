-- ----------------------------------------------------------------
-- upgrade pg_catalog.pg_collation 
-- ----------------------------------------------------------------
CREATE OR REPLACE FUNCTION pg_catalog.Insert_pg_collation_temp(
IN collname text,
IN collnamespace integer,
IN collowner integer,
IN collencoding integer,
IN collcollate text,
IN collctype text,
IN collpadattr text,
IN collisdef bool
)
RETURNS void
AS $$
DECLARE
  row_name record;
  query_str_nodes text;
BEGIN
  query_str_nodes := 'select * from dbe_perf.node_name';
  FOR row_name IN EXECUTE(query_str_nodes) LOOP
      insert into pg_catalog.pg_collation values (collname, collnamespace, collowner, collencoding, collcollate, collctype, collpadattr, collisdef);
  END LOOP;
  return;
END; $$
LANGUAGE 'plpgsql';
 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1327;
select pg_catalog.Insert_pg_collation_temp('gbk_chinese_ci', 11, 10, 6, 'gbk_chinese_ci', 'gbk_chinese_ci', 'PAD SPACE', true);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1328;
select pg_catalog.Insert_pg_collation_temp('gbk_bin', 11, 10, 6, 'gbk_bin', 'gbk_bin', 'PAD SPACE', null);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1800;
select pg_catalog.Insert_pg_collation_temp('gb18030_chinese_ci', 11, 10, 36, 'gb18030_chinese_ci', 'gb18030_chinese_ci', 'PAD SPACE', true);
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 1801;
select pg_catalog.Insert_pg_collation_temp('gb18030_bin', 11, 10, 36, 'gb18030_bin', 'gb18030_bin', 'PAD SPACE', null);

DROP FUNCTION pg_catalog.Insert_pg_collation_temp;