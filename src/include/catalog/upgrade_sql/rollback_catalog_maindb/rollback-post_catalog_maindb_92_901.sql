CREATE OR REPLACE FUNCTION Delete_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_amproc where amprocnum = 3 and (amproc = 4608 or amproc = 4609)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Delete_pg_amproc_temp();
DROP FUNCTION Delete_pg_amproc_temp();

DROP FUNCTION IF EXISTS pg_catalog.btvarstrequalimage();
DROP FUNCTION IF EXISTS pg_catalog.btequalimage();

CREATE OR REPLACE FUNCTION Update_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'update pg_catalog.pg_am set amsupport = 2, amhandler = 0 where amname = ''btree'' or amname = ''ubtree''';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Update_pg_amproc_temp();
DROP FUNCTION Update_pg_amproc_temp();