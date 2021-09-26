/* distribute insert row into table pg_am */
CREATE OR REPLACE FUNCTION Delete_pg_am_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_am where amname=''ubtree'' ';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4439;
SELECT Delete_pg_am_temp();
DROP FUNCTION Delete_pg_am_temp();

/* openGauss insert row into table pg_amop */
CREATE OR REPLACE FUNCTION Delete_pg_amop_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_amop where amopfamily IN (6976,6989,7789,6991,6970,5429,6986,6994,5426 ,5428,5436,8806,9535,5421,5434,6996,7000,6982 ,6984,6974,6988,5424,5423,7002,7095,7097,7099 ,7233,7234,5397,7994,7968,8522,8626,8683,8901,9570)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Delete_pg_amop_temp();
DROP FUNCTION Delete_pg_amop_temp();

/* openGauss insert row into table pg_amproc */
CREATE OR REPLACE FUNCTION Delete_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_amproc where amprocfamily IN (5421,5397,5423,5424,5426,5428,5429,6974,5434,5436,6970,6976,6982,6984,6986,6988,6989,6991,7994,6994,6996,7000,7002,7095,7097,7099,7233,7234,7789,7968,8522,8806,9535,
9570,8901,8626,8683)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Delete_pg_amproc_temp();
DROP FUNCTION Delete_pg_amproc_temp();

/* openGauss insert row into table pg_opclass */
CREATE OR REPLACE FUNCTION Delete_pg_opclass_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_opclass where opcmethod = 4439 ';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SELECT Delete_pg_opclass_temp();
DROP FUNCTION Delete_pg_opclass_temp();

/* distribute insert row into table pg_opfamily */
/* -----------------------------------*/
CREATE OR REPLACE FUNCTION Delete_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'delete from pg_catalog.pg_opfamily where opfmethod = 4439';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Delete_pg_opfamily_temp();
DROP FUNCTION Delete_pg_opfamily_temp();

DROP FUNCTION IF EXISTS pg_catalog.gs_txid_oldestxmin();
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_meta(int4, int4, int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_undo_translot (int4, int4);
DROP FUNCTION IF EXISTS pg_catalog.gs_stat_ustore();
DROP FUNCTION IF EXISTS pg_catalog.ubtinsert(internal, internal, internal, internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtbeginscan(internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtgettuple(internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtgetbitmap(internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtrescan(internal, internal, internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtendscan(internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtmarkpos(internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtrestrpos(internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtmerge(internal, internal, internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtbuild(internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtbuildempty(internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtbulkdelete(internal, internal, internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtvacuumcleanup(internal, internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtcanreturn(internal);
DROP FUNCTION IF EXISTS pg_catalog.ubtoptions(_text, bool);
DROP FUNCTION IF EXISTS pg_catalog.ubtcostestimate(internal, internal, internal, internal, internal, internal, internal);