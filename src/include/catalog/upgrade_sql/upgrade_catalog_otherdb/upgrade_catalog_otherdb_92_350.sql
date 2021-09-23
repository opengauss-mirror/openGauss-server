--step1.create sysfuction
/* add sys fuction gs_txid_oldestxmin */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 8642;
CREATE FUNCTION pg_catalog.gs_txid_oldestxmin() RETURNS bigint LANGUAGE INTERNAL as 'gs_txid_oldestxmin';

/*------ add sys fuction gs_undo_meta ------*/
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4430;
CREATE FUNCTION pg_catalog.gs_undo_meta(int4, int4, int4, OUT zoneId oid, OUT persistType oid, OUT insertptr text, OUT discard text, OUT endptr text, OUT used text, OUT lsn text) 
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_meta';

/* add sys fuction gs_undo_translot */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4431;
CREATE FUNCTION pg_catalog.gs_undo_translot(int4, int4, OUT grpId oid, OUT xactId text, OUT startUndoPtr text, OUT endUndoPtr text, OUT lsn text) 
RETURNS SETOF record LANGUAGE INTERNAL as 'gs_undo_translot';

/* add sys fuction gs_stat_ustore */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 7893;
CREATE FUNCTION pg_catalog.gs_stat_ustore() RETURNS text LANGUAGE INTERNAL as 'gs_stat_ustore';

/* add sys fuction ubtinsert */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4750;
CREATE FUNCTION pg_catalog.ubtinsert(internal, internal, internal, internal, internal, internal) RETURNS bool LANGUAGE INTERNAL as 'ubtinsert';

/* add sys fuction ubtbeginscan */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4751;
CREATE FUNCTION pg_catalog.ubtbeginscan(internal, internal, internal) RETURNS internal LANGUAGE INTERNAL as 'ubtbeginscan';

/* add sys fuction ubtgettuple */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4752;
CREATE FUNCTION pg_catalog.ubtgettuple(internal, internal) RETURNS bool LANGUAGE INTERNAL as 'ubtgettuple';

/* add sys fuction ubtgetbitmap */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4753;
CREATE FUNCTION pg_catalog.ubtgetbitmap(internal, internal) RETURNS bigint LANGUAGE INTERNAL as 'ubtgetbitmap';

/* add sys fuction ubtrescan */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4754;
CREATE FUNCTION pg_catalog.ubtrescan(internal, internal, internal, internal, internal) RETURNS void LANGUAGE INTERNAL as 'ubtrescan';

/* add sys fuction ubtendscan */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4755;
CREATE FUNCTION pg_catalog.ubtendscan(internal) RETURNS void LANGUAGE INTERNAL as 'ubtendscan';

/* add sys fuction ubtmarkpos */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4756;
CREATE FUNCTION pg_catalog.ubtmarkpos(internal) RETURNS void LANGUAGE INTERNAL as 'ubtmarkpos';

/* add sys fuction ubtrestrpos */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4757;
CREATE FUNCTION pg_catalog.ubtrestrpos(internal) RETURNS void LANGUAGE INTERNAL as 'ubtrestrpos';

/* add sys fuction ubtmerge */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4758;
CREATE FUNCTION pg_catalog.ubtmerge(internal, internal, internal, internal, internal) RETURNS void LANGUAGE INTERNAL as 'ubtmerge';

/* add sys fuction ubtbuild */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4759;
CREATE FUNCTION pg_catalog.ubtbuild(internal, internal, internal) RETURNS internal LANGUAGE INTERNAL as 'ubtbuild';

/* add sys fuction ubtbuildempty */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4760;
CREATE FUNCTION pg_catalog.ubtbuildempty(internal) RETURNS void LANGUAGE INTERNAL as 'ubtbuildempty';

/* add sys fuction ubtbulkdelete */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4761;
CREATE FUNCTION pg_catalog.ubtbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE INTERNAL as 'ubtbulkdelete';

/* add sys fuction ubtvacuumcleanup */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4762;
CREATE FUNCTION pg_catalog.ubtvacuumcleanup(internal, internal) RETURNS internal LANGUAGE INTERNAL as 'ubtvacuumcleanup';

/* add sys fuction ubtcanreturn */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4763;
CREATE FUNCTION pg_catalog.ubtcanreturn(internal) RETURNS bool LANGUAGE INTERNAL as 'ubtcanreturn';

/* add sys fuction ubtoptions */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4764;
CREATE FUNCTION pg_catalog.ubtoptions(_text, bool) RETURNS bytea LANGUAGE INTERNAL as 'ubtoptions';

/* add sys fuction ubtoptions */
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4765;
CREATE FUNCTION pg_catalog.ubtcostestimate(internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE INTERNAL as 'ubtcostestimate';

/* centralized insert row into table pg_am */
CREATE OR REPLACE FUNCTION pg_catalog.Insert_pg_am_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_am values (''ubtree'',5,2,true,false,true,true,true,true,true,true,false,true,true,0,''ubtinsert'',''ubtbeginscan'',''ubtgettuple'',''ubtgetbitmap'',''ubtrescan'',''ubtendscan'',''ubtmarkpos'',''ubtrestrpos'',''ubtmerge'',''ubtbuild'',''ubtbuildempty'',''ubtbulkdelete'',''ubtvacuumcleanup'',''ubtcanreturn'',''ubtcostestimate'',''ubtoptions'')';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 4439;
SELECT pg_catalog.Insert_pg_am_temp();
DROP FUNCTION pg_catalog.Insert_pg_am_temp();


/* openGauss insert row into table pg_amop */
CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 21, 1, ''s'', 95, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7000;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 21, 2, ''s'', 522, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7001;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 21, 3, ''s'', 94, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7002;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 21, 4, ''s'', 524, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7003;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 21, 5, ''s'', 520, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7004;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 23, 1, ''s'', 534, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7005;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 23, 2, ''s'', 540, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7006;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 23, 3, ''s'', 532, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7007;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 23, 4, ''s'', 542, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7008;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 23, 5, ''s'', 536, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7009;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 20, 1, ''s'', 1864, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7010;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 20, 2, ''s'', 1866, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7011;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 20, 3, ''s'', 1862, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7012;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 20, 4, ''s'', 1867, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7013;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  21, 20, 5, ''s'', 1865, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7014;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 23, 1, ''s'', 97, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7015;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 23, 2, ''s'', 523, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7016;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 23, 3, ''s'', 96, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7017;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 23, 4, ''s'', 525, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7018;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 23, 5, ''s'', 521, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7019;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 21, 1, ''s'', 535, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7020;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 21, 2, ''s'', 541, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7021;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 21, 3, ''s'', 533, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7022;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 21, 4, ''s'', 543, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7023;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 21, 5, ''s'', 537, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7024;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 20, 1, ''s'', 37, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7025;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 20, 2, ''s'', 80, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7026;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 20, 3, ''s'', 15, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7027;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 20, 4, ''s'', 82, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7028;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  23, 20, 5, ''s'', 76, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7029;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 20, 1, ''s'', 412, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7030;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 20, 2, ''s'', 414, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7031;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 20, 3, ''s'', 410, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7032;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 20, 4, ''s'', 415, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7033;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 20, 5, ''s'', 413, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7034;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 21, 1, ''s'', 1870, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7035;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 21, 2, ''s'', 1872, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7036;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 21, 3, ''s'', 1868, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7037;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 21, 4, ''s'', 1873, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7038;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 21, 5, ''s'', 1871, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7039;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 23, 1, ''s'', 418, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7040;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 23, 2, ''s'', 420, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7041;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 23, 3, ''s'', 416, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7042;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 23, 4, ''s'', 430, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7043;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6976,  20, 23, 5, ''s'', 419, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7044;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6989,  26, 26, 1, ''s'', 609, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7045;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6989,  26, 26, 2, ''s'', 611, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7046;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6989,  26, 26, 3, ''s'', 607, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7047;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6989,  26, 26, 4, ''s'', 612, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7048;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6989,  26, 26, 5, ''s'', 610, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7049;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7789,  27, 27, 1, ''s'', 2799, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7050;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7789,  27, 27, 2, ''s'', 2801, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7051;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7789,  27, 27, 3, ''s'', 387, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7052;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7789,  27, 27, 4, ''s'', 2802, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7053;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7789,  27, 27, 5, ''s'', 2800, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7054;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6991,  30, 30, 1, ''s'', 645, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7055;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6991,  30, 30, 2, ''s'', 647, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7056;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6991,  30, 30, 3, ''s'', 649, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7057;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6991,  30, 30, 4, ''s'', 648, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7058;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6991,  30, 30, 5, ''s'', 646, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7059;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 700, 1, ''s'', 622, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7060;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 700, 2, ''s'', 624, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7061;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 700, 3, ''s'', 620, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7062;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 700, 4, ''s'', 625, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7063;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 700, 5, ''s'', 623, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7064;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 701, 1, ''s'', 1122, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7065;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 701, 2, ''s'', 1124, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7066;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 701, 3, ''s'', 1120, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7067;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 701, 4, ''s'', 1125, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7068;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  700, 701, 5, ''s'', 1123, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7069;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 701, 1, ''s'', 672, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7070;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 701, 2, ''s'', 673, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7071;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 701, 3, ''s'', 670, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7072;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 701, 4, ''s'', 675, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7073;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 701, 5, ''s'', 674, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7074;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 700, 1, ''s'', 1132, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7075;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 700, 2, ''s'', 1134, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7076;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 700, 3, ''s'', 1130, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7077;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 700, 4, ''s'', 1135, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7078;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6970,  701, 700, 5, ''s'', 1133, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7079;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5429,  18, 18, 1, ''s'', 631, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7080;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5429,  18, 18, 2, ''s'', 632, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7081;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5429,  18, 18, 3, ''s'', 92, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7082;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5429,  18, 18, 4, ''s'', 634, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7083;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5429,  18, 18, 5, ''s'', 633, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7084;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6986,  19, 19, 1, ''s'', 660, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7085;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6986,  19, 19, 2, ''s'', 661, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7086;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6986,  19, 19, 3, ''s'', 93, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7087;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6986,  19, 19, 4, ''s'', 663, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7088;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6986,  19, 19, 5, ''s'', 662, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7089;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6994,  25, 25, 1, ''s'', 664, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7090;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6994,  25, 25, 2, ''s'', 665, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7091;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6994,  25, 25, 3, ''s'', 98, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7092;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6994,  25, 25, 4, ''s'', 667, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7093;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6994,  25, 25, 5, ''s'', 666, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7094;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5426,  1042, 1042, 1, ''s'', 1058, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7095;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5426,  1042, 1042, 2, ''s'', 1059, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7096;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5426,  1042, 1042, 3, ''s'', 1054, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7097;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5426,  1042, 1042, 4, ''s'', 1061, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7098;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5426,  1042, 1042, 5, ''s'', 1060, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7099;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5428,  17, 17, 1, ''s'', 1957, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7100;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5428,  17, 17, 2, ''s'', 1958, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7101;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5428,  17, 17, 3, ''s'', 1955, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7102;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5428,  17, 17, 4, ''s'', 1960, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7103;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5428,  17, 17, 5, ''s'', 1959, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7104;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5436,  4402, 4402, 3, ''s'', 4453, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7105;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8806,  86, 86, 1, ''s'', 3800, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7106;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8806,  86, 86, 2, ''s'', 3801, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7107;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8806,  86, 86, 3, ''s'', 3798, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7108;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8806,  86, 86, 4, ''s'', 3803, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7109;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8806,  86, 86, 5, ''s'', 3802, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7110;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9535,  5545, 5545, 1, ''s'', 5515, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7111;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9535,  5545, 5545, 2, ''s'', 5516, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7112;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9535,  5545, 5545, 3, ''s'', 5513, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7113;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9535,  5545, 5545, 4, ''s'', 5518, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7114;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9535,  5545, 5545, 5, ''s'', 5517, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7115;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5421,  702, 702, 1, ''s'', 562, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7116;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5421,  702, 702, 2, ''s'', 564, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7117;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5421,  702, 702, 3, ''s'', 560, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7118;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5421,  702, 702, 4, ''s'', 565, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7119;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5421,  702, 702, 5, ''s'', 563, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7120;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1082, 1, ''s'', 1095, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7121;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1082, 2, ''s'', 1096, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7122;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1082, 3, ''s'', 1093, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7123;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1082, 4, ''s'', 1098, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7124;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1082, 5, ''s'', 1097, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7125;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1114, 1, ''s'', 2345, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7126;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1114, 2, ''s'', 2346, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7127;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1114, 3, ''s'', 2347, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7128;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1114, 4, ''s'', 2348, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7129;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1114, 5, ''s'', 2349, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7130;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1184, 1, ''s'', 2358, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7131;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1184, 2, ''s'', 2359, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7132;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1184, 3, ''s'', 2360, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7133;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1184, 4, ''s'', 2361, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7134;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1082, 1184, 5, ''s'', 2362, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7135;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1114, 1, ''s'', 2062, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7136;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1114, 2, ''s'', 2063, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7137;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1114, 3, ''s'', 2060, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7138;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1114, 4, ''s'', 2065, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7139;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1114, 5, ''s'', 2064, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7140;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1082, 1, ''s'', 2371, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7141;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1082, 2, ''s'', 2372, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7142;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1082, 3, ''s'', 2373, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7143;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1082, 4, ''s'', 2374, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7144;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1082, 5, ''s'', 2375, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7145;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1184, 1, ''s'', 2534, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7146;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1184, 2, ''s'', 2535, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7147;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1184, 3, ''s'', 2536, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7148;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1184, 4, ''s'', 2537, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7149;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1114, 1184, 5, ''s'', 2538, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7150;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1184, 1, ''s'', 1322, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7151;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1184, 2, ''s'', 1323, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7152;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1184, 3, ''s'', 1320, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7153;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1184, 4, ''s'', 1325, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7154;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1184, 5, ''s'', 1324, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7155;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1082, 1, ''s'', 2384, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7156;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1082, 2, ''s'', 2385, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7157;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1082, 3, ''s'', 2386, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7158;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1082, 4, ''s'', 2387, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7159;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1082, 5, ''s'', 2388, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7160;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1114, 1, ''s'', 2540, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7161;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1114, 2, ''s'', 2541, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7162;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1114, 3, ''s'', 2542, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7163;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1114, 4, ''s'', 2543, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7164;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5434,  1184, 1114, 5, ''s'', 2544, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7165;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6996,  1083, 1083, 1, ''s'', 1110, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7166;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6996,  1083, 1083, 2, ''s'', 1111, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7167;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6996,  1083, 1083, 3, ''s'', 1108, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7168;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6996,  1083, 1083, 4, ''s'', 1113, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7169;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6996,  1083, 1083, 5, ''s'', 1112, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7170;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7000,  1266, 1266, 1, ''s'', 1552, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7171;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7000,  1266, 1266, 2, ''s'', 1553, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7172;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7000,  1266, 1266, 3, ''s'', 1550, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7173;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7000,  1266, 1266, 4, ''s'', 1555, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7174;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7000,  1266, 1266, 5, ''s'', 1554, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7175;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6982,  1186, 1186, 1, ''s'', 1332, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7176;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6982,  1186, 1186, 2, ''s'', 1333, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7177;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6982,  1186, 1186, 3, ''s'', 1330, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7178;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6982,  1186, 1186, 4, ''s'', 1335, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7179;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6982,  1186, 1186, 5, ''s'', 1334, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7180;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6984,  829, 829, 1, ''s'', 1222, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7181;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6984,  829, 829, 2, ''s'', 1223, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7182;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6984,  829, 829, 3, ''s'', 1220, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7183;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6984,  829, 829, 4, ''s'', 1225, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7184;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6984,  829, 829, 5, ''s'', 1224, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7185;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6974,  869, 869, 1, ''s'', 1203, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7186;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6974,  869, 869, 2, ''s'', 1204, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7187;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6974,  869, 869, 3, ''s'', 1201, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7188;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6974,  869, 869, 4, ''s'', 1206, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7189;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6974,  869, 869, 5, ''s'', 1205, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7190;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6988,  1700, 1700, 1, ''s'', 1754, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7191;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6988,  1700, 1700, 2, ''s'', 1755, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7192;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6988,  1700, 1700, 3, ''s'', 1752, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7193;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6988,  1700, 1700, 4, ''s'', 1757, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7194;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (6988,  1700, 1700, 5, ''s'', 1756, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7195;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5424,  16, 16, 1, ''s'', 58, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7196;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5424,  16, 16, 2, ''s'', 1694, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7197;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5424,  16, 16, 3, ''s'', 91, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7198;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5424,  16, 16, 4, ''s'', 1695, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7199;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5424,  16, 16, 5, ''s'', 59, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7200;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5423,  1560, 1560, 1, ''s'', 1786, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7201;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5423,  1560, 1560, 2, ''s'', 1788, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7202;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5423,  1560, 1560, 3, ''s'', 1784, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7203;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5423,  1560, 1560, 4, ''s'', 1789, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7204;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5423,  1560, 1560, 5, ''s'', 1787, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7205;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7002,  1562, 1562, 1, ''s'', 1806, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7206;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7002,  1562, 1562, 2, ''s'', 1808, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7207;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7002,  1562, 1562, 3, ''s'', 1804, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7208;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7002,  1562, 1562, 4, ''s'', 1809, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7209;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7002,  1562, 1562, 5, ''s'', 1807, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7210;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7095,  25, 25, 1, ''s'', 2314, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7211;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7095,  25, 25, 2, ''s'', 2315, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7212;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7095,  25, 25, 3, ''s'', 98, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7213;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7095,  25, 25, 4, ''s'', 2317, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7214;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7095,  25, 25, 5, ''s'', 2318, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7215;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7097,  1042, 1042, 1, ''s'', 2326, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7216;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7097,  1042, 1042, 2, ''s'', 2327, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7217;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7097,  1042, 1042, 3, ''s'', 1054, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7218;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7097,  1042, 1042, 4, ''s'', 2329, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7219;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7097,  1042, 1042, 5, ''s'', 2330, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7220;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7099,  790, 790, 1, ''s'', 902, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7221;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7099,  790, 790, 2, ''s'', 904, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7222;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7099,  790, 790, 3, ''s'', 900, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7223;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7099,  790, 790, 4, ''s'', 905, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7224;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7099,  790, 790, 5, ''s'', 903, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7225;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7233,  703, 703, 1, ''s'', 568, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7226;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7233,  703, 703, 2, ''s'', 570, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7227;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7233,  703, 703, 3, ''s'', 566, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7228;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7233,  703, 703, 4, ''s'', 571, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7229;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7233,  703, 703, 5, ''s'', 569, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7230;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7234,  704, 704, 1, ''s'', 813, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7231;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7234,  704, 704, 2, ''s'', 815, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7232;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7234,  704, 704, 3, ''s'', 811, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7233;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7234,  704, 704, 4, ''s'', 816, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7234;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7234,  704, 704, 5, ''s'', 814, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7235;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5397,  2277, 2277, 1, ''s'', 1072, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7236;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5397,  2277, 2277, 2, ''s'', 1074, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7237;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5397,  2277, 2277, 3, ''s'', 1070, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7238;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5397,  2277, 2277, 4, ''s'', 1075, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7239;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (5397,  2277, 2277, 5, ''s'', 1073, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7240;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7994,  2249, 2249, 1, ''s'', 2990, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7241;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7994,  2249, 2249, 2, ''s'', 2992, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7242;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7994,  2249, 2249, 3, ''s'', 2988, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7243;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7994,  2249, 2249, 4, ''s'', 2993, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7244;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7994,  2249, 2249, 5, ''s'', 2991, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7245;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7968,  2950, 2950, 1, ''s'', 2974, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7246;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7968,  2950, 2950, 2, ''s'', 2976, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7247;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7968,  2950, 2950, 3, ''s'', 2972, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7248;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7968,  2950, 2950, 4, ''s'', 2977, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7249;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (7968,  2950, 2950, 5, ''s'', 2975, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7250;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8522,  3500, 3500, 1, ''s'', 3518, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7251;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8522,  3500, 3500, 2, ''s'', 3520, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7252;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8522,  3500, 3500, 3, ''s'', 3516, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7253;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8522,  3500, 3500, 4, ''s'', 3521, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7254;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8522,  3500, 3500, 5, ''s'', 3519, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7255;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8626,  3614, 3614, 1, ''s'', 3627, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7256;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8626,  3614, 3614, 2, ''s'', 3628, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7257;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8626,  3614, 3614, 3, ''s'', 3629, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7258;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8626,  3614, 3614, 4, ''s'', 3631, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7259;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8626,  3614, 3614, 5, ''s'', 3632, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7260;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8683,  3615, 3615, 1, ''s'', 3674, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7261;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8683,  3615, 3615, 2, ''s'', 3675, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7262;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8683,  3615, 3615, 3, ''s'', 3676, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7263;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8683,  3615, 3615, 4, ''s'', 3678, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7264;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8683,  3615, 3615, 5, ''s'', 3679, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7265;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8901,  3831, 3831, 1, ''s'', 3884, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7266;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8901,  3831, 3831, 2, ''s'', 3885, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7267;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8901,  3831, 3831, 3, ''s'', 3882, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7268;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8901,  3831, 3831, 4, ''s'', 3886, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7269;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (8901,  3831, 3831, 5, ''s'', 3887, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7270;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9570,  9003, 9003, 1, ''s'', 5552, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7271;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9570,  9003, 9003, 2, ''s'', 5553, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7272;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9570,  9003, 9003, 3, ''s'', 5550, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7273;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9570,  9003, 9003, 4, ''s'', 5549, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7274;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();

CREATE OR REPLACE FUNCTION Insert_pg_amop_temp()
RETURNS void
AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amop values (9570,  9003, 9003, 5, ''s'', 5554, 4439, 0 )';
EXECUTE(query_str);
return;
END; 
$$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7275;
SELECT Insert_pg_amop_temp();
DROP FUNCTION Insert_pg_amop_temp();


/* openGauss insert row into table pg_amproc */
CREATE OR REPLACE FUNCTION Insert_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amproc values
(5397, 2277, 2277, 1, ''btarraycmp''),
(5421, 702,  702,  1, ''btabstimecmp''),
(5423, 1560, 1560, 1, ''bitcmp''),
(5424, 16,   16,   1, ''btboolcmp''),
(5426, 1042, 1042, 1, ''bpcharcmp''),
(5426, 1042, 1042, 2, ''bpchar_sortsupport''),
(5428, 17,   17,   1, ''byteacmp''),
(5428, 17,   17,   2, ''bytea_sortsupport''),
(5429, 18,   18,   1, ''btcharcmp''),
(5434, 1082, 1082, 1, ''date_cmp''),
(5434, 1082, 1082, 2, ''date_sortsupport''),
(5434, 1082, 1114, 1, ''date_cmp_timestamp''),
(5434, 1082, 1184, 1, ''date_cmp_timestamptz''),
(5434, 1114, 1114, 1, ''timestamp_cmp''),
(5434, 1114, 1114, 2, ''timestamp_sortsupport''),
(5434, 1114, 1082, 1, ''timestamp_cmp_date''),
(5434, 1114, 1184, 1, ''timestamp_cmp_timestamptz''),
(5434, 1184, 1184, 1, ''timestamptz_cmp''),
(5434, 1184, 1184, 2, ''timestamp_sortsupport''),
(5434, 1184, 1082, 1, ''timestamptz_cmp_date''),
(5434, 1184, 1114, 1, ''timestamptz_cmp_timestamp''),
(5436, 4402, 4402, 1, ''byteawithoutorderwithequalcolcmp''),
(5436, 4402, 4402, 2, ''bytea_sortsupport''),
(6970, 700,  700,  1, ''btfloat4cmp''),
(6970, 700,  700,  2, ''btfloat4sortsupport''),
(6970, 700,  701,  1, ''btfloat48cmp''),
(6970, 701,  701,  1, ''btfloat8cmp''),
(6970, 701,  701,  2, ''btfloat8sortsupport''),
(6970, 701,  700,  1, ''btfloat84cmp''),
(6974, 869,  869,  1, ''network_cmp''),
(6976, 21,   21,   1, ''btint2cmp''),
(6976, 21,   21,   2, ''btint2sortsupport''),
(6976, 21,   23,   1, ''btint24cmp''),
(6976, 21,   20,   1, ''btint28cmp''),
(6976, 23,   23,   1, ''btint4cmp''),
(6976, 23,   23,   2, ''btint4sortsupport''),
(6976, 23,   20,   1, ''btint48cmp''),
(6976, 23,   21,   1, ''btint42cmp''),
(6976, 20,   20,   1, ''btint8cmp''),
(6976, 20,   20,   2, ''btint8sortsupport''),
(6976, 20,   23,   1, ''btint84cmp''),
(6976, 20,   21,   1, ''btint82cmp''),
(6982, 1186, 1186, 1, ''interval_cmp''),
(6984, 829,  829,  1, ''macaddr_cmp''),
(6986, 19,   19,   1, ''btnamecmp''),
(6986, 19,   19,   2, ''btnamesortsupport''),
(6988, 1700, 1700, 1, ''numeric_cmp''),
(6988, 1700, 1700, 2, ''numeric_sortsupport''),
(6989, 26,   26,   1, ''btoidcmp''),
(6989, 26,   26,   2, ''btoidsortsupport''),
(6991, 30,   30,   1, ''btoidvectorcmp''),
(7994, 2249, 2249, 1, ''btrecordcmp''),
(6994, 25,   25,   1, ''bttextcmp''),
(6994, 25,   25,   2, ''bttextsortsupport''),
(6996, 1083, 1083, 1, ''time_cmp''),
(7000, 1266, 1266, 1, ''timetz_cmp''),
(7002, 1562, 1562, 1, ''varbitcmp''),
(7095, 25,   25,   1, ''bttext_pattern_cmp''),
(7097, 1042, 1042, 1, ''btbpchar_pattern_cmp''),
(7099, 790,  790,  1, ''cash_cmp''),
(7233, 703,  703,  1, ''btreltimecmp''),
(7234, 704,  704,  1, ''bttintervalcmp''),
(7789, 27,   27,   1, ''bttidcmp''),
(7968, 2950, 2950, 1, ''uuid_cmp''),
(8522, 3500, 3500, 1, ''enum_cmp''),
(8806, 86,   86,   1, ''rawcmp''),
(9535, 5545, 5545, 1, ''int1cmp''),
(9570, 9003, 9003, 1, ''smalldatetime_cmp''),
(8901, 3831, 3831, 1, ''range_cmp''),
(8626, 3614, 3614, 1, ''tsvector_cmp''),
(8683, 3615, 3615, 1, ''tsquery_cmp'')';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Insert_pg_amproc_temp();
DROP FUNCTION Insert_pg_amproc_temp();

/* openGauss insert row into table pg_opclass */
CREATE OR REPLACE FUNCTION Insert_pg_opclass_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opclass values 
(4439, ''abstime_ops'',                      11, 10, 5421, 702,  ''t'',0),
(4439, ''array_ops'',                        11, 10, 5397, 2277, ''t'',0),
(4439, ''bit_ops'',                          11, 10, 5423, 1560, ''t'',0),
(4439, ''bool_ops'',                         11, 10, 5424, 16,   ''t'',0),
(4439, ''bpchar_ops'',                       11, 10, 5426, 1042, ''t'',0),
(4439, ''bytea_ops'',                        11, 10, 5428, 17,   ''t'',0),
(4439, ''char_ops'',                         11, 10, 5429, 18,   ''t'',0),
(4439, ''cidr_ops'',                         11, 10, 6974, 869,  ''f'',0),
(4439, ''date_ops'',                         11, 10, 5434, 1082, ''t'',0),
(4439, ''float4_ops'',                       11, 10, 6970, 700,  ''t'',0),
(4439, ''float8_ops'',                       11, 10, 6970, 701,  ''t'',0),
(4439, ''inet_ops'',                         11, 10, 6974, 869,  ''t'',0),
(4439, ''int2_ops'',                         11, 10, 6976, 21,   ''t'',0),
(4439, ''int4_ops'',                         11, 10, 6976, 23,   ''t'',0),
(4439, ''int8_ops'',                         11, 10, 6976, 20,   ''t'',0),
(4439, ''interval_ops'',                     11, 10, 6982, 1186, ''t'',0),
(4439, ''macaddr_ops'',                      11, 10, 6984, 829,  ''t'',0),
(4439, ''name_ops'',                         11, 10, 6986, 19,   ''t'',5),
(4439, ''numeric_ops'',                      11, 10, 6988, 1700, ''t'',0),
(4439, ''oid_ops'',                          11, 10, 6989, 26,   ''t'',0),
(4439, ''oidvector_ops'',                    11, 10, 6991, 30,   ''t'',0),
(4439, ''record_ops'',                       11, 10, 7994, 2249, ''t'',0),
(4439, ''text_ops'',                         11, 10, 6994, 25,   ''t'',0),
(4439, ''time_ops'',                         11, 10, 6996, 1083, ''t'',0),
(4439, ''timestamptz_ops'',                  11, 10, 5434, 1184, ''t'',0),
(4439, ''timetz_ops'',                       11, 10, 7000, 1266, ''t'',0),
(4439, ''varbit_ops'',                       11, 10, 7002, 1562, ''t'',0),
(4439, ''varchar_ops'',                      11, 10, 6994, 25,   ''f'',0),
(4439, ''timestamp_ops'',                    11, 10, 5434, 1114, ''t'',0),
(4439, ''text_pattern_ops'',                 11, 10, 7095, 25,   ''f'',0),
(4439, ''varchar_pattern_ops'',              11, 10, 7095, 25,   ''f'',0),
(4439, ''bpchar_pattern_ops'',               11, 10, 7097, 1042, ''f'',0),
(4439, ''money_ops'',                        11, 10, 7099, 790,  ''t'',0),
(4439, ''tid_ops'',                          11, 10, 7789, 27,   ''t'',0),
(4439, ''reltime_ops'',                      11, 10, 7233, 703,  ''t'',0),
(4439, ''tinterval_ops'',                    11, 10, 7234, 704,  ''t'',0),
(4439, ''uuid_ops'',                         11, 10, 7968, 2950, ''t'',0),
(4439, ''enum_ops'',                         11, 10, 8522, 3500, ''t'',0),
(4439, ''tsvector_ops'',                     11, 10, 8626, 3614, ''t'',0),
(4439, ''tsquery_ops'',                      11, 10, 8683, 3615, ''t'',0),
(4439, ''range_ops'',                        11, 10, 8901, 3831, ''t'',0),
(4439, ''raw_ops'',                          11, 10, 8806, 86,   ''t'',0),
(4439, ''int1_ops'',                         11, 10, 9535, 5545, ''t'',0),
(4439, ''smalldatetime_ops'',                11, 10, 9570, 9003, ''t'',0),
(4439, ''byteawithoutorderwithequalcol_ops'',11, 10, 5436, 4402, ''t'',0)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Insert_pg_opclass_temp();
DROP FUNCTION Insert_pg_opclass_temp();

/* distribute insert row into table pg_opfamily */
/* 1 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''abstime_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5421;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 2 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''array_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5397;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 3 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''bit_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5423;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 4 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''bool_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5424;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 5 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''bpchar_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5426;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 6 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''bytea_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5428;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 7 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''byteawithoutorderwithequalcol_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5436;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 8 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''char_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5429;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 9 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''datetime_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 5434;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 10 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''float_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6970;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 11 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''network_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6974;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 12 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''integer_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6976;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 13 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''interval_ops'',11,10)';
EXECUTE(query_str);
return;
END;  $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6982;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 14 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''macaddr_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6984;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 15 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''name_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6986;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 16 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''numeric_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6988;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 17 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''oid_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6989;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 18 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''oidvector_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6991;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 19 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''record_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7994;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 20 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''text_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6994;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 21 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''time_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6996;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 22 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''timetz_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7000;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 23 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''varbit_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7002;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 24 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''text_pattern_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7095;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 25 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''bpchar_pattern_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7097;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 26 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''money_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7099;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 27 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''tid_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7789;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 28 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''reltime_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7233;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 29 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''tinterval_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7234;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 30 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''uuid_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 7968;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 31 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''enum_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8522;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 32 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''tsvector_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8626;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 33 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''tsquery_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8683;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 34 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''range_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8901;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 35 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''raw_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8806;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 36 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''int1_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 9535;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();

/* 37 */
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_opfamily values (4439,''smalldatetime_ops'',11,10)';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 9570;
SELECT Insert_pg_opfamily_temp();
DROP FUNCTION Insert_pg_opfamily_temp();