CREATE OR REPLACE FUNCTION Update_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'update pg_catalog.pg_am set amsupport = 3, amhandler = 0 where amname = ''btree'' or amname = ''ubtree''';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Update_pg_amproc_temp();
DROP FUNCTION Update_pg_amproc_temp();

DROP FUNCTION IF EXISTS pg_catalog.btvarstrequalimage(anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4608;
CREATE FUNCTION pg_catalog.btvarstrequalimage(Oid) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE AS 'btvarstrequalimage';

DROP FUNCTION IF EXISTS pg_catalog.btequalimage(anyelement) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 4609;
CREATE FUNCTION pg_catalog.btequalimage(Oid) RETURNS boolean LANGUAGE INTERNAL IMMUTABLE AS 'btequalimage';

CREATE OR REPLACE FUNCTION Insert_pg_amproc_temp()
RETURNS void AS $$
DECLARE
query_str text;
BEGIN
query_str := 'insert into pg_catalog.pg_amproc values
(423, 1560, 1560, 3, ''btequalimage''),
(424, 16, 16, 3, ''btequalimage''),
(426, 1042, 1042, 3, ''btvarstrequalimage''),
(428, 17, 17, 3, ''btequalimage''),
(429, 18, 18, 3, ''btequalimage''),
(434, 1082, 1082, 3, ''btequalimage''),
(434, 1114, 1114, 3, ''btequalimage''),
(434, 1184, 1184, 3, ''btequalimage''),
(1974, 869, 869, 3, ''btequalimage''),
(1976, 21, 21, 3, ''btequalimage''),
(1976, 23, 23, 3, ''btequalimage''),
(1976, 20, 20, 3, ''btequalimage''),
(1982, 1186, 1186, 3, ''btequalimage''),
(1984, 829, 829, 3, ''btequalimage''),
(1986, 19, 19, 3, ''btvarstrequalimage''),
(1989, 26, 26, 3, ''btequalimage''),
(1991, 30, 30, 3, ''btequalimage''),
(1994, 25, 25, 3, ''btvarstrequalimage''),
(1996, 1083, 1083, 3, ''btequalimage''),
(2000, 1266, 1266, 3, ''btequalimage''),
(2002, 1562, 1562, 3, ''btequalimage''),
(2095, 25, 25, 3, ''btequalimage''),
(2097, 1042, 1042, 3, ''btequalimage''),
(2099, 790, 790, 3, ''btequalimage''),
(2789, 27, 27, 3, ''btequalimage''),
(2968, 2950, 2950, 3, ''btequalimage''),
(5423, 1560, 1560, 3, ''btequalimage''),
(5424, 16, 16, 3, ''btequalimage''),
(5426, 1042, 1042, 3, ''btvarstrequalimage''),
(5428, 17, 17, 3, ''btequalimage''),
(5429, 18, 18, 3, ''btequalimage''),
(5434, 1082, 1082, 3, ''btequalimage''),
(5434, 1114, 1114, 3, ''btequalimage''),
(5434, 1184, 1184, 3, ''btequalimage''),
(6974, 869, 869, 3, ''btequalimage''),
(6976, 21, 21, 3, ''btequalimage''),
(6976, 23, 23, 3, ''btequalimage''),
(6976, 20, 20, 3, ''btequalimage''),
(6982, 1186, 1186, 3, ''btequalimage''),
(6984, 829, 829, 3, ''btequalimage''),
(6986, 19, 19, 3, ''btvarstrequalimage''),
(6989, 26, 26, 3, ''btequalimage''),
(6991, 30, 30, 3, ''btequalimage''),
(6994, 25, 25, 3, ''btvarstrequalimage''),
(6996, 1083, 1083, 3, ''btequalimage''),
(7000, 1266, 1266, 3, ''btequalimage''),
(7002, 1562, 1562, 3, ''btequalimage''),
(7095, 25, 25, 3, ''btequalimage''),
(7097, 1042, 1042, 3, ''btequalimage''),
(7099, 790, 790, 3, ''btequalimage''),
(7789, 27, 27, 3, ''btequalimage''),
(7968, 2950, 2950, 3, ''btequalimage'')
';
EXECUTE(query_str);
return;
END; $$ LANGUAGE 'plpgsql';

SELECT Insert_pg_amproc_temp();
DROP FUNCTION Insert_pg_amproc_temp();