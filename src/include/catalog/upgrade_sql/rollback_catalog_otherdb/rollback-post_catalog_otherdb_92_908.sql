-- ----------------------------------------------------------------
-- roolback pg_catalog.pg_conversion 
-- ----------------------------------------------------------------
 
delete from pg_catalog.pg_conversion where conname = 'gb18030_2022_to_utf8';
delete from pg_catalog.pg_conversion where conname = 'utf8_to_gb18030_2022';
DROP FUNCTION IF EXISTS pg_catalog.gb18030_2022_to_utf8(integer, integer, cstring, internal, integer) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.utf8_to_gb18030_2022(integer, integer, cstring, internal, integer) CASCADE;
 
UPDATE pg_catalog.pg_conversion SET conforencoding=37 WHERE conname like 'sjis_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=37 WHERE conname like '%_to_sjis';
UPDATE pg_catalog.pg_conversion SET conforencoding=38 WHERE conname like 'big5_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=38 WHERE conname like '%_to_big5';
UPDATE pg_catalog.pg_conversion SET conforencoding=39 WHERE conname like 'uhc_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=39 WHERE conname like '%_to_uhc';
UPDATE pg_catalog.pg_conversion SET conforencoding=40 WHERE conname like 'johab_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=40 WHERE conname like '%to_johab';
UPDATE pg_catalog.pg_conversion SET conforencoding=41 WHERE conname like 'shift_jis_2004_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=41 WHERE conname like '%to_shift_jis_2004';