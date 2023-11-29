-- ----------------------------------------------------------------
-- upgrade pg_catalog.pg_conversion 
-- ----------------------------------------------------------------
 
DROP FUNCTION IF EXISTS pg_catalog.gb18030_2022_to_utf8(integer, integer, cstring, internal, integer) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.gb18030_2022_to_utf8(integer, integer, cstring, internal, integer)
RETURNS void
LANGUAGE c
STRICT NOT FENCED NOT SHIPPABLE
AS '$libdir/utf8_and_gb18030', $function$gb18030_2022_to_utf8$function$;
COMMENT ON FUNCTION pg_catalog.gb18030_2022_to_utf8(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER)
IS 'internal conversion function for GB18030_2022 to UTF8';
 
insert into pg_catalog.pg_conversion values ('gb18030_2022_to_utf8', 11, 10, 37, 7, 'gb18030_2022_to_utf8', true);
 
DROP FUNCTION IF EXISTS pg_catalog.utf8_to_gb18030_2022(integer, integer, cstring, internal, integer) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.utf8_to_gb18030_2022(integer, integer, cstring, internal, integer)
RETURNS void
LANGUAGE c
STRICT NOT FENCED NOT SHIPPABLE
AS '$libdir/utf8_and_gb18030', $function$utf8_to_gb18030_2022$function$;
COMMENT ON FUNCTION pg_catalog.utf8_to_gb18030_2022(INTEGER, INTEGER, CSTRING, INTERNAL, INTEGER)
IS 'internal conversion function for UTF8 to GB18030_2022';
 
insert into pg_catalog.pg_conversion values ('utf8_to_gb18030_2022', 11, 10, 7, 37, 'utf8_to_gb18030_2022', true);
 
UPDATE pg_catalog.pg_conversion SET conforencoding=38 WHERE conname like 'sjis_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=38 WHERE conname like '%_to_sjis';
UPDATE pg_catalog.pg_conversion SET conforencoding=39 WHERE conname like 'big5_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=39 WHERE conname like '%_to_big5';
UPDATE pg_catalog.pg_conversion SET conforencoding=40 WHERE conname like 'uhc_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=40 WHERE conname like '%_to_uhc';
UPDATE pg_catalog.pg_conversion SET conforencoding=41 WHERE conname like 'johab_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=41 WHERE conname like '%to_johab';
UPDATE pg_catalog.pg_conversion SET conforencoding=42 WHERE conname like 'shift_jis_2004_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=42 WHERE conname like '%to_shift_jis_2004';