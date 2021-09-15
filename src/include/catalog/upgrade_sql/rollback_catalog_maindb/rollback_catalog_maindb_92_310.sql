-- --------------------------------------------------------------
-- rollback pg_catalog.pg_conversion
-- --------------------------------------------------------------
UPDATE pg_catalog.pg_conversion SET conforencoding=39 WHERE conname like 'gb18030_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=39 WHERE conname like '%_to_gb18030';

UPDATE pg_catalog.pg_conversion SET conforencoding=36 WHERE conname like 'shis_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=36 WHERE conname like '%_to_sjis';

UPDATE pg_catalog.pg_conversion SET conforencoding=37 WHERE conname like 'big5_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=37 WHERE conname like '%_to_big5';

UPDATE pg_catalog.pg_conversion SET conforencoding=38 WHERE conname like 'uhc_to_%';
UPDATE pg_catalog.pg_conversion SET contoencoding=38 WHERE conname like '%_to_uhc';