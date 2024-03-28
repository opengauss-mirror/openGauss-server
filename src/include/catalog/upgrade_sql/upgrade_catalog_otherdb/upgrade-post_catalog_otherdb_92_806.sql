DO $upgrade$
BEGIN
IF working_version_num() < 92780 then
--sha
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 558;
CREATE OR REPLACE FUNCTION pg_catalog.sha(text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$sha$function$;
comment on function PG_CATALOG.sha(text) is 'use the sha1 algorithm to hash';
 
-- sha1
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 559;
CREATE OR REPLACE FUNCTION pg_catalog.sha1(text)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$sha1$function$;
comment on function PG_CATALOG.sha1(text) is 'use the sha1 algorithm to hash';
 
-- sha2
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 560;
DROP FUNCTION IF EXISTS pg_catalog.sha2(text, bigint) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.sha2(text, int) CASCADE;
CREATE OR REPLACE FUNCTION pg_catalog.sha2(text, bigint)
 RETURNS text
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$sha2$function$;
comment on function PG_CATALOG.sha2(text,bigint) is 'use the sha2 algorithm to hash';
 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
END IF;
END $upgrade$;
