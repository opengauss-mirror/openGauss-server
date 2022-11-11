DROP FUNCTION IF EXISTS pg_catalog.pgsysconf() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2371;
CREATE FUNCTION
pg_catalog.pgsysconf(OUT os_page_size   bigint,
          OUT os_pages_free  bigint,
          OUT os_total_pages bigint)
RETURNS record LANGUAGE INTERNAL VOLATILE
AS 'pgfincore';
COMMENT ON FUNCTION pg_catalog.pgsysconf()
IS 'Get system configuration information at run time, man 3 sysconf for details';


DROP FUNCTION IF EXISTS pg_catalog.pgsysconf_pretty() CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2534;
CREATE FUNCTION
pg_catalog.pgsysconf_pretty(OUT os_page_size   text,
                 OUT os_pages_free  text,
                 OUT os_total_pages text)
RETURNS record LANGUAGE SQL VOLATILE
AS '
select pg_catalog.pg_size_pretty(os_page_size)                  as os_page_size,
       pg_catalog.pg_size_pretty(os_pages_free * os_page_size)  as os_pages_free,
       pg_catalog.pg_size_pretty(os_total_pages * os_page_size) as os_total_pages
from pgsysconf()'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise(regclass, text, int) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2372;
CREATE OR REPLACE FUNCTION
pg_catalog.pgfadvise(IN regclass, IN text, IN int,
		  OUT relpath text,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT os_pages_free bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE
AS 'pgfadvise'
;
COMMENT ON FUNCTION pg_catalog.pgfadvise(regclass, text, int)
IS 'Predeclare an access pattern for file data';


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_willneed(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2535;
CREATE FUNCTION
pg_catalog.pgfadvise_willneed(IN regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 10)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_dontneed(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2536;
CREATE FUNCTION
pg_catalog.pgfadvise_dontneed(IN regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 20)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_normal(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2537;
CREATE FUNCTION
pg_catalog.pgfadvise_normal(IN regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 30)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_sequential(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2538;
CREATE FUNCTION
pg_catalog.pgfadvise_sequential(IN regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 40)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_random(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2542;
CREATE FUNCTION
pg_catalog.pgfadvise_random(IN regclass,
				   OUT relpath text,
				   OUT os_page_size bigint,
				   OUT rel_os_pages bigint,
				   OUT os_pages_free bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise($1, ''main'', 50)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, text, char, text, int, bool, bool, varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2373;
CREATE FUNCTION
pg_catalog.pgfadvise_loader(IN regclass, IN text, IN char, IN text, IN int, IN bool, IN bool, IN varbit,
				 OUT relpath text,
				 OUT os_page_size bigint,
				 OUT os_pages_free bigint,
				 OUT pages_loaded bigint,
				 OUT pages_unloaded bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE
AS 'pgfadvise_loader'
;
COMMENT ON FUNCTION pg_catalog.pgfadvise_loader(regclass, text, char, text, int, bool, bool, varbit)
IS 'Restore cache from the snapshot, options to load/unload each block to/from cache';


DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, char, text, int, bool, bool, varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2539;
CREATE FUNCTION
pg_catalog.pgfadvise_loader(IN regclass, IN char, IN text, IN int, IN bool, IN bool, IN varbit,
				 OUT relpath text,
				 OUT os_page_size bigint,
				 OUT os_pages_free bigint,
				 OUT pages_loaded bigint,
				 OUT pages_unloaded bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT pg_catalog.pgfadvise_loader($1, ''main'', $2, $3, $4, $5, $6, $7)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, text, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2374;
CREATE FUNCTION
pg_catalog.pgfincore(IN regclass, IN text, IN bool,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE INTERNAL VOLATILE
AS 'pgfincore'
;
COMMENT ON FUNCTION pg_catalog.pgfincore(regclass, text, bool)
IS 'Utility to inspect and get a snapshot of the system cache';


DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2541;
CREATE FUNCTION
pg_catalog.pgfincore(IN regclass,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT * from pg_catalog.pgfincore($1, ''main'', false)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, bool) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2540;
CREATE FUNCTION
pg_catalog.pgfincore(IN regclass, IN bool,
		  OUT relpath text,
		  OUT segment int,
		  OUT os_page_size bigint,
		  OUT rel_os_pages bigint,
		  OUT pages_mem bigint,
		  OUT group_mem bigint,
		  OUT os_pages_free bigint,
		  OUT databit      varbit,
		  OUT pages_dirty bigint,
		  OUT group_dirty bigint)
RETURNS setof record LANGUAGE SQL VOLATILE
AS 'SELECT * from pg_catalog.pgfincore($1, ''main'', $2)'
;


DROP FUNCTION IF EXISTS pg_catalog.pgfincore_drawer(varbit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 2543;
CREATE FUNCTION
pg_catalog.pgfincore_drawer(IN varbit,
		  OUT drawer cstring)
RETURNS cstring LANGUAGE INTERNAL IMMUTABLE
AS 'pgfincore_drawer'
;
COMMENT ON FUNCTION pg_catalog.pgfincore_drawer(varbit)
IS 'A naive drawing function to visualize page cache per object';
