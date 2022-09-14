DROP FUNCTION IF EXISTS pg_catalog.pgsysconf_pretty() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgsysconf() CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_willneed(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_dontneed(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_normal(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_sequential(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_random(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise(regclass, text, int) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, text, char, text, int, bool, bool, varbit) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfadvise_loader(regclass, char, text, int, bool, bool, varbit) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, bool) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfincore(regclass, text, bool) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.pgfincore_drawer(varbit) CASCADE;


