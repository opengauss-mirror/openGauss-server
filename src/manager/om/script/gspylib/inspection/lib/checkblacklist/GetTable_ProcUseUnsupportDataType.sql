with oid2relname AS
(
	SELECT 
		n.nspname AS schemaname, 
		c.relname AS tablename,
		pg_get_userbyid(c.relowner) as relowner,
		c.oid AS relid
   FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
)
,
oid2typename AS
(
	SELECT 
		n.nspname AS typschema, 
		t.typname AS typname,
		pg_get_userbyid(typowner) as typowner,
		t.oid AS typoid
   FROM pg_type t LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
)

select
	schemaname,
	tablename,
	relowner,
	typschema,
	typname,
	typowner
from pg_attribute 
inner join oid2relname on (attrelid = relid)
inner join oid2typename on (typoid = atttypid)
where (atttypid in (628, 629, 142, 194) or (typname like '%reg%') or atttypid > 16384)
and attrelid > 16384
;

with oid2typename AS
(
	SELECT 
		n.nspname AS typschema, 
		t.typname AS typname,
		pg_get_userbyid(typowner) as typowner,
		t.oid AS typoid
   FROM pg_type t LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
)

select
	n.nspname AS proschema,
	p.proname AS proname,
	pg_get_userbyid(proowner) as proowner,
	typschema,
	typname,
	typowner
from pg_proc p
INNER JOIN pg_namespace n ON n.oid = p.pronamespace
INNER JOIN oid2typename ON (typoid = prorettype or typoid = any(proargtypes))
where (typoid in (628, 629, 142, 194) or (typname like '%reg%') or typoid > 16384)
and p.oid > 16384
;
