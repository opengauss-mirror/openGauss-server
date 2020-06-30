with oid2typename AS
(
	SELECT 
		n.nspname AS typschema, 
		t.typname AS typname,
		pg_get_userbyid(typowner) as typowner,
		t.oid AS typoid,
		typrelid
   FROM pg_type t LEFT JOIN pg_namespace n ON t.typnamespace = n.oid
   where  typoid > 16384 and typcategory  = 'U'
)

select
	typschema,
	typname,
	typowner,
	typoid,
	typrelid
from oid2typename;