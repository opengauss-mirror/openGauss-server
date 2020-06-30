select 
	schemaname, 
	tablename, 
	indexname 
from pg_indexes where indexdef not like '%btree%' and indexdef not like '%psort%';