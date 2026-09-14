--
-- Check deparsing of a RECORD field when its subquery plan was optimized
-- away as a proven-dummy relation (PostgreSQL bug #18576).
--
\pset format unaligned
explain (verbose, costs off)
select ordinal_position
from information_schema.parameters
where specific_name = null;

explain performance
select ordinal_position
from information_schema.parameters
where specific_name = null;
