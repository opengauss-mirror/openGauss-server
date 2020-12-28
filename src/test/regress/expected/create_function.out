create function create_function_test(integer,integer) RETURNS integer
AS 'SELECT $1 + $2;'
LANGUAGE SQL
IMMUTABLE SHIPPABLE
RETURNS NULL ON NULL INPUT;
drop function create_function_test;
