--
--@@GaussDB@@
--plsql_packages test
--

/* BEGIN DBMS_SQL PACKAGE */

create table t1(c1 int, c2 int, c3 int);
create table t2(c1 int, c2 text, c3 int);
create table t3(c1 int, c2 char(20), c3 int);
create table t4(c1 int, c2 varchar(20), c3 int);
create table t5(c1 bigint, c2 char(20), c3 bytea);
insert into t1 select v,v,v from generate_series(101,110) as v;
update t1 set c2 = NULL where c1 = 107; 
insert into t2 values(1,'AAAAA', 1);
insert into t2 values(2,'BBBBB', 2);
insert into t2 values(3,'CCCCC', 3);
insert into t2 values(4,'DDDDD', 4);
insert into t2 values(5,'EEEEE', 5);
insert into t2 values(NULL,'FFFFF', 6);
insert into t2 values(7,NULL, 7);

insert into t3 select * from t2;
insert into t4 select * from t2;
insert into t5 values(1,'AAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAA');
insert into t5 values(2,'BBBBB', 'BBBBBBBBBBBBBBBBBBBBBBBB');
insert into t5 values(3,'CCCCC', 'CCCCCCCCCCCCCCCCCCCCCCCC');
insert into t5 values(4,'DDDDD', 'DDDDDDDDDDDDDDDDDDDDDDDD');
insert into t5 values(5,'EEEEE', 'EEEEEEEEEEEEEEEEEEEEEEEE');
/* verify the DBMS_SQL library routines */
select proname, prorettype, proargtypes, fencedmode 
from pg_proc 
where pronamespace in (
	select oid
	from pg_namespace
	where nspname = 'dbms_sql') 
order by 1,2,3,4;

CREATE or REPLACE FUNCTION test_dbmssql_int()
RETURNS text
AS $$
DECLARE
    cursorid int;
	query text;
	define_column_ret int;
	parse_ret int;
	execute_ret int;
	fetch_rows_ret int;
	column_values_ret int;
	close_cursor_ret int;
	c1   int;
	c2   int;
	c1value int;
	c2value int;
	final_result text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	-- step1 open cursor 
	cursorid := dbms_sql.open_cursor();
	/* raise notice 'open_cursor() cursorid: %', cursorid;*/
	
	-- step2 parse
	parse_ret := dbms_sql.parse(cursorid, query, 1);
	/*raise notice 'parse() %', parse_ret;*/

	-- step3 define column
	define_column_ret := dbms_sql.define_column(cursorid, 1, c1);
	define_column_ret := dbms_sql.define_column(cursorid, 2, c2);
	/*raise notice 'define_column() define_column_ret: %', define_column_ret;*/
	
	-- step3 execute
	execute_ret := dbms_sql.execute(cursorid);
	/*raise notice 'execute() ret %', execute_ret;*/
	
	-- step 4
	final_result := '{';
	LOOP
        EXIT WHEN (dbms_sql.fetch_rows(cursorid) <= 0);
			dbms_sql.column_value(cursorid, 1, c1value);
			dbms_sql.column_value(cursorid, 2, c2value);
			final_result := final_result || '(' || c1value || ', ' || c2value || ')'; 
	END LOOP;
	final_result := final_result ||'}';

	-- step 5
	close_cursor_ret := dbms_sql.close_cursor(cursorid);
	/*raise notice 'close_cursor() ret %', close_cursor_ret;*/
	return final_result;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE or REPLACE FUNCTION test_dbmssql_text()
RETURNS text
AS $$
DECLARE
    cursorid int;
	query text;
	define_column_ret int;
	parse_ret int;
	execute_ret int;
	fetch_rows_ret int;
	column_values_ret int;
	close_cursor_ret int;
	c1   int;
	c2   text;
	c1value int;
	c2value text;
	final_result text;
BEGIN
	query := 'select c1,c2 from t2 order by 1;';

	-- step1 open cursor 
	cursorid := dbms_sql.open_cursor();
	--raise notice 'open_cursor() cursorid: %', cursorid;
	
	-- step2 parse
	parse_ret := dbms_sql.parse(cursorid, query, 1);
	--raise notice 'parse() %', parse_ret;

	-- step3 define column
	define_column_ret := dbms_sql.define_column(cursorid, 1, c1);
	define_column_ret := dbms_sql.define_column(cursorid, 2, c2, 200);
	--raise notice 'define_column() define_column_ret: %', define_column_ret;
	
	-- step3 execute
	execute_ret := dbms_sql.execute(cursorid);
	--raise notice 'execute() ret %', execute_ret;
	
	-- step 4
	final_result := '{';
	LOOP
        EXIT WHEN (dbms_sql.fetch_rows(cursorid) <= 0);
			dbms_sql.column_value(cursorid, 1, c1value);
			dbms_sql.column_value(cursorid, 2, c2value);
			final_result := final_result || '(' || c1value || ', ' || c2value || ')'; 
	END LOOP;
	final_result := final_result ||'}';

	-- step 5
	close_cursor_ret := dbms_sql.close_cursor(cursorid);
	--raise notice 'close_cursor() ret %', close_cursor_ret;
	return final_result;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE or REPLACE FUNCTION test_dbmssql_varchar()
RETURNS text
AS $$
DECLARE
    cursorid int;
	query text;
	define_column_ret int;
	parse_ret int;
	execute_ret int;
	fetch_rows_ret int;
	column_values_ret int;
	close_cursor_ret int;
	c1   int;
	c2   varchar;
	c1value int;
	c2value varchar;
	final_result text;
BEGIN
	query := 'select c1,c2 from t4 order by 1;';

	-- step1 open cursor 
	cursorid := dbms_sql.open_cursor();
	--raise notice 'open_cursor() cursorid: %', cursorid;
	
	-- step2 parse
	parse_ret := dbms_sql.parse(cursorid, query, 1);
	--raise notice 'parse() %', parse_ret;

	-- step3 define column
	define_column_ret := dbms_sql.define_column(cursorid, 1, c1);
	define_column_ret := dbms_sql.define_column(cursorid, 2, c2, 200);
	--raise notice 'define_column() define_column_ret: %', define_column_ret;
	
	-- step3 execute
	execute_ret := dbms_sql.execute(cursorid);
	--raise notice 'execute() ret %', execute_ret;
	
	-- step 4
	final_result := '{';
	LOOP
        EXIT WHEN (dbms_sql.fetch_rows(cursorid) <= 0);
			c1value := dbms_sql.column_value(cursorid, 1, c1);
			c2value := dbms_sql.column_value(cursorid, 2, c2);
			final_result := final_result || '(' || c1value || ', ' || c2value || ')'; 
	END LOOP;
	final_result := final_result ||'}';

	-- step 5
	close_cursor_ret := dbms_sql.close_cursor(cursorid);
	--raise notice 'close_cursor() ret %', close_cursor_ret;
	return final_result;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE or REPLACE FUNCTION test_dbmssql_char()
RETURNS text
AS $$
DECLARE
    cursorid int;
	query text;
	define_column_ret int;
	parse_ret int;
	execute_ret int;
	fetch_rows_ret int;
	column_values_ret int;
	close_cursor_ret int;
	c1   int;
	c2   char(20);
	c1value int;
	c2value char(20);
	c3value char(20);
	err numeric;
	act_len int;
	value text;
	final_result text;
BEGIN
	query := 'select c1,c2 from t3 order by 1;';
	act_len := 3;
	-- step1 open cursor 
	cursorid := dbms_sql.open_cursor();
	--raise notice 'open_cursor() cursorid: %', cursorid;
	
	-- step2 parse
	parse_ret := dbms_sql.parse(cursorid, query, 1);
	--raise notice 'parse() %', parse_ret;

	-- step3 define column
	define_column_ret := dbms_sql.define_column(cursorid, 1, c1);
	define_column_ret := dbms_sql.define_column(cursorid, 2, c2, 200);
	define_column_ret := dbms_sql.define_column(cursorid, 2, c2, 2);
	raise notice 'define_column() define_column_ret: %', define_column_ret;
	
	-- step3 execute
	execute_ret := dbms_sql.execute(cursorid);
	--raise notice 'execute() ret %', execute_ret;
	
	-- step 4
	final_result := '{';
	LOOP
        EXIT WHEN (dbms_sql.fetch_rows(cursorid) <= 0);
			c1value := dbms_sql.column_value(cursorid, 1, c1);
			--raise notice 'column_value_char() c1value: %', c1value;
			dbms_sql.column_value_char(cursorid, 2, c2value);
			--raise notice 'column_value_char() c2value: %', value;
			dbms_sql.column_value_char(cursorid, 2, c3value, err, act_len);
			--raise notice 'column_value_char() c3value: %', value;
			final_result := final_result || '(' || c1value || ', ' || c2value || ',' || c3value || ')';
	END LOOP;
	final_result := final_result ||'}';

	-- step 5
	close_cursor_ret := dbms_sql.close_cursor(cursorid);
	--raise notice 'close_cursor() ret %', close_cursor_ret;
	return final_result;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

CREATE or REPLACE FUNCTION test_dbmssql_bytea_long()
RETURNS text
AS $$
DECLARE
    cursorid int;
	query text;
	define_column_ret int;
	parse_ret int;
	execute_ret int;
	fetch_rows_ret int;
	column_values_ret int;
	close_cursor_ret int;
	value_length int;
	c1   bigint;
	c2   char(20);
	c3	 bytea;
	value1 text;
	c2value text;
	c3value bytea;
	final_result text;
BEGIN
	query := 'select c2,c3 from t5 order by 1;';
	value_length := 2;
	-- step1 open cursor 
	cursorid := dbms_sql.open_cursor();
	--raise notice 'open_cursor() cursorid: %', cursorid;
	
	-- step2 parse
	parse_ret := dbms_sql.parse(cursorid, query, 1);
	--raise notice 'parse() %', parse_ret;

	-- step3 define column
	define_column_ret := dbms_sql.define_column_long(cursorid, 1);
	--raise notice 'define_column2() define_column_ret: %', define_column_ret;
	define_column_ret := dbms_sql.define_column(cursorid, 2, c3);
	--raise notice 'define_column() define_column_ret: %', define_column_ret;
	
	-- step3 execute
	execute_ret := dbms_sql.execute(cursorid);
	--raise notice 'execute() ret %', execute_ret;
	
	-- step 4
	final_result := '{';
	LOOP
        EXIT WHEN (dbms_sql.fetch_rows(cursorid) <= 0);
			dbms_sql.column_value_long(cursorid, 1, 3, 1, c2value, value_length);
			--raise notice 'column_value_long() value: %', value1;
			dbms_sql.column_value_raw(cursorid, 2, c3value,0,5);
			--raise notice 'column_value_raw() c3value: %', c3value;
			final_result := final_result || '(' || c2value || ', ' || c3value || ')'; 
	END LOOP;
	final_result := final_result ||'}';
	
	-- step 5 
	close_cursor_ret := dbms_sql.close_cursor(cursorid);
	--raise notice 'close_cursor() ret %', close_cursor_ret;
	return final_result;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select test_dbmssql_int();
select test_dbmssql_text();
select test_dbmssql_char();
select test_dbmssql_varchar();
select test_dbmssql_bytea_long();

drop function test_dbmssql_int;
drop function test_dbmssql_text;
drop function test_dbmssql_char;
drop function test_dbmssql_varchar;
drop function test_dbmssql_bytea_long();

/*
 * --------------------------------------------------------------------------------
 * Exceptional test
 * --------------------------------------------------------------------------------
 */
-- test1: verify that cursor is not found
-- test1.1 cursorid is not found in parse()
CREATE or REPLACE FUNCTION dbmssql_exception_test1()
RETURNS int AS $$
DECLARE
    cursorid int;
    query text;
BEGIN
    query := 'select c1,c2 from t3 order by 1;';
    cursorid := dbms_sql.open_cursor();
	-- invalid request
    dbms_sql.parse(cursorid + 1, query, 1);
	
	dbms_sql.close_cursor(cursorid);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test1();
drop function dbmssql_exception_test1;

-- test1.2 cursorid is not found in execute()
CREATE or REPLACE FUNCTION dbmssql_exception_test2()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
    query text;
BEGIN
	query := 'select c1,c2 from t3 order by 1;';

	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid, query, 1);

	-- invalid request
    retval := dbms_sql.execute(cursorid + 1);

	dbms_sql.close_cursor(cursorid);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test2();
drop function dbmssql_exception_test2;

-- test1.3 cursorid not found in define_column
CREATE or REPLACE FUNCTION dbmssql_exception_test3()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid, query, 1);

	retval := dbms_sql.define_column(cursorid, 1, c1);
	-- invalid request
    retval := dbms_sql.execute(cursorid + 1);

	dbms_sql.close_cursor(cursorid);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test3();
drop function dbmssql_exception_test3;

-- test1.4 cursorid not found in fetch_rows
CREATE or REPLACE FUNCTION dbmssql_exception_test4()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid, query, 1);

	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	-- invalid request
	retval := dbms_sql.fetch_rows(cursorid+1);

	dbms_sql.close_cursor(cursorid);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test4();
drop function dbmssql_exception_test4;


-- test1.5 cursorid not found in column_value
CREATE or REPLACE FUNCTION dbmssql_exception_test5()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid, query, 1);

	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	-- invalid request
	retval := dbms_sql.column_value(cursorid+1, 1, c1);

	dbms_sql.close_cursor(cursorid);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test5();
drop function dbmssql_exception_test5;


-- test1.6 cursorid not found in close_cursor
create table tx(c1 int, c2 int);
CREATE or REPLACE FUNCTION dbmssql_exception_test6()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from tx order by 1;';

	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid, query, 1);

	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	retval := dbms_sql.column_value(cursorid, 1, c1);
	-- invalid request
	dbms_sql.close_cursor(cursorid+1);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;
select dbmssql_exception_test6();
insert into tx values(1,1);
insert into tx values(2,2);
select dbmssql_exception_test6();
drop table tx;
drop function dbmssql_exception_test6;

-- test1.7
CREATE or REPLACE FUNCTION dbmssql_exception_test7()
RETURNS int AS $$
DECLARE
    cursorid int;
    retval  int;
    c1 int;
    c2 int;
    query text;
BEGIN
    query := 'select c1,c2 from tx_not_exist order by 1;';

    cursorid := dbms_sql.open_cursor();
    dbms_sql.parse(cursorid, query, 1);

    retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
    retval := dbms_sql.fetch_rows(cursorid);
    retval := dbms_sql.column_value(cursorid, 1, c1);
    -- invalid request
    dbms_sql.close_cursor(cursorid+1);
    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

--Call twice to test if there is un cleaned cursor context
select dbmssql_exception_test7();
select dbmssql_exception_test7();
drop function dbmssql_exception_test7;

-- test2: verify that use is not following regular parse() execute() define_column() process to run dbms_sql
-- test2.1 run define_column without parse()
CREATE or REPLACE FUNCTION dbmssql_exception_test21()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	--retval := dbms_sql.parse(cursorid, query, 1);
	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	retval := dbms_sql.column_value(cursorid, 1, c1);
	dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test21();
drop function dbmssql_exception_test21;

-- test2.2 run execute without parse()
CREATE or REPLACE FUNCTION dbmssql_exception_test22()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	--retval := dbms_sql.parse(cursorid, query, 1);
	--retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	retval := dbms_sql.column_value(cursorid, 1, c1);
	dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test22();
drop function dbmssql_exception_test22;

-- test2.3 run fetch_rows without execute()
CREATE or REPLACE FUNCTION dbmssql_exception_test23()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	retval := dbms_sql.parse(cursorid, query, 1);
	retval := dbms_sql.define_column(cursorid, 1, c1);
    --retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	retval := dbms_sql.column_value(cursorid, 1, c1);
	dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test23();
drop function dbmssql_exception_test23;

-- test2.4 run column_value without fetch_rows
CREATE or REPLACE FUNCTION dbmssql_exception_test24()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	retval := dbms_sql.parse(cursorid, query, 1);
	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	--retval := dbms_sql.fetch_rows(cursorid);
	retval := dbms_sql.column_value(cursorid, 1, c1);
	dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test24();
drop function dbmssql_exception_test24;

-- test2.5 double close same cursorid
CREATE or REPLACE FUNCTION dbmssql_exception_test25()
RETURNS int AS $$
DECLARE
    cursorid int;
	retval  int;
	c1 int;
	c2 int;
    query text;
BEGIN
	query := 'select c1,c2 from t1 order by 1;';

	cursorid := dbms_sql.open_cursor();
	retval := dbms_sql.parse(cursorid, query, 1);
	retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
	retval := dbms_sql.fetch_rows(cursorid);
	--retval := dbms_sql.column_value(cursorid, 1, c1);
	dbms_sql.close_cursor(cursorid);
	dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test25();
drop function dbmssql_exception_test25;

-- test2.6 Test define_column unspported
CREATE or REPLACE FUNCTION dbmssql_exception_test26()
RETURNS int AS $$
DECLARE
    cursorid int;
    retval  int;
    c1 int;
    c2 timestamp;
    query text;
BEGIN
    query := 'select c1,c2 from t1 order by 1;';

    cursorid := dbms_sql.open_cursor();
    retval := dbms_sql.parse(cursorid, query, 1);
    retval := dbms_sql.define_column(cursorid, 1, c2);
    retval := dbms_sql.execute(cursorid);
    retval := dbms_sql.fetch_rows(cursorid);
    --retval := dbms_sql.column_value(cursorid, 1, c1);
    dbms_sql.close_cursor(cursorid);
    dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test26();
drop function dbmssql_exception_test26;

-- test2.7 Test column_value unspported
CREATE or REPLACE FUNCTION dbmssql_exception_test27()
RETURNS int AS $$
DECLARE
    cursorid int;
    retval  int;
    c1 int;
    c2 timestamp;
    query text;
BEGIN
    query := 'select c1,c2 from t1 order by 1;';

    cursorid := dbms_sql.open_cursor();
    retval := dbms_sql.parse(cursorid, query, 1);
    retval := dbms_sql.define_column(cursorid, 1, c1);
    retval := dbms_sql.execute(cursorid);
    retval := dbms_sql.fetch_rows(cursorid);
    retval := dbms_sql.column_value(cursorid, 1, c2);
    dbms_sql.close_cursor(cursorid);
    dbms_sql.close_cursor(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test27();
drop function dbmssql_exception_test27;

-- test2.8 Test is_ipen
CREATE or REPLACE FUNCTION dbmssql_exception_test28()
RETURNS int AS $$
DECLARE
    cursorid int;
    retval  int;
	is_open boolean;
    c1 int;
    query text;
BEGIN
    query := 'select c1,c2 from t1 order by 1;';

    cursorid := dbms_sql.open_cursor();
	is_open := dbms_sql.is_open(cursorid);
    retval := dbms_sql.parse(cursorid, query, 1);
	is_open := dbms_sql.is_open(cursorid);
    retval := dbms_sql.define_column(cursorid, 1, c1);
	is_open := dbms_sql.is_open(cursorid);
    retval := dbms_sql.execute(cursorid);
	is_open := dbms_sql.is_open(cursorid);
    retval := dbms_sql.fetch_rows(cursorid);
	is_open := dbms_sql.is_open(cursorid);
    dbms_sql.close_cursor(cursorid);
	is_open := dbms_sql.is_open(cursorid);
    dbms_sql.close_cursor(cursorid);
	is_open := dbms_sql.is_open(cursorid);

    return 0;
END;
$$
LANGUAGE 'plpgsql' NOT FENCED;

select dbmssql_exception_test28();
drop function dbmssql_exception_test28;

drop table t1;
drop table t2;
drop table t3;
drop table t4;
drop table t5;

--Test Real Work
CREATE OR REPLACE FUNCTION FUNC_SPMS_FPMS_DATA ( i_query in varchar2 ,i_separator in varchar2 ,i_dir in varchar2 ,i_filename in varchar2 )
     RETURN number AUTHID CURRENT_USER is l_output utl_file.file_type ;
     l_theCursor integer default dbms_sql.open_cursor ;
     l_columnValue varchar2 ( 5000 ) ;
     l_status integer ;
     l_colCnt number default 0 ;
     l_separator varchar2 ( 10 ) default '' ;
     l_cnt number default 0 ;
BEGIN
    l_output := utl_file.fopen ( i_dir ,i_filename ,'w' ,32767 ) ;
    dbms_sql.parse ( l_theCursor ,i_query ,dbms_sql.native ) ;
    FOR i in 1..255 LOOP
        BEGIN
            dbms_sql.define_column ( l_theCursor ,i ,l_columnValue ,5000 ) ;
            l_colCnt := i ;
            exception WHEN others
            THEN
                IF ( sqlcode = - 1007 ) THEN
                     EXIT ;
                ELSE
                    raise ;
                END IF ;
            END ;
    END LOOP ;
    dbms_sql.define_column ( l_theCursor ,1 ,l_columnValue ,5000 ) ;
    l_status := dbms_sql.execute ( l_theCursor ) ;
    LOOP EXIT WHEN( dbms_sql.fetch_rows ( l_theCursor ) <= 0 ) ;
        l_separator := '' ;
        FOR i in 1.. l_colCnt LOOP
            dbms_sql.column_value ( l_theCursor ,i ,l_columnValue ) ;
            IF i = l_colCnt
            THEN
                utl_file.put ( l_output ,l_separator || l_columnValue || chr ( 13 ) ) ;
            ELSE
                utl_file.put ( l_output ,l_separator || l_columnValue ) ;
            END IF ;
            l_separator := i_separator ;
        END LOOP ;
        utl_file.new_line ( l_output ) ;
        l_cnt := l_cnt + 1 ;
    END LOOP ;
    dbms_sql.close_cursor ( l_theCursor ) ;
    utl_file.fclose ( l_output ) ;
    RETURN l_cnt ;
END;
/

DROP FUNCTION FUNC_SPMS_FPMS_DATA;

--open_cursor twice but close once
create or replace procedure pro_dbms_sql_err_open_05()
as 
declare
cursorid int;
cursorid1 int;
query varchar2(2000);
begin
cursorid := dbms_sql.open_cursor();
cursorid1 := dbms_sql.open_cursor();
dbms_sql.close_cursor(cursorid);
EXCEPTION
WHEN OTHERS THEN
DBMS_SQL.CLOSE_CURSOR(cursorid1);
end;
/

call pro_dbms_sql_err_open_05();

--param number more than required
create or replace procedure pro_dbms_sql_parse_05()
as
declare
        cursorid int;
        query varchar(2000);
        v_stat     int;
begin
        query := 'select * from dual;';
        cursorid := dbms_sql.open_cursor();
        dbms_sql.parse(cursorid,query, 1, 2);
        v_stat := dbms_sql.execute(cursorid);
        dbms_sql.close_cursor(cursorid);
        EXCEPTION
        WHEN OTHERS THEN
        DBMS_SQL.CLOSE_CURSOR(cursorid);
end;
/

call pro_dbms_sql_parse_05();

--param number not enough
create or replace procedure pro_dbms_sql_parse_05()
as 
declare
	cursorid int;
	query varchar(2000);
	v_stat     int;
begin
	query := 'select * from dual;';
	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid,query);
	v_stat := dbms_sql.execute(cursorid);
	dbms_sql.close_cursor(cursorid);
	EXCEPTION
        WHEN OTHERS THEN
        DBMS_SQL.CLOSE_CURSOR(cursorid);
end;
/

call pro_dbms_sql_parse_05();


--2th argumen is null
create or replace procedure pro_dbms_sql_parse_15()
as 
declare
	cursorid int;
	query varchar(2000);
	v_stat     int;
begin
	query := 'select * from dual;';
	cursorid := dbms_sql.open_cursor();
	dbms_sql.parse(cursorid,'',1);
	v_stat := dbms_sql.execute(cursorid);
	dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_parse_15();

--insert
create table dbms_sql_udi_01(n_id   number,  v_name  varchar2(50), d_insert_date char(20));

create or replace procedure pro_dbms_sql_insert_01()
is 
declare
   v_cursor   int;
   v_sql      varchar2(200);
   v_id       int;
   v_name     varchar2(50);
   v_date     char(20);
   v_stat     int;
begin   
   v_id := 1;
   v_name := '1';
   v_date := '85412';
   v_cursor := dbms_sql.open_cursor();  
   v_sql := 'insert into dbms_sql_udi_01(n_id, v_name, d_insert_date) values('||v_id||','||v_name||','||v_date||');';
   dbms_sql.parse(v_cursor, v_sql,1);    
   v_stat := dbms_sql.execute(v_cursor);  
   dbms_sql.close_cursor(v_cursor);   
end;
/


call pro_dbms_sql_insert_01();

select * from dbms_sql_udi_01;
insert into dbms_sql_udi_01(n_id, v_name, d_insert_date) values (1, 'Tome', '20190306');
--update 
create or replace procedure pro_dbms_sql_update_01()
as 
declare
   v_cursor   number;
   v_sql      varchar2(200);
   v_id       number;
   v_name     varchar2(50);
   v_stat     number;
begin
    v_name := '1';
    v_id := 1;
    v_cursor := dbms_sql.open_cursor();
    v_sql := 'update dbms_sql_udi_01 set v_name = '||v_name||', d_insert_date = 8454 where n_id = '||v_id||';';
    dbms_sql.parse(v_cursor, v_sql, 1);
    v_stat := dbms_sql.execute(v_cursor);
    dbms_sql.close_cursor(v_cursor);
    commit;
end;
/

call pro_dbms_sql_update_01();

select * from dbms_sql_udi_01;

--delete
create or replace procedure pro_dbms_sql_delete_01()
as 
declare
    v_cursor   number;
    v_sql      varchar2(200);
    v_id       number;
    v_stat     number;
begin

   v_id := 1;
   v_sql := 'delete from dbms_sql_udi_01 where n_id = '||v_id||';';
   v_cursor := dbms_sql.open_cursor();
   dbms_sql.parse(v_cursor, v_sql,1);
   v_stat := dbms_sql.execute(v_cursor);
   dbms_sql.close_cursor(v_cursor);
end;
/

call pro_dbms_sql_delete_01();

select * from dbms_sql_udi_01;

create or replace procedure pro_dbms_sql_all_03(in_raw raw,in_int int,in_long bigint,in_text text,in_char char(30),in_varchar varchar(30))
as 
declare
cursorid int;
v_id int;
v_info bytea;
v_long bigint;
v_text text;
v_char char(30);
v_varchar varchar(30);
query varchar(2000);
execute_ret int;
define_column_ret_raw raw;
define_column_ret int;
define_column_ret_long bigint;
define_column_ret_text text;
define_column_ret_char char(30);
define_column_ret_varchar varchar(30);
begin
drop table if exists pro_dbms_sql_all_tb1_03 ;
create table pro_dbms_sql_all_tb1_03(a int ,b raw,c bigint,d text,e char(30),f varchar(30));
insert into pro_dbms_sql_all_tb1_03 values(1,HEXTORAW('DEADBEEF'),in_long,in_text,in_char,in_varchar);
insert into pro_dbms_sql_all_tb1_03 values(in_int,in_raw,-9223372036854775808,'5','SDSWFSWFSFWF','845injnj');
insert into pro_dbms_sql_all_tb1_03 values(3,HEXTORAW('DEADBEEF'),9223372036854775807,'4','sqsm','zhimajie');
insert into pro_dbms_sql_all_tb1_03 values(-2147483648,HEXTORAW('1'),'3','3','nuannuan','weicn');
insert into pro_dbms_sql_all_tb1_03 values(2147483647,HEXTORAW('2'),'4','2',in_varchar,in_char);
query := 'select * from pro_dbms_sql_all_tb1_03 order by 1';

cursorid := dbms_sql.open_cursor();

dbms_sql.parse(cursorid, query, 1);

define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_raw    := dbms_sql.define_column(cursorid,2,v_info);
define_column_ret_long   := dbms_sql.define_column(cursorid,3,v_long);
define_column_ret_text   := dbms_sql.define_column(cursorid,4,v_text);
define_column_ret_char   := dbms_sql.define_column_char(cursorid,5,v_char,30);
define_column_ret_varchar:= dbms_sql.define_column_varchar(cursorid,6,v_varchar,30);

execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);

dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value(cursorid,2,v_info);
dbms_sql.column_value_long(cursorid,3,8,1,v_long,8);
dbms_sql.column_value(cursorid,4,v_text);
dbms_sql.column_value_char(cursorid,5,v_char,30);
dbms_sql.column_value_char(cursorid,6,v_varchar,30);


end loop;

dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_all_03(HEXTORAW('DEADBEEF'),2,4,'SDSWFSWFSFWF','sdjddd','dcjdshj23');


drop table if exists pro_dbms_sql_err_open_06_tb1;
create table pro_dbms_sql_err_open_06_tb1(c1 int,c2 int);
create or replace procedure pro_dbms_sql_err_open_06() as
declare
id numeric ;
start_time int;
cursorid int; 
query text;
   a text;
   b int;
execute_ret int;
begin
cursorid := dbms_sql.open_cursor();
query := 'select * from pro_dbms_sql_err_open_06_tb1;';
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, a);
dbms_sql.define_column(cursorid,2, b);
execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);
dbms_sql.column_value(cursorid,1, a);
dbms_sql.column_value(cursorid,2, b);
end loop;
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_err_open_06();
call pro_dbms_sql_err_open_06();
call pro_dbms_sql_err_open_06();

create or replace procedure pro_dbms_sql_char_02()
as 
declare
cursorid int;
v_id int;
v_info char(10):=1;
query varchar(2000);
execute_ret int;
define_column_ret_text char(10) ;
define_column_ret int;
begin
drop table if exists pro_dbms_sql_char_tb1_02 ;
create table pro_dbms_sql_char_tb1_02(a int ,b char(20));
insert into pro_dbms_sql_char_tb1_02 values(1,'闲听落花');
insert into pro_dbms_sql_char_tb1_02 values(2,'天空之城such');
query := 'select * from pro_dbms_sql_char_tb1_02 order by 1';

cursorid := dbms_sql.open_cursor();

dbms_sql.parse(cursorid, query, 1);

define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_text:= dbms_sql.define_column_char(cursorid,2,v_info,10);

execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);

dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value_char(cursorid,2,v_info,10);

end loop;

dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_char_02();
call pro_dbms_sql_char_02();

create or replace procedure pro_dbms_sql_fetch_rows_04()
as 
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select 1 from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute(cursorid);
dbms_sql.fetch_rows(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_fetch_rows_04();

create or replace procedure pro_dbms_sql_fetch_rows_03()
as 
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select true from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute(cursorid);
dbms_sql.fetch_rows(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_fetch_rows_03();

create or replace procedure pro_dbms_sql_all_02_02()
as
declare
cursorid int;
v_id int;
v_info text;
query varchar(2000);
execute_ret int;
define_column_ret_text text;
define_column_ret int;

cursor c1 for select * from pro_dbms_sql_all_tb2_02 order by 1;

begin
drop table if exists pro_dbms_sql_all_tb2_02 ;
create table pro_dbms_sql_all_tb2_02(a int ) distribute by hash(a);
insert into pro_dbms_sql_all_tb2_02 values(1);
insert into pro_dbms_sql_all_tb2_02 values(2);
insert into pro_dbms_sql_all_tb2_02 values(3);
insert into pro_dbms_sql_all_tb2_02 values(4);
insert into pro_dbms_sql_all_tb2_02 values(5);
insert into pro_dbms_sql_all_tb2_02 values(6);
query := 'select * from pro_dbms_sql_all_tb2_02 order by 1';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_text:= dbms_sql.define_column(cursorid,2,v_info);
execute_ret := dbms_sql.execute(cursorid);

loop
exit when (dbms_sql.fetch_rows(cursorid) <= 0);
dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value(cursorid,2,v_info);
end loop;
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_all_02_02();

create or replace procedure pro_dbms_sql_all_01()
as 
declare
cursorid int;
v_id int;
v_info text;
query varchar(2000);
execute_ret int;
define_column_ret_text text;
define_column_ret int;
begin
drop table if exists pro_dbms_sql_all_tb1_01 ;
create table pro_dbms_sql_all_tb1_01(a int ,b text);
insert into pro_dbms_sql_all_tb1_01 values(1,'闲听落花');
insert into pro_dbms_sql_all_tb1_01 values(2,'私は音楽が好きです,肖申克的救赎，天空之城suchcbhdc');
query := 'with tmp as (select * from pro_dbms_sql_all_tb1_01)select * from tmp order by 1';

cursorid := dbms_sql.open_cursor();

dbms_sql.parse(cursorid, query, 1);

define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_text:= dbms_sql.define_column(cursorid,2,v_info);

execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);

dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value(cursorid,2,v_info);

end loop;

dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_all_01();

create or replace procedure pro_dbms_sql_select_into_01()
as 
declare
cursorid int;
v_id int;
v_info text;
query varchar(2000);
execute_ret int;
define_column_ret_text text;
define_column_ret int;
begin
drop table if exists pro_dbms_sql_select_into_tb1_01 ;
create table pro_dbms_sql_select_into_tb1_01(a int ,b text);
insert into pro_dbms_sql_select_into_tb1_01 values(1,'闲听落花');
insert into pro_dbms_sql_select_into_tb1_01 values(2,'私は音楽が好きです,肖申克的救赎，天空之城suchcbhdc');
query := 'select * into pro_dbms_sql_select_into_tb1_02 from pro_dbms_sql_select_into_tb1_01 order by 1';

cursorid := dbms_sql.open_cursor();

dbms_sql.parse(cursorid, query, 1);

define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_text:= dbms_sql.define_column(cursorid,2,v_info);

execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);

dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value(cursorid,2,v_info);

end loop;

dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_select_into_01();



drop table pro_dbms_sql_001;
drop table pro_dbms_sql_002;
create table pro_dbms_sql_001 (id int,col1 int,info varchar(10)) distribute by hash(id);
create table pro_dbms_sql_002 (id int,col1 int,info varchar(10)) distribute by hash(id);
insert into pro_dbms_sql_001 values(1,2,'AAAAA');
insert into pro_dbms_sql_001 values(2,3,'BBBBB');
insert into pro_dbms_sql_001 values(3,1,'CCCCC');
insert into pro_dbms_sql_002 values(1,2,'AAAAAAAA');
insert into pro_dbms_sql_002 values(2,3,'BBBBBBBB');
insert into pro_dbms_sql_002 values(3,1,'CCCCCCCC');

-- define_column_char
create or replace procedure pro_dbms_sql_01(lenth int)
as 
declare
cursorid int;
err int;
v_id int;
v_col1 int;
v_info varchar(10) :=1;
query varchar(2000);
execute_ret int;
define_column_ret_char varchar(10);
define_column_ret int;
begin
query := 'select * from pro_dbms_sql_001 order by 1;';

cursorid := dbms_sql.open_cursor();

dbms_sql.parse(cursorid, query, 1);

define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret:= dbms_sql.define_column(cursorid,2,v_col1);
define_column_ret_char:= dbms_sql.define_column_char(cursorid,3,v_info,10);

execute_ret := dbms_sql.execute(cursorid);
loop 
exit when (dbms_sql.fetch_rows(cursorid) <= 0);

dbms_sql.column_value(cursorid,1,v_id);
dbms_sql.column_value(cursorid,2,v_col1);
dbms_sql.column_value_char(cursorid,3,v_info,err,lenth);

end loop;

dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_01(1);

create or replace procedure pro_dbms_sql_fetch_rows_06()
as
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select ''2012-12-30''::date from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute(cursorid);
dbms_sql.fetch_rows(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
exception when others then
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_fetch_rows_06();

create or replace procedure pro_dbms_sql_fetch_rows_07()
as
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select ''2012-12-30''::clob from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute(cursorid);
dbms_sql.fetch_rows(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
exception when others then
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_fetch_rows_07();

create or replace procedure pro_dbms_sql_last_row_count()
as
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select ''2012-12-30''::date from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute(cursorid);
loop
exit when (dbms_sql.fetch_rows(cursorid) <= 0);
dbms_sql.column_value(cursorid,1,v_id);
end loop;
dbms_sql.last_row_count(cursorid);
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_last_row_count();


create or replace procedure pro_dbms_sql_execute_and_fetch()
as
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select ''2012-12-30''::date from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.define_column(cursorid,1, v_id);
fetch_rows_ret := dbms_sql.execute_and_fetch(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
exception when others then
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_execute_and_fetch();

create or replace procedure pro_dbms_sql_bind_var()
as
declare
cursorid int;
v_id varchar(3000) :=0;
v_col1 int;
v_info varchar(10);
query varchar(2000);
fetch_rows_ret int;
begin
query := 'select * from dual;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
dbms_sql.bind_variable(cursorid);
fetch_rows_ret := dbms_sql.execute(cursorid);
dbms_sql.fetch_rows(cursorid);
dbms_sql.column_value(cursorid,1,v_id);
exception when others then
dbms_sql.close_cursor(cursorid);
end;
/

call pro_dbms_sql_bind_var();
create or replace procedure pro_dbms_sql_text()
as
declare
cursorid int;
v_id int :=1;
v_info text :=1;
query varchar(2000);
execute_ret int;
define_column_ret_text varchar(30) ;
define_column_ret int;
begin
create table pro_dbms_sql_varchar_tb1_001(a int ,b varchar) distribute by hash(a);
insert into pro_dbms_sql_varchar_tb1_001 values(1,'abcdefg');
query := ' ;';
cursorid := dbms_sql.open_cursor();
dbms_sql.parse(cursorid, query, 1);
define_column_ret:= dbms_sql.define_column(cursorid,1,v_id);
define_column_ret_text:= dbms_sql.define_column_text(cursorid,2,v_info);
execute_ret := dbms_sql.execute(cursorid);
loop
exit when (dbms_sql.fetch_rows(cursorid) <= 0);
dbms_sql.column_value(cursorid,1,v_id);
v_info:=dbms_sql.column_value_text(cursorid,2);
end loop;
dbms_sql.close_cursor(cursorid);
EXCEPTION
WHEN OTHERS THEN
DBMS_SQL.CLOSE_CURSOR(cursorid);
end;
/
call pro_dbms_sql_text();

set work_mem='64kB';
DECLARE
    cursorid int;
    retval  int;
	is_open boolean;
    c1 int;
    c2 char(3996);
    query text;
BEGIN
	Drop table if exists t01;
	Create table t01 (c1 int, c2 char(3996)) distribute by hash(c1); --4kB each row
	Insert into t01 select v,to_char(v) from generate_series(0,999) as v;--4M
	query := 'select c1,c2 from t01 order by 1;';

	--打开游标
	cursorid := dbms_sql.open_cursor();
	--编译游标
	dbms_sql.parse(cursorid, query, 1);
	--定义列
	dbms_sql.define_column(cursorid,1,c1);
	dbms_sql.define_column(cursorid,2,c2);
	--执行
	retval := dbms_sql.execute(cursorid);
	--savepoint p1;
	loop
	exit when (dbms_sql.fetch_rows(cursorid) <= 0);
		--获取值
		dbms_sql.column_value(cursorid,1,c1);
		dbms_sql.column_value(cursorid,2,c2);
		--输出结果
	end loop;
	--关闭游标
	dbms_sql.close_cursor(cursorid);
	--return 0;
END;
/
reset work_mem;
/* END DBMS_SQL PACKAGE*/

