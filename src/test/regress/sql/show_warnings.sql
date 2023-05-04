drop database if exists db_show_warnings;
create database db_show_warnings dbcompatibility 'b';
\c db_show_warnings

show max_error_count;
show sql_note;
create table test(id int, name varchar default 11);
insert into t1 values(1,'test');
show warnings limit 1;
show errors limit 1;
show count(*) warnings;
show count(*) errors;
set max_error_count = 0;
show errors;
set max_error_count = 64;

CREATE OR REPLACE FUNCTION TEST_FUNC(tempdata char) RETURNS VOID AS $$
BEGIN
	raise info'TEST CHAR VALUE IS %',tempdata;  
END;
$$ LANGUAGE plpgsql;
select TEST_FUNC('abc'::clob);

show warnings;

drop table if exists t_showtest;
create table t_showtest(a int, primary key(a));
show errors;
show errors limit 10;

drop table if exists t_showtest;
create table t_showtest(a int, primary key(a));
show warnings;
drop table if exists t_showtest;

update user set b = 'x' where a = 1;
show warnings;
show errors;

set sql_note=false;
select TEST_FUNC('abc'::clob);
show warnings;

SELECT pg_advisory_unlock(1), pg_advisory_unlock_shared(2), pg_advisory_unlock(1, 1), pg_advisory_unlock_shared(2, 2);
show warnings;
show warnings limit 2, 4;

show enable_thread_pool;
set query_mem='32MB';
set query_max_mem='32MB';
create table aaa(a int,b date,partial cluster key(b)) with (orientation = column);
insert into aaa(a ) select generate_series(1,10000000);


\c postgres
drop database if exists db_show_warnings;
