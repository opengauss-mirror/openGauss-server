reset search_path;
create extension if not exists gms_utility;

-- start test db_version
declare
    version varchar2(50);
    compatibility varchar2(50);
begin
    gms_utility.db_version(version, compatibility);
    raise info 'version: %', version;
    raise info 'compatibility: %', compatibility;
end;
/
declare
    version varchar2(50);
begin
    gms_utility.db_version(version);
    raise info 'version: %', version;
end;    -- error
/
call gms_utility.db_version();  -- error
declare
    version int;
    compatibility int;
begin
    gms_utility.db_version(version, compatibility);
    raise info 'version: %', version;
    raise info 'compatibility: %', compatibility;
end;    -- error
/
-- end test db_version

-- start test get_cpu_time
select gms_utility.get_cpu_time();
select gms_utility.get_cpu_time(100);   -- error
declare
    cputime char(1);
begin
    cputime := gms_utility.get_cpu_time();
    raise info 'cpuTimeDelta: %', cputime;
end;
/
declare
    cputime json;
begin
    cputime := gms_utility.get_cpu_time();
    raise info 'cpuTimeDelta: %', cputime;
end;
/
declare
    t1 number := 0;
    t2 number := 0;
    timeDelta number;
    i integer;
    sum integer;
begin
    t1 := gms_utility.get_cpu_time();
    for i in 1..1000000 loop
       sum := sum + i * 2 + i / 2;
    end loop;
    t2 := gms_utility.get_cpu_time();
    timeDelta = t2 - t1;
    raise info 'cpuTimeDelta: %', timeDelta;
end;
/
declare
    i number;
    j number;
    k number;
begin
    i := gms_utility.get_cpu_time;

    select count(*)
    into j
    from pg_class t, pg_index i
    where t.Oid = i.indexrelid;

    k := gms_utility.get_cpu_time;
    raise info 'costtime: %', (k-i);
end;
/
-- end test get_cpu_time

-- start test get_endianness
select gms_utility.get_endianness();
select gms_utility.get_endianness(1000);
-- end test get_endianness

-- start test get_sql_hash
declare
    hash    raw(50);
    l4b     number;
begin
    gms_utility.get_sql_hash('Today is a good day!', hash, l4b);
    raise info 'hash: %', hash;
    raise info 'last4byte: %', l4b;
end;
/
declare
    hash    raw(50);
    l4b     number;
begin
    gms_utility.get_sql_hash(NULL, hash, l4b);
    raise info 'hash: %', hash;
    raise info 'last4byte: %', l4b;
end;
/
declare
    hash    raw(50);
    l4b     number;
begin
    gms_utility.get_sql_hash('', hash, l4b);
    raise info 'hash: %', hash;
    raise info 'last4byte: %', l4b;
end;
/
call gms_utility.get_sql_hash();
-- end test get_sql_hash

-- start test is_bit_set
declare
	r raw(50) := '123456AF';
	result NUMBER;
	pos NUMBER;
begin
	for pos in 1..32 loop
		result := gms_utility.is_bit_set(r, pos);
		raise info 'position = %, result = %', pos, result;
	end loop;
end;
/
declare
	r raw(50) := '123456AF';
	result NUMBER;
	pos NUMBER;
begin
	for pos in 33..64 loop
		result := gms_utility.is_bit_set(r, pos);
		raise info 'position = %, result = %', pos, result;
	end loop;
end;
/
select gms_utility.is_bit_set(); -- error
select gms_utility.is_bit_set(NULL, 10); -- error
select gms_utility.is_bit_set('', 10);  -- error
select gms_utility.is_bit_set('123456'); -- error
select gms_utility.is_bit_set('123456AF', 9);
select gms_utility.is_bit_set('12345678AF', 9);
select gms_utility.is_bit_set('123456TT', 9); -- error
select gms_utility.is_bit_set('12345678TT', 9); -- error
select gms_utility.is_bit_set('1234', 9);
select gms_utility.is_bit_set('12345678', -10); -- error
select gms_utility.is_bit_set('12345678', 0); -- error
select gms_utility.is_bit_set('12345678', 100);
select gms_utility.is_bit_set('12345678', 3.14E100); -- error
select gms_utility.is_bit_set('团团又圆圆', 20); -- error
-- end test is_bit_set

-- start test is_cluster_database
select gms_utility.is_cluster_database();
select gms_utility.is_cluster_database(1000);
begin
	if gms_utility.is_cluster_database then
		raise info 'true';
	else
		raise info 'false';
	end if;
end;
/
-- end test is_cluster_database

-- start test old_current_schema
select gms_utility.old_current_schema();
select gms_utility.old_current_schema(2000);
-- end test old_current_schema

-- start test old_current_user
select gms_utility.old_current_user();
select gms_utility.old_current_user(2000);
-- end test old_current_user

---------------------------
-- expand_sql_text
---------------------------
create schema test_utility_est;
set search_path to test_utility_est;

declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text(NULL, sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text(NULL);
    raise info 'output_sql_text: %', sqlTxt;
end;
/

create table test_dx(id int);
create view view1 as select * from test_dx;
create materialized view mv_dx as select * from test_dx;

declare
    input_sql_text clob := 'select * from test_utility_est.view1';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
declare
    input_sql_text clob := 'select * from test_utility_est.view1;';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
-- don't expand materialized view sql, just print
declare
    input_sql_text clob := 'select * from test_utility_est.mv_dx';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
-- test multi sql, error
declare
    input_sql_text clob := 'select * from test_utility_est.view1;select * from test_utility_est.view1';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
-- test select into
insert into test_dx values (3);
declare
    num int := 0;
    input_sql_text clob := 'select * into t_into from test_utility_est.view1;';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
declare
    num int := 0;
    output_sql_text clob;
begin
    gms_utility.expand_sql_text('select id into num from test_utility_est.view1', output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
--test with select
declare
    num int := 0;
    input_sql_text clob := 'with ws (id) AS (select id from test_utility_est.view1) select * from ws';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
-- test select for update
declare
    input_sql_text clob := 'select * from test_utility_est.test_dx for update';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/
declare
    input_sql_text clob := 'select * from test_utility_est.view1 for update';
    output_sql_text clob;
begin
    gms_utility.expand_sql_text(input_sql_text, output_sql_text);
    raise info 'output_sql_text: %', output_sql_text;
end;
/

create table test_1(id int, name varchar(20));
create table test_2(id int, name varchar(20));
create view view2 as select * from test_1;
create view view3 as select * from test_1 union select * from test_2;
create view view4 as select * from test_1 union select * from view3;

declare
 input_sql_text1 clob := 'select * from test_utility_est.view3';
 output_sql_text1 clob;
 input_sql_text2 clob := 'select * from test_utility_est.view4';
 output_sql_text2 clob;
begin
  gms_utility.expand_sql_text(input_sql_text1, output_sql_text1);
  raise info 'output_sql_text1: %', output_sql_text1;
  gms_utility.expand_sql_text(input_sql_text2, output_sql_text2);
  raise info 'output_sql_text2: %', output_sql_text2;
END;
/

declare
 input_sql_text1 clob := 'select * from test_utility_est.view3';
 output_sql_text1 clob;
 input_sql_text2 clob := 'select * from test_utility_est.view4';
 output_sql_text2 clob;
begin
  gms_utility.expand_sql_text(input_sql_text1, output_sql_text1);
  raise info 'output_sql_text1: %', output_sql_text1;
  gms_utility.expand_sql_text(input_sql_text2, output_sql_text2);
  raise info 'output_sql_text2: %', output_sql_text2;
END;
/

-- test not select query, error
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text('create table test(id int)', sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text('begin', sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text('update test_1 set name = ''ggboom'' where id = 1', sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text('delete from test_1', sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/

-- test not valid query, error
declare
    sqlTxt CLOB;
begin
    gms_utility.expand_sql_text('today is a good day', sqlTxt);
    raise info 'output_sql_text: %', sqlTxt;
end;
/

create table t1(srvr_id int);
create view expandv as select * from t1;

declare
    vclobin clob := 
'select distinct srvr_id 
from public.expandv 
where srvr_id not in 
(select srvr_id 
from public.expandv 
minus 
select srvr_id 
from public.t1)';
    vclobout clob;
begin
    gms_utility.expand_sql_text(vclobin, vclobout);
    raise info 'output_sql_text: %', vclobout; 
end;
/

-- test privileges
create user user01 password 'utility@123';
set session AUTHORIZATION user01 password 'utility@123';
set search_path to test_utility_est;

declare
 input_sql_text1 clob := 'select * from test_utility_est.view3';
 output_sql_text1 clob;
begin
  gms_utility.expand_sql_text(input_sql_text1, output_sql_text1);
  raise info 'output_sql_text1: %', output_sql_text1;
END;
/

RESET SESSION AUTHORIZATION;

drop view view4;
drop view view3;
drop view view2;
drop view view1;
drop view expandv;
drop materialized view mv_dx;
drop table t1;
drop table if exists t_into;
drop talbe if exists test_2;
drop talbe if exists test_1;
drop talbe if exists test_dx;

drop schema test_utility_est;

---------------------------
-- analyze_schema|database
---------------------------
create user test_utility_analyze password "test_utility_analyze@123";
set search_path to test_utility_analyze;

create table t1 (id int, c2 text);
create unique index t1_id_uidx on t1 using btree(id);
insert into t1 values (generate_series(1, 100), 'aabbcc');
insert into t1 values (generate_series(101, 200), '123dfg');
insert into t1 values (generate_series(201, 300), '人面桃花相映红');
insert into t1 values (generate_series(301, 400), 'fortunate');
insert into t1 values (generate_series(401, 500), 'open@gauss');
insert into t1 values (generate_series(501, 600), '127.0.0.1');
insert into t1 values (generate_series(601, 700), '!@#$!%#!');
insert into t1 values (generate_series(701, 800), '[1,2,3,4]');
insert into t1 values (generate_series(801, 900), '{"name":"张三","age":18}');
insert into t1 values (generate_series(901, 1000), '');

create or replace function batch_insert(count INT)
returns int as $$
declare
	i INT;
	start INT;
begin
	select count(*) into start from t1;
	for i in select generate_series(1, count) loop
		insert into t1 values (start + i, left((pg_catalog.random() * i)::text, 6));
	end loop;

	return count;
end;
$$ language plpgsql;

select batch_insert(50000);

-- analyze_schema
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000);
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=-100);	-- error
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_percent:=20);
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_percent:=0);
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_percent:=101); -- error
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101);
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR TABLE');
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL COLUMNS');
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL COLUMNS SIZE 100');
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_schema('test_utility_analyze', 'ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXED COLUMNS SIZE 100');

call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', estimate_rows:=10000);
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', estimate_percent:=-100);
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', estimate_rows:=10000, estimate_percent:=-100);
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR TABLE');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL INDEXED COLUMNS SIZE 100');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL COLUMNS');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL COLUMNS SIZE 100');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR TABLE FOR ALL INDEXES');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR TABLE FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR TABLE123');	-- error
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='111FOR TABLE');	-- error

call gms_utility.analyze_schema('test_utility_analyze', 'DELETE');
call gms_utility.analyze_schema('test_utility_analyze', 'DELETE', method_opt:='FOR TABLE');	-- error
DECLARE
    res boolean;
BEGIN
    gms_utility.analyze_schema('SYSTEM','ESTIMATE', NULL, 10);
    EXCEPTION
    WHEN OTHERS THEN
        NULL;
END;
/

--  test analyze database 
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000);
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=-100);	-- error
call gms_utility.analyze_database('ESTIMATE', estimate_percent:=20);
call gms_utility.analyze_database('ESTIMATE', estimate_percent:=0);
call gms_utility.analyze_database('ESTIMATE', estimate_percent:=101); -- error
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101);
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR TABLE');
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL COLUMNS');
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL COLUMNS SIZE 100');
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_database('ESTIMATE', estimate_rows:=10000, estimate_percent:=101, method_opt:='FOR ALL INDEXED COLUMNS SIZE 100');

call gms_utility.analyze_database('COMPUTE');
call gms_utility.analyze_database('COMPUTE', estimate_rows:=10000);
call gms_utility.analyze_database('COMPUTE', estimate_percent:=-100);
call gms_utility.analyze_database('COMPUTE', estimate_rows:=10000, estimate_percent:=-100);
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR TABLE');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL INDEXED COLUMNS SIZE 100');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL COLUMNS');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL COLUMNS SIZE 100');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR TABLE FOR ALL INDEXES');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR TABLE FOR ALL INDEXED COLUMNS');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR TABLE123');	-- error
call gms_utility.analyze_database('COMPUTE', method_opt:='111FOR TABLE');	-- error

call gms_utility.analyze_database('DELETE');
call gms_utility.analyze_database('DELETE', method_opt:='FOR TABLE');	-- error

ALTER INDEX t1_id_uidx UNUSABLE;
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL INDEXES');

ALTER INDEX t1_id_uidx REBUILD;
call gms_utility.analyze_schema('test_utility_analyze', 'COMPUTE', method_opt:='FOR ALL INDEXES');
call gms_utility.analyze_database('COMPUTE', method_opt:='FOR ALL INDEXES');

drop table t1 cascade;
drop function batch_insert;

reset search_path;
drop user test_utility_analyze cascade;

---------------------------
-- canonicalize
---------------------------
create schema test_utility_canonicalize;
set search_path to test_utility_canonicalize;

-- canonicalize
declare
    canon_name varchar2(100);
begin
    gms_utility.canonicalize('uwclass.test', canon_name, 16);
    raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

-- empty、space
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize(NULL, canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- NULL
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('""', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('     ', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('    .abcd', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll..nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll."".nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('"koll"', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('   "  koll.rooy"  .nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('"~!#@@#$%)(#@(!@))<>?/*-+".nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy#_$nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll."rooy"  abc.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;  	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nu"uo"p', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy."nu"u"o"p', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll."rooy"."nuuop.', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

-- identy
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.123rooy.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll._rooy.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;	-- ok for og, error for A
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll."_rooy".nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('table', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.table.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end; -- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('column', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.column.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end; -- error
/
-- test indentifier overlength; og >= 64, A >= 128
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.DoYouThinkCausalUnderstandingIsADefiningCharacteristicOfHumanCognition.nuuop.a', canon_name, 100);
	raise info 'canon_name: %', canon_name;
end;
/

-- param canon_len
declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, -10);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 0);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 10);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('开车.rooy.nuuop.a', canon_name, 10);
	raise info 'canon_name: %', canon_name;
end;
/

declare
	canon_name varchar2(10);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 20);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

drop schema test_utility_canonicalize;

---------------------------
-- compile schema
---------------------------
create schema test_utility_compile;
set search_path to test_utility_compile;

create table classes (  
    class_id int primary key,  
    class_name varchar(50) not null  
)
with (orientation=row, compression=no);
  
create table students (  
    student_id int primary key,  
    class_id int,  
    student_name varchar(50) not null
)
with (orientation=row, compression=no);
  
create table studentscores (  
    score_id int primary key,  
    student_id int,  
    subject varchar(50),  
    score int
)
with (orientation=row, compression=no);

create or replace procedure insertclass(in classname varchar(50))
as
	ct int;
begin
	select count(class_id) into ct from classes;
    insert into classes values (ct + 1, classname);
end;
/
  
create or replace procedure insertstudent(in classname varchar(50), in studentname varchar(50))
as
	classid int;
	ct int;
begin  
    select class_id into classid from classes where class_name = classname;
	select count(student_id) into ct from students;
    insert into students values (ct + 1, classid, studentname);
end;
/

create or replace function insertscore(studentid int, subject varchar(50), score int)  
returns int
as $$
declare
	ct int;
begin
	select count(score_id) into ct from studentscores;
    insert into studentscores values (ct + 1, studentid, subject, score);
    return ct + 1;  
end;
$$ language plpgsql;

create or replace function getrank(score int)
returns text
as $$
declare
    rk text;
begin
    if score = 100 then
        rk := 'perfect';
    elsif score >= 80 then
        rk := 'nice';
    elsif score >= 60 then
        rk := 'ordinary';
    elsif score >= 0 then
        rk := 'poor';
    else
        raise notice 'error input';
    end if;

    return rk;
end;
$$ language plpgsql;

call insertclass('class a');  
call insertclass('class b');  
  
call insertstudent('class a', 'alice');  
call insertstudent('class a', 'bob');  
call insertstudent('class b', 'charlie');  
call insertstudent('class b', 'rose');
call insertstudent('class b', 'jack');
  
select insertscore(1, 'math', 90);  
select insertscore(1, 'science', 85);  
select insertscore(2, 'math', 95);  
select insertscore(2, 'science', 88);  
select insertscore(3, 'math', 78);  
select insertscore(3, 'science', 82);
select insertscore(4, 'math', 100);  
select insertscore(4, 'science', 66);
select insertscore(5, 'math', 57);  
select insertscore(5, 'science', 68);

create view stud_class as select s.student_name, c.class_name from students s join classes c on s.class_id = c.class_id;
create view stud_score as select b.student_name, a.subject, a.score, getrank(a.score) from studentscores a join students b on a.student_id = b.student_id;

create table t_pkg (c1 text);

create package p_batch as
	procedure insertpkg(txt text);
	function batchinsert(num int) return int;
end p_batch;
/

create package body p_batch as
    procedure insertpkg(txt text)
    as
    begin
        insert into t_pkg values (txt);
    end;

    function batchinsert(num int)
    return int
    is
        start int := 0;
        i int := 0;
    begin
        select count(*) into start from t_pkg;
        for i in select generate_series(1, num) loop
            insert into t1 values (start + i, left((pg_catalog.random() * i)::text, 6));
        end loop;

        return i;
    end;
end p_batch;
/

call gms_utility.compile_schema('test_utility_compile');
call gms_utility.compile_schema('test_utility_compile', false);
call gms_utility.compile_schema('test_utility_compile', true, false);

call gms_utility.compile_schema(); -- error
call gms_utility.compile_schema(1); -- error
call gms_utility.compile_schema('no_schema'); -- error

create or replace function getrank(score int)
returns text
as $$
declare
    rk text;
begin
    if score = 100 then
        rk := 'perfect';
    elsif score >= 80 then
        rk := 'nice';
    elsif score >= 60 then
        rk := 'ordinary';
    elsif score >= 0 then
        rk := 'poor';
    else
        rk := 'error';
    end if;

    return rk;
end;
$$ language plpgsql;

call gms_utility.compile_schema('test_utility_compile');

drop package body p_batch;
drop package p_batch;
drop table t_pkg;
drop view stud_score;
drop view stud_class;
drop function getrank;
drop function insertscore;
drop procedure insertstudent;
drop procedure insertclass;
drop table studentscores;
drop table students;
drop table classes;

-- test invalid object compile
set behavior_compat_options = 'plpgsql_dependency';
create type s_type as (
    id integer,
    name varchar,
    addr text
);
create or replace procedure type_alter(a s_type)
is
begin    
    raise info 'call a: %', a;
end;
/
select valid from pg_object where object_type='p' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
alter type s_type add attribute a int;
select valid from pg_object where object_type='p' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='p' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));

create table stu(sno int, name varchar, sex varchar, cno int);
create type r1 as (a int, c stu%rowtype);
create or replace package pkg
is    
    procedure proc1(p_in r1);
end pkg;
/
create or replace package body pkg
is
declare
    v1 r1;
    v2 stu%rowtype;
    procedure proc1(p_in r1) as
    begin        
    raise info 'call p_in: %', p_in;
    end;
end pkg;
/
call pkg.proc1((1,(1,'zhang','m',1)));
select valid from pg_object where object_type='b' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
alter table stu add column b int;
select valid from pg_object where object_type='b' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='b' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));

create view v_stu as select * from stu;
select * from v_stu;
select valid from pg_object where object_type='v' and object_oid in (select oid from pg_class where relname='v_stu' and relkind='v' and relnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
alter table stu drop column b;
select valid from pg_object where object_type='v' and object_oid in (select oid from pg_class where relname='v_stu' and relkind='v' and relnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='v' and object_oid in (select oid from pg_class where relname='v_stu' and relkind='v' and relnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
select * from v_stu;

alter table stu add column b int;
select valid from pg_object where object_type='v' and object_oid in (select oid from pg_class where relname='v_stu' and relkind='v' and relnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='v' and object_oid in (select oid from pg_class where relname='v_stu' and relkind='v' and relnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
select * from v_stu;

drop view v_stu;
drop package body pkg;
drop package pkg cascade;
drop type r1;
drop table stu;
drop procedure type_alter;

-- test procedure compile error
set behavior_compat_options = 'plpgsql_dependency';
create or replace procedure proc_err(p_in s_type) as
begin
    raise info 'call p_in: %', p_in;
end;
/
drop type s_type;
select valid from pg_object where object_type='p' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='proc_err' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='p' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='proc_err' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));

drop procedure proc_err;

drop schema test_utility_compile cascade;

---------------------------
-- name tokenize
---------------------------
create schema test_utility_tokenize;
set search_path to test_utility_tokenize;

declare
    name varchar2(50) := 'peer.lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/

-- test empty character
declare
    name varchar2(50) := NULL;
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := '';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := '""';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := '  .  ';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := '"  .  "';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.   lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer."   lokppe.vuumee"@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/

-- test support special char
declare
    name varchar2(50) := 'peer._lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- ok for og, error for A
/
declare
    name varchar2(50) := 'peer.lokp_pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.$lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- ok for og, error for A
/
declare
    name varchar2(50) := 'peer.lokp$pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.lokp233pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.233lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = , c = , dblink = , nextpos = 4
/
declare
    name varchar2(50) := 'peer.-lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer."-lokppe".vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.lokp-pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKP, c = , dblink = , nextpos = 9
/
declare
    name varchar2(50) := 'peer.lokp=pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKP, c = , dblink = , nextpos = 9
/
declare
    name varchar2(50) := 'peer.=lokppe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokp`pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokp~pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokp%pe.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/

-- test keyword
declare
    name varchar2(50) := 'peer.table.vuumee@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/

-- test @
declare
    name varchar2(50) := 'peer.lokppe.vuumee@';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@_ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- ok for og, error for A
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@"_ookeyy"';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@$ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- ok for og, error for A
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@"$ookeyy"';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@123ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@ookeyy.zk';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := '@vuumee';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@ook=eyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKPPE, c = VUUMEE, dblink = OOK, nextpos = 22
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@=ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee@ook%eyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/

-- test double quote
declare
    name varchar2(50) := 'peer.lokppe.vuumee"@ookeyy"';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKPPE, c = VUUMEE, dblink = , nextpos = 18
/
declare
    name varchar2(50) := 'peer.lokppe.vu"ume"e@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKPPE, c = VU, dblink = , nextpos = 14
/
declare
    name varchar2(50) := 'peer.lokppe."vu"ume"e@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- a = PEER, b = LOKPPE, c = vu, dblink = , nextpos = 16
/
declare
    name varchar2(50) := 'peer.lokppe.vuumee.aking@ookeyy';
    a   varchar2(50);
    b   varchar2(50);
    c   varchar2(50);
    dblink  varchar2(50);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/

-- test idnetifier overlength. og >= 64, A >= 128
declare
    name varchar2(100) := 'peer.DoYouThinkCausalUnderstandingIsADefiningCharacteristicOfHumanCognition.vuumee.aking@ookeyy';
    a   varchar2(100);
    b   varchar2(100);
    c   varchar2(100);
    dblink  varchar2(100);
    nextpos integer;
begin
    gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
    raise info 'a = %, b = %, c = %, dblink = %, nextpos = %', a, b, c, dblink, nextpos;
end;    -- error
/

drop schema test_utility_tokenize;

---------------------------
-- name resolve
---------------------------
reset search_path;

-- prepare data
CREATE TABLE public.t_resolve (c1 NUMBER, c2 VARCHAR2(100));
CREATE UNIQUE INDEX IF NOT EXISTS public.t_resolve_c1_udx ON public.t_resolve(c1);
CREATE TABLE public.t_log (c1 NUMBER, c2 TIMESTAMP);

CREATE SEQUENCE IF NOT EXISTS public.t_seq
    INCREMENT BY 1
    NOMINVALUE
    NOMAXVALUE
    START WITH 1
    NOCYCLE;
SELECT public.t_seq.NEXTVAL;

CREATE OR REPLACE FUNCTION public.add_numbers (  
    p_num1 IN INT,  
    p_num2 IN INT  
) RETURN NUMBER IS  
    v_result INT;  
BEGIN  
    v_result := p_num1 + p_num2;  
    RETURN v_result;  
END;  
/

CREATE OR REPLACE PROCEDURE public.insert_val(name VARCHAR2(100)) IS 
	id INT := 0;
BEGIN 
	SELECT public.t_seq.NEXTVAL INTO id;
	INSERT INTO public.t_resolve VALUES (id, name);
END;
/

CREATE OR REPLACE FUNCTION public.tg_log() RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO public.t_log VALUES (NEW.c1, SYSDATE);
    RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER log_resolve_after_insert
AFTER INSERT ON public.t_resolve 
FOR EACH ROW 
EXECUTE PROCEDURE public.tg_log();

CREATE OR REPLACE PACKAGE public.t_pkg IS
    FUNCTION multi_number(num1 IN INT, num2 IN INT) RETURN INT;
    PROCEDURE delete_val(id IN INT);
END t_pkg;
/

CREATE OR REPLACE PACKAGE BODY public.t_pkg IS
    FUNCTION multi_number(num1 IN INT, num2 IN INT) RETURN INT IS
        v_res INT;
    BEGIN
        v_res = num1 * num2;
        return v_res;
    END multi_number;

    PROCEDURE delete_val(id IN INT) AS
    BEGIN
        DELETE FROM t_resolve WHERE c1 = id;
    END delete_val;
END t_pkg;
/

CREATE TYPE public.t_typ AS (t1 INT, t2 TEXT);

CREATE OR REPLACE SYNONYM public.syn_tbl FOR t_resolve;
CREATE OR REPLACE SYNONYM public.syn_idx FOR t_resolve_c1_udx;
CREATE OR REPLACE SYNONYM public.syn_seq FOR t_seq;
CREATE OR REPLACE SYNONYM public.syn_fun FOR add_numbers;
CREATE OR REPLACE SYNONYM public.syn_pro FOR insert_val;
CREATE OR REPLACE SYNONYM public.syn_tg FOR log_resolve_after_insert;
CREATE OR REPLACE SYNONYM public.syn_pkg FOR t_pkg;
CREATE OR REPLACE SYNONYM public.syn_typ FOR t_typ;


-- test table
declare 
    name varchar2 := 'public.t_resolve';
    context number := 0;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 't_resolve';
    context number := 0;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_tbl';
    context number := 0;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 'syn_tbl';
    context number := 0;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
-- test PL/SQL
declare
    name varchar2 := 'public.add_numbers';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 'add_numbers';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_fun';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_fun';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.insert_val';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 'insert_val';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_pro';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 'syn_pro';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
-- test package
declare
    name varchar2 := 'public.t_pkg.multi_number';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.t_pkg.delete_val';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.t_pkg.qwer';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 't_pkg.multi_number';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 't_pkg.delete_val';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 't_pkg.qwer';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_pkg.multi_number';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_pkg.delete_var';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_pkg.qwer';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_pkg.multi_number';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_pkg.delete_var';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_pkg.qwer';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

--  test trigger
declare
    name varchar2 := 'public.log_resolve_after_insert';
    context number := 3;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_tg';
    context number := 3;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;    -- error
/
declare
    name varchar2 := 'log_resolve_after_insert';
    context number := 3;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_tg';
    context number := 3;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;    -- error
/

-- test sequence
declare
    name varchar2 := 'public.t_seq';
    context number := 2;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_seq';
    context number := 2;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 't_seq';
    context number := 2;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare 
    name varchar2 := 'syn_seq';
    context number := 2;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

-- test type
declare
    name varchar2 := 'public.t_typ';
    context number := 7;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_typ';
    context number := 7;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 't_typ';
    context number := 7;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_typ';
    context number := 7;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

-- test index
declare
    name varchar2 := 'public.t_resolve_c1_udx';
    context number := 9;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.syn_idx';
    context number := 9;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;    -- error
/
declare
    name varchar2 := 't_resolve_c1_udx';
    context number := 9;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'syn_idx';
    context number := 9;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;    -- error
/

-- test java
declare
    name varchar2 := 'public.java.class';
    context number := 4;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.java.class';
    context number := 5;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.java.class';
    context number := 6;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'public.java.class';
    context number := 8;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

-- test with dblink
declare
    name varchar2 := 'peer.lokppe.vuumee@ookeyy';
    context number := 0;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'peer@ookeyy';
    context number := 1;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'peer@ookeyy';
    context number := 2;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'peer.@ookeyy';
    context number := 3;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;    -- error
/
declare
    name varchar2 := 'peer.lokppe@ookeyy';
    context number := 7;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'peer.lokppe@ookeyy';
    context number := 9;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/
declare
    name varchar2 := 'peer.lokppe@ookeyy';
    context number := 10;
    schema  varchar2;
    part1   varchar2;
    part2   varchar2;
    dblink  varchar2;
    part1_type  number;
    object_number   number;
begin
    gms_utility.NAME_RESOLVE(name, context, schema, part1, part2, dblink, part1_type, object_number);
    raise info 'schema = %, part1 = %, part2 = %, dblink = %, part1_type = %, object_number = %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

DROP SYNONYM syn_tbl;
DROP SYNONYM syn_idx;
DROP SYNONYM syn_seq;
DROP SYNONYM syn_fun;
DROP SYNONYM syn_pro;
DROP SYNONYM syn_tg;
DROP SYNONYM syn_pkg;
DROP SYNONYM syn_typ;
DROP TYPE public.t_typ;
DROP PACKAGE BODY public.t_pkg;
DROP PACKAGE public.t_pkg;
DROP TRIGGER log_resolve_after_insert ON t_resolve;
DROP FUNCTION public.tg_log;
DROP FUNCTION public.add_numbers;
DROP PROCEDURE public.insert_val;
DROP INDEX public.t_resolve_c1_udx;
DROP TABLE public.t_log;
DROP TABLE public.t_resolve;
DROP SEQUENCE public.t_seq;

drop extension gms_utility cascade;
reset search_path;