reset search_path;
create extension if not exists gms_utility;
create extension gms_output;
select gms_output.enable;
set behavior_compat_options="bind_procedure_searchpath";

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
select gms_utility.is_bit_set('12345678AF', null);
select gms_utility.is_bit_set('F2345678AF', null);
select gms_utility.is_bit_set('12345678AF', '');
select gms_utility.is_bit_set('F2345678AF', '');
declare
    raw_data raw(50) := hextoraw('ff11aa33');
    bit_position number := null;
    bit_status number;
begin
    bit_status := gms_utility.is_bit_set(raw_data, bit_position);
    raise info 'bit osition is: %', bit_status;
end;
/
declare
    raw_data raw(50) := hextoraw('ff11aa33');
    bit_position number := '';
    bit_status number;
begin
    bit_status := gms_utility.is_bit_set(raw_data, bit_position);
    raise info 'bit osition is: %', bit_status;
end;
/
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
from test_utility_est.expandv 
where srvr_id not in 
(select srvr_id 
from test_utility_est.expandv 
minus 
select srvr_id 
from test_utility_est.t1)';
    vclobout clob;
begin
    gms_utility.expand_sql_text(vclobin, vclobout);
    raise info 'output_sql_text: %', vclobout; 
end;
/

-- test privileges
create user user01 password 'utility@123';
grant select on test_utility_est.view3 to user01;
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

grant usage on schema test_utility_est to user01;
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
declare
 input_sql_text1 clob := 'select * from test_utility_est.view2';
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

create or replace procedure test_canonicalize(name in varchar2, len in int)
as
declare
    canon_name varchar2(100);
begin
    gms_utility.canonicalize(name, canon_name, len);
    raise info 'canon_name: [%]', canon_name;
end;
/

declare
	canon_name varchar2(100);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

call test_canonicalize('koll.rooy.nuuop.a', 200);

-- empty、space
call test_canonicalize(NULL, 200); -- NULL
call test_canonicalize('', 200); -- NULL
call test_canonicalize('""', 200); -- error
call test_canonicalize('     ', 200);
call test_canonicalize('''', 200); -- error
call test_canonicalize('''''', 200); -- error
call test_canonicalize('"''"', 200);
call test_canonicalize('"''''"', 200);
call test_canonicalize('koll.ro oy.nuuop.a', 200); -- error
call test_canonicalize('koll."ro oy".nuuop.a', 200);
call test_canonicalize('exa mple.tab_na me', 200); -- error
call test_canonicalize('exa   mple', 200); -- error
call test_canonicalize('    .abcd', 200); -- error
call test_canonicalize('koll..nuuop.a', 200); -- error
call test_canonicalize('koll."".nuuop.a', 200); -- error
call test_canonicalize('koll', 200);
call test_canonicalize('"koll"', 200);
call test_canonicalize('   "  koll.rooy"  .nuuop.a', 200);
call test_canonicalize('koll.rooy.nuuop.a', 200);

-- special char
call test_canonicalize('"~!#@@#$%)(#@(!@))<>?/*-+".nuuop.a', 200);
call test_canonicalize('koll.rooy#_$nuuop.a', 200);
call test_canonicalize('koll.ro,oy.nuuop.a', 200); -- error
call test_canonicalize('koll."rooy"  abc.nuuop.a', 200); -- error
call test_canonicalize('koll.rooy.nuuop.', 200); -- error
call test_canonicalize('koll.rooy.nu"uo"p', 200); -- error
call test_canonicalize('koll.rooy."nu"u"o"p', 200); -- error
call test_canonicalize('koll."rooy"."nuuop.', 200); -- error
call test_canonicalize('abc.test,c.f', 200); -- error

-- identy
call test_canonicalize('koll.123rooy.nuuop.a', 200); -- error
call test_canonicalize('123', 200);
call test_canonicalize('  123  ', 200);
call test_canonicalize('123abc', 200); -- error
call test_canonicalize('  123.123  ', 200);
call test_canonicalize('"123".123', 200); -- error
call test_canonicalize('"123"."123"', 200); -- error
call test_canonicalize('123."123"', 200);
call test_canonicalize('  123.123456789123456789', 200);
call test_canonicalize('123.123.123', 200); -- error
call test_canonicalize('123.gaussdb', 200); -- error
call test_canonicalize('"123".gaussdb', 200);
call test_canonicalize('gaussdb.123', 200); -- error
call test_canonicalize('gaussdb."123"', 200);
call test_canonicalize('gaussdb."123nuuop"', 200);
call test_canonicalize('koll._rooy.nuuop.a', 200); -- ok for og, error for A
call test_canonicalize('koll.#rooy.nuuop.a', 200); -- error
call test_canonicalize('koll.$rooy.nuuop.a', 200); -- error
call test_canonicalize('koll."_rooy".nuuop.a', 200);
call test_canonicalize('table', 200);
call test_canonicalize('koll.rooy.table.a', 200); -- error
call test_canonicalize('column', 200);
call test_canonicalize('koll.column.nuuop.a', 200); -- error

-- test indentifier overlength; og >= 64, A >= 128
call test_canonicalize('koll.DoYouThinkCausalUnderstandingIsADefiningCharacteristicOfHumanCognition.nuuop.a', 200); -- error
call test_canonicalize('agdbchwnnw_test_adhsd_123_dbscbswcbswcbswjbc$2384243758475_fhdkj', 200); -- error

-- param canon_len
call test_canonicalize('koll.rooy.nuuop.a', -10); -- error
call test_canonicalize('koll.rooy.nuuop.a', 0);
call test_canonicalize('koll.rooy.nuuop.a', 10);
call test_canonicalize('开车.rooy.nuuop.a', 10);

declare
	canon_name varchar2(10);
begin
	gms_utility.canonicalize('koll.rooy.nuuop.a', canon_name, 20);
	raise info 'canon_name: %', canon_name;
end;	-- error
/

drop procedure test_canonicalize;
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
select valid from pg_object where object_type='P' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
alter type s_type add attribute a int;
select valid from pg_object where object_type='P' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='P' and object_oid in (select oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));

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
select valid from pg_object where object_type='B' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
alter table stu add column b int;
select valid from pg_object where object_type='B' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));
call gms_utility.compile_schema('test_utility_compile', false);
select valid from pg_object where object_type='B' and object_oid in (select oid from gs_package where pkgname='pkg' and pkgnamespace = (select oid from pg_namespace where nspname = 'test_utility_compile'));

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
drop package pkg;
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

create or replace procedure test_name_tokenize(name in varchar2)
as
  a varchar2(100);
  b varchar2(100);
  c varchar2(100);
  dblink varchar2(100);
  nextpos int;
begin
  gms_utility.name_tokenize(name, a, b, c, dblink, nextpos);
  raise info 'a: [%], b: [%], c: [%], dblink: [%], nextpos: %', a, b, c, dblink, nextpos;
end;
/

call test_name_tokenize('peer.lokppe.vuumee@ookeyy');

call test_name_tokenize(NULL); -- error
call test_name_tokenize(''); -- error
call test_name_tokenize('""'); -- error
call test_name_tokenize('  .  '); -- error
call test_name_tokenize('"  .  "');
call test_name_tokenize('peer.   lokppe.vuumee@ookeyy');
call test_name_tokenize('peer."   lokppe.vuumee"@ookeyy');
call test_name_tokenize('sco  ot');  -- SCO 5
call test_name_tokenize('"sco  ot"');
call test_name_tokenize('sco  ot.foook'); -- SCO 5
call test_name_tokenize('"sco  ot".fook');
call test_name_tokenize('"sco  ot"   .fook');
call test_name_tokenize('"sco  ot"   abcd.fook'); -- sco  ot 12

-- test support special char
call test_name_tokenize('peer._lokppe.vuumee@ookeyy'); -- ok for og, error for A
call test_name_tokenize('peer.lokp_pe.vuumee@ookeyy');
call test_name_tokenize('peer.$lokppe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer.lokp$pe.vuumee@ookeyy');
call test_name_tokenize('peer.lokp233pe.vuumee@ookeyy');
call test_name_tokenize('peer.233lokppe.vuumee@ookeyy');
call test_name_tokenize('peer.-lokppe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer."-lokppe".vuumee@ookeyy');
call test_name_tokenize('peer.lokp-pe.vuumee@ookeyy'); -- 9
call test_name_tokenize('peer.lokp=pe.vuumee@ookeyy'); -- 9
call test_name_tokenize('peer.=lokppe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer.]lokppe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer.lokp`pe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer.lokp~pe.vuumee@ookeyy'); -- error
call test_name_tokenize('peer.lokp%pe.vuumee@ookeyy'); -- error

call test_name_tokenize('123'); -- error
call test_name_tokenize('"123"');
call test_name_tokenize('  123  '); -- error
call test_name_tokenize('123abc'); -- error
call test_name_tokenize('  123.123  '); -- error
call test_name_tokenize('"123".123');
call test_name_tokenize('"123"."123"');
call test_name_tokenize('  123.123456789123456789'); -- error
call test_name_tokenize('123."123"'); -- error
call test_name_tokenize('123.123.123'); -- error
call test_name_tokenize('123.gaussdb'); -- error
call test_name_tokenize('"123".gaussdb');
call test_name_tokenize('gaussdb.123');
call test_name_tokenize('gauss123.vuumee.123');
call test_name_tokenize('gaussdb."123"');

call test_name_tokenize(''''''); -- error
call test_name_tokenize('"''''"');
call test_name_tokenize(''''); -- error
call test_name_tokenize('"''"'); 
call test_name_tokenize('peer,lokppe,vuumee@ookeyy'); 
call test_name_tokenize('peer.lokppe,vuumee@ookeyy');

-- test keyword
call test_name_tokenize('peer.table.vuumee@ookeyy'); -- error
call test_name_tokenize('peer."table".vuumee@ookeyy');
call test_name_tokenize('peer.column.vuumee@ookeyy'); -- error
call test_name_tokenize('peer."column".vuumee@ookeyy');

-- test @
call test_name_tokenize('peer.lokppe.vuumee@'); -- error
call test_name_tokenize('peer.lokppe.vuumee@_ookeyy'); -- ok for og, error for A
call test_name_tokenize('peer.lokppe.vuumee@"_ookeyy"');
call test_name_tokenize('peer.lokppe.vuumee@$ookeyy');
call test_name_tokenize('peer.lokppe.vuumee@"$ookeyy"');
call test_name_tokenize('peer.lokppe.vuumee@123ookeyy'); -- error
call test_name_tokenize('peer.lokppe.vuumee@ookeyy.zk');
call test_name_tokenize('peer.lokppe.vuumee@ookeyy@zk');
call test_name_tokenize('@vuumee'); -- error
call test_name_tokenize('peer.lokppe.vuumee@ook=eyy');
call test_name_tokenize('peer.lokppe.vuumee@=ookeyy'); -- error
call test_name_tokenize('peer.lokppe.vuumee@ook eyy');
call test_name_tokenize('peer.lokppe.vuumee@ook%eyy'); -- error
call test_name_tokenize('"scott@db1"');
call test_name_tokenize('"scott.@db1"');
call test_name_tokenize('peer."@test"."@name"@dblink');
call test_name_tokenize('test."name#!^&*()-=+[]{}/|:<>@"scott@ins');

-- test double quote
call test_name_tokenize('peer.lokppe.vuumee"@ookeyy"');
call test_name_tokenize('peer.lokppe.vu"ume"e@ookeyy');
call test_name_tokenize('peer.lokppe."vu"ume"e@ookeyy');
call test_name_tokenize('peer.lokppe.vuumee.aking@ookeyy'); -- error

-- test idnetifier overlength. og >= 64, A >= 128
call test_name_tokenize('peer.DoYouThinkCausalUnderstandingIsADefiningCharacteristicOfHumanCognition.vuumee@ookeyy'); -- error

drop procedure test_name_tokenize;
drop schema test_utility_tokenize;

---------------------------
-- name resolve
---------------------------
reset search_path;

-- prepare data
CREATE TABLE public.t_resolve (c1 NUMBER, c2 VARCHAR2(100));
CREATE UNIQUE INDEX IF NOT EXISTS public.t_resolve_c1_udx ON public.t_resolve(c1);
CREATE TABLE public.t_log (c1 NUMBER, c2 TIMESTAMP);
CREATE VIEW public.v_resolve AS SELECT * FROM public.t_resolve;

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
CREATE OR REPLACE SYNONYM public.syn_view FOR v_resolve;
CREATE OR REPLACE SYNONYM public.syn_idx FOR t_resolve_c1_udx;
CREATE OR REPLACE SYNONYM public.syn_seq FOR t_seq;
CREATE OR REPLACE SYNONYM public.syn_fun FOR add_numbers;
CREATE OR REPLACE SYNONYM public.syn_pro FOR insert_val;
CREATE OR REPLACE SYNONYM public.syn_tg FOR log_resolve_after_insert;
CREATE OR REPLACE SYNONYM public.syn_pkg FOR t_pkg;
CREATE OR REPLACE SYNONYM public.syn_typ FOR t_typ;

create or replace procedure test_name_resolve(input in varchar2, context in numeric)
as
  schema varchar2(100);
  part1 varchar2(100);
  part2 varchar2(100);
  dblink varchar2(100);
  part1_type number;
  object_number number;
begin
  gms_utility.name_resolve(input, context, schema, part1, part2, dblink, part1_type, object_number);
  raise info 'schema: [%], part1: [%], part2: [%], dblink: [%], part1_type: %, object_number: %', schema, part1, part2, dblink, part1_type, object_number;
end;
/

-- test table
call test_name_resolve('public.t_resolve', 0);
call test_name_resolve('public.t_resolve', NULL);
call test_name_resolve('t_resolve', 0);
call test_name_resolve('t_resolve', NULL);
call test_name_resolve('public.syn_tbl', 0);
call test_name_resolve('public.syn_tbl', NULL);
call test_name_resolve('syn_tbl', 0);
call test_name_resolve('syn_tbl', NULL);
call test_name_resolve('public.t_resolve', 2);
call test_name_resolve('public.t_resolve', 7);
call test_name_resolve('public.t_resolve', 9); -- error

-- test view
call test_name_resolve('public.v_resolve', 0);
call test_name_resolve('public.v_resolve', NULL);
call test_name_resolve('v_resolve', 0);
call test_name_resolve('v_resolve', NULL);
call test_name_resolve('public.syn_view', 0);
call test_name_resolve('public.syn_view', NULL);
call test_name_resolve('syn_view', 0);
call test_name_resolve('syn_view', NULL);
call test_name_resolve('public.v_resolve', 2);
call test_name_resolve('public.v_resolve', 7);
call test_name_resolve('public.v_resolve', 9); -- error

-- test PL/SQL
call test_name_resolve('public.add_numbers', 1);
call test_name_resolve('add_numbers', 1);
call test_name_resolve('public.syn_fun', 1);
call test_name_resolve('syn_fun', 1);
call test_name_resolve('public.insert_val', 1);
call test_name_resolve('insert_val', 1);
call test_name_resolve('public.syn_pro', 1);
call test_name_resolve('syn_pro', 1);

-- test package
call test_name_resolve('public.t_pkg.multi_number', 1);
call test_name_resolve('public.t_pkg.delete_val', 1);
call test_name_resolve('public.t_pkg.qwer', 1);
call test_name_resolve('t_pkg.multi_number', 1);
call test_name_resolve('t_pkg.delete_val', 1);
call test_name_resolve('t_pkg.qwer', 1);
call test_name_resolve('public.syn_pkg.multi_number', 1);
call test_name_resolve('public.syn_pkg.delete_var', 1);
call test_name_resolve('public.syn_pkg.qwer', 1);
call test_name_resolve('syn_pkg.multi_number', 1);
call test_name_resolve('syn_pkg.delete_var', 1);
call test_name_resolve('syn_pkg.qwer', 1);
call test_name_resolve('public.t_pkg', 1);
call test_name_resolve('t_pkg', 1);
call test_name_resolve('public.syn_pkg', 1);
call test_name_resolve('syn_pkg', 1);

--  test trigger
call test_name_resolve('public.log_resolve_after_insert', 3);
call test_name_resolve('public.syn_tg', 3); -- error
call test_name_resolve('log_resolve_after_insert', 3);
call test_name_resolve('syn_tg', 3); -- error

-- test sequence
call test_name_resolve('public.t_seq', 2);
call test_name_resolve('public.syn_seq', 2);
call test_name_resolve('t_seq', 2);
call test_name_resolve('syn_seq', 2);
call test_name_resolve('public.t_seq', 0); -- error
call test_name_resolve('public.t_seq', 7);
call test_name_resolve('public.t_seq', 9); -- error

-- test type
call test_name_resolve('public.t_typ', 7);
call test_name_resolve('public.syn_typ', 7);
call test_name_resolve('t_typ', 7);
call test_name_resolve('syn_typ', 7);
call test_name_resolve('public.t_typ', 0); -- error
call test_name_resolve('public.t_typ', 2); -- error
call test_name_resolve('public.t_typ', 9); -- error

-- test index
call test_name_resolve('public.t_resolve_c1_udx', 9);
call test_name_resolve('public.syn_idx', 9); -- error
call test_name_resolve('t_resolve_c1_udx', 9);
call test_name_resolve('syn_idx', 9); -- error
call test_name_resolve('public.t_resolve_c1_udx', 0); -- error
call test_name_resolve('public.t_resolve_c1_udx', 2); -- error
call test_name_resolve('public.t_resolve_c1_udx', 7); -- error

-- test java
call test_name_resolve('public.java.class', 4);
call test_name_resolve('public.java.class', 5);
call test_name_resolve('public.java.class', 6);
call test_name_resolve('public.java.class', 8);

-- test with dblink
call test_name_resolve('peer.lokppe.vuumee@ookeyy', 0);
call test_name_resolve('peer@ookeyy', 1);
call test_name_resolve('peer@ookeyy', 2);
call test_name_resolve('peer.@ookeyy', 3); -- error
call test_name_resolve('peer.lokppe@ookeyy', 7);
call test_name_resolve('peer.lokppe@ookeyy', 9);
call test_name_resolve('peer.lokppe@ookeyy', 10);
call test_name_resolve('"look"@"dblink"', 0);
call test_name_resolve('lo ok@dbli nk', 0); -- error
call test_name_resolve('lo=ok@dbli nk', 0); -- error
call test_name_resolve('"123bac"@dblink', 0);
call test_name_resolve('"@bac"@dblink', 0);
call test_name_resolve('"!#$%^&*()_-+={}@[]|\:;'',<>./?~`"@dblink', 0);

-- test context float
call test_name_resolve('peer.lokppe.vuumee@ookeyy', 1.5);
call test_name_resolve('peer.lokppe.vuumee@ookeyy', 3.0);
call test_name_resolve('peer.lokppe.vuumee@ookeyy', 3.05);
call test_name_resolve('peer.lokppe.vuumee@ookeyy', 11);
call test_name_resolve('peer.lokppe.vuumee@ookeyy', -1);

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
DROP VIEW public.v_resolve;
DROP TABLE public.t_log;
DROP TABLE public.t_resolve;
DROP SEQUENCE public.t_seq;

---------------------------
-- format error stack
---------------------------
select gms_utility.format_error_stack();
select gms_utility.format_error_stack(100);

create or replace function t_inner(a int, b int)
returns int as $$
declare
    res int := 0;
begin
    res = a / b;
    return res;
exception
    when others then
    raise exception 'expected exception';
end;
$$ language plpgsql;

create or replace function t_outter(a int, b int)
returns int as $$
declare
    res int := 0;
begin
    res := t_inner(a, b);
    return res;
exception
    when others then
    gms_output.put_line(gms_utility.format_error_stack());
    return -1;
end;
$$ language plpgsql;

select t_outter(100, 2);
select t_outter(100, 0);

-- test stack overflow
create or replace procedure t_recursion(count in out int)
as
begin
    if count < 1000 then
        count := count + 1;
        t_recursion(count);
    else
        t_inner(100, 0);
    end if;
exception
    when others then
    gms_output.put_line(gms_utility.format_error_stack());
end;
/
declare
    c int := 0;
begin
    t_recursion(c);
end;
/

drop procedure t_recursion;
drop function t_outter;
drop function t_inner;

---------------------------
-- format error backtrace
---------------------------
select gms_utility.format_error_backtrace();
select gms_utility.format_error_backtrace(100);

create or replace function t_inner(a int, b int)
returns int as $$
declare
    res int := 0;
begin
    res = a / b;
    return res;
exception
    when others then
    raise exception 'expected exception';
end;
$$ language plpgsql;

create or replace function t_outter(a int, b int)
returns int as $$
declare
    res int := 0;
begin
    res := t_inner(a, b);
    return res;
exception
    when others then
    gms_output.put_line(gms_utility.format_error_backtrace());
    return -1;
end;
$$ language plpgsql;

select t_outter(100, 2);
select t_outter(100, 0);

-- test stack overflow
create or replace procedure t_recursion(count in out int, max in int)
as
begin
    if count < max then
        count := count + 1;
        t_recursion(count, max);
    else
        t_inner(100, 0);
    end if;
exception
    when others then
    gms_output.put_line(gms_utility.format_error_backtrace());
end;
/
declare
    c int := 0;
begin
    t_recursion(c, 5);
end;
/

drop procedure t_recursion;
drop function t_outter;
drop function t_inner;

---------------------------
-- format call stack
---------------------------
select gms_utility.format_call_stack();
select gms_utility.format_call_stack(100);

create or replace function t_inner
returns void as $$
begin
    gms_output.put_line('t_inner call stack: ');
    gms_output.put_line(gms_utility.format_call_stack());
end;
$$ language plpgsql;

create or replace function t_outter
returns void as $$
begin
    t_inner();
end;
$$ language plpgsql;

select t_inner();
select t_outter();

create or replace procedure t_recursion (count in out int, max in int) 
as
begin
    if count < max then
        count := count + 1;
        t_recursion(count, max);
    else
        t_inner();
    end if;
end;
/

declare
    count int := 0;
begin
    t_recursion(count, 5);
end;
/
declare
    count int := 0;
begin
    t_recursion(count, 20);
end;
/

drop procedure t_recursion;
drop function t_outter;
drop function t_inner;

---------------------------
-- get time
---------------------------
select gms_utility.get_time();
select gms_utility.get_time(100);

declare
    t1 number;
    t2 number;
    td number;
    sum bigint := 0;
    i int := 0;
begin
    t1 = gms_utility.get_time();
    for i in 1..1000000 loop
        sum := sum + i * 2 - i / 2;
    end loop;
    t2 = gms_utility.get_time();
    td = t2 - t1;
    gms_output.put_line('costtime: ' || td);
end;
/

---------------------------
-- comma to table
---------------------------
create or replace procedure test_comma_to_table(list in varchar2)
as
    tablen binary_integer := 0;
    tab varchar2[];
    i int;
begin
    gms_utility.comma_to_table(list, tablen, tab);
    gms_output.put_line('table len: ' || tablen);
    for i in 1..tablen loop
        gms_output.put_line('tablename: ' || tab(i));
    end loop;
end;
/

call test_comma_to_table('gaussdb.dept');
call test_comma_to_table('gaussdb.dept, gaussdb.emp, gaussdb.jobhist');
call test_comma_to_table('  gaussdb.dept, gaussdb.emp, gaussdb.jobhist  ');
call test_comma_to_table('gaussdb.dept,    gaussdb.emp, gaussdb.jobhist');

call test_comma_to_table(NULL); -- error
call test_comma_to_table(''); -- error
call test_comma_to_table('    '); -- error
call test_comma_to_table('"     "');
call test_comma_to_table('gaus  sdb.dept'); -- error
call test_comma_to_table('"gaus  sdb".dept');

call test_comma_to_table('gaussdb.dept,,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb.dept,gaussdb.emp,'); -- error
call test_comma_to_table('gaussdb..dept,gaussdb.emp'); -- error

call test_comma_to_table('gaussdb.dept@dblink,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb.dept_dblink,gaussdb.emp');
call test_comma_to_table('gaussdb.dept$dblink,gaussdb.emp');
call test_comma_to_table('gaussdb.dept#dblink,gaussdb.emp');
call test_comma_to_table('gaussdb.dept123dblink,gaussdb.emp');
call test_comma_to_table('gaussdb._deptdblink,gaussdb.emp'); -- ok for og, error for a
call test_comma_to_table('gaussdb.$deptdblink,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb."$deptdblink",gaussdb.emp');
call test_comma_to_table('gaussdb.#deptdblink,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb."#deptdblink",gaussdb.emp');

call test_comma_to_table('gaussdb.123deptdblink,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb."123deptdblink",gaussdb.emp');
call test_comma_to_table('123,gaussdb.emp'); -- error
call test_comma_to_table('123.abc,gaussdb.emp'); -- error

call test_comma_to_table('gaussdb.DoYouThinkCausalUnderstandingIsADefiningCharacteristicOfHumanCognition,gaussdb.emp'); -- error

call test_comma_to_table('gaussdb.table,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb."table",gaussdb.emp');
call test_comma_to_table('gaussdb.column,gaussdb.emp'); -- error
call test_comma_to_table('gaussdb."column",gaussdb.emp');

call test_comma_to_table('"gaussdb.dept,gaussdb.emp"'); -- error
call test_comma_to_table('"gauss"db.d"ept,gaussdb.emp'); -- error
call test_comma_to_table('gauss"db".dept,gaussdb.emp'); -- error

drop procedure test_comma_to_table;

---------------------------
-- exec_ddl_statement
---------------------------
call gms_utility.exec_ddl_statement('create table public.t_exec_ddl (c1 int, c2 text);');
call gms_utility.exec_ddl_statement('alter table public.t_exec_ddl add column c3 boolean default true');

-- test no ddl sql
call gms_utility.exec_ddl_statement('insert into public.t_exec_ddl values (1, 234, 1)'); -- error
call gms_utility.exec_ddl_statement('update public.t_exec_ddl set c2 = 666'); -- error
call gms_utility.exec_ddl_statement('select * from public.t_exec_ddl;'); -- error

-- test invalid sql
call gms_utility.exec_ddl_statement('today is a good day!'); -- error
call gms_utility.exec_ddl_statement(''); -- error
call gms_utility.exec_ddl_statement(null); -- error

call gms_utility.exec_ddl_statement('alter table public.t_exec_ddl drop column c3; alter table public.t_exec_ddl add column c3 boolean default true;');

call gms_utility.exec_ddl_statement('create table public.t_exec_ddl (c1 int, c2 text);'); -- error
call gms_utility.exec_ddl_statement('drop table public.t_exec_ddl');

call gms_utility.exec_ddl_statement(); -- error
call gms_utility.exec_ddl_statement('create table public.t_exec_ddl2 (c1 int, c2 text);', 'test'); -- error

declare 
    parse_string date:='2024-10-1';
begin
    gms_utility.exec_ddl_statement(parse_string);
end; -- error
/

---------------------------
-- get_hash_value
---------------------------
select gms_utility.get_hash_value('Today is a good day', 1000, 1024);
select gms_utility.get_hash_value('Today is a good day', NULL, 2048); -- error
select gms_utility.get_hash_value('Today is a good day', '', 2048); -- error
select gms_utility.get_hash_value('Today is a good day', 1000, NULL); -- error
select gms_utility.get_hash_value('Today is a good day', 1000, ''); -- error
select gms_utility.get_hash_value('Today is a good day', 1000, 0); -- error
select gms_utility.get_hash_value('Today is a good day', 0, 2048);
select gms_utility.get_hash_value('Today is a good day', -1000, 2048);
select gms_utility.get_hash_value('Today is a good day', 2147483647, 2048);
select gms_utility.get_hash_value('Today is a good day', 2147483648, 2048); -- error, OVER int32
select gms_utility.get_hash_value('Today is a good day', 1024, 2147483647);
select gms_utility.get_hash_value('Today is a good day', 1024, 2147483648); -- error, OVER int32
select gms_utility.get_hash_value('Today is a good day', 1024, -2147483648.4); -- error, OVER int32

select gms_utility.get_hash_value(null, 1000, 1024);
select gms_utility.get_hash_value('', 1000, 1024);

select gms_utility.get_hash_value('select 1;', 0, 100);

select gms_utility.get_hash_value(); -- error
select gms_utility.get_hash_value('2024-11-27'); -- error
select gms_utility.get_hash_value('2024-11-27', 'test'); -- error
select gms_utility.get_hash_value('2024-11-27', 0); -- error
select gms_utility.get_hash_value('2024-11-27', 0, 'test'); -- error
select gms_utility.get_hash_value('2024-11-27', 0, 100, 200); -- error

---------------------------
-- table_to_comma
---------------------------
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: ['|| list || ']');
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '';
    tab(2) := '';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: ['|| list || ']');
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '  ';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: ['|| list || ']');
end;
/
declare
    tablen  integer;
    list    varchar2;
begin
    gms_utility.table_to_comma(NULL, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end; -- error
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := 'build';
    tab(2) := 'test';
    tab(3) := 'date';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '  build';
    tab(2) := 'test  ';
    tab(3) := '  date';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '@build';
    tab(2) := '$test  ';
    tab(3) := '-  date';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '123build';
    tab(2) := '(test  ';
    tab(3) := '&date';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := '123build';
    tab(2) := '(test  ';
    tab(3) := '&date';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2[];
    tablen  integer;
    list    varchar2;
begin
    tab(1) := 'table';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end;
/
declare
    tab varchar2;
    tablen  integer;
    list    varchar2;
begin
    tab := 'table';
    gms_utility.table_to_comma(tab, tablen, list);
    gms_output.put_line('tablen: '|| tablen ||', result: '|| list);
end; -- error
/
declare
    tab varchar2[];
    tablen  integer;
begin
    tab(1) := 'build';
    gms_utility.table_to_comma(tab, tablen);
end; -- error
/

reset behavior_compat_options;
drop extension gms_output;
drop extension gms_utility cascade;
reset search_path;
