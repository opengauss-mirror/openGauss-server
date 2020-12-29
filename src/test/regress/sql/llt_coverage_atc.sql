-- add test case for exchange_parameters function
drop schema if exists test_anonymous_block cascade;
CREATE SCHEMA test_anonymous_block;
create table tmp_tb1_llt_coverage_atc( a int);
insert into tmp_tb1_llt_coverage_atc values(1);
create table test_anonymous_block.test_table_1(
    ID       INTEGER,
    NAME     varchar2(20),
    AGE      INTEGER,
    ADDRESS  varchar2(20),
    TELE     varchar2(20)
);
insert into test_anonymous_block.test_table_1 values(1,'leon',10,'adsf');
insert into test_anonymous_block.test_table_1 values(2,'mary',20,'zcv','234');
insert into test_anonymous_block.test_table_1 values(3,'mike',30,'zcv','567');

DO $$
DECLARE
  url varchar:='hello';
  i integer;
  D_SQL varchar(200);
  arr integer[]:='{43,6,8,10}';
  MYINTEGER INTEGER;
  MYCHAR   VARCHAR2(20);
BEGIN
  FOR i IN 1..3
  LOOP
    RAISE NOTICE 'hi';
  END LOOP;
  RAISE NOTICE '%',url;
  IF exists (select * from information_schema.tables where table_name='_x') then drop table if exists _x;
  end if;
  unnest(arr);
  create table _x(q integer);
  insert into _x select * from generate_subscripts(arr,1);
  D_SQL := 'begin 
    perform * from _x;
  end;';
  EXECUTE IMMEDIATE D_SQL;
  MYINTEGER := 1;
  D_SQL := 'begin select name into :b from test_anonymous_block.test_table_1 where id = :a ; end;';
  EXECUTE IMMEDIATE D_SQL USING   out MYCHAR,IN MYINTEGER;
  raise info 'NAME is %', MYCHAR;
END
$$;

--error condition
DO $$
DECLARE
  D_SQL varchar(200);
  MYINTEGER INTEGER;
  MYCHAR   VARCHAR2(20);
BEGIN
  MYINTEGER := 1;
  D_SQL := 'declare tmp integer; begin select name from test_anonymous_block.test_table_1 where id = :a into :b; end;';
  EXECUTE IMMEDIATE D_SQL USING IN MYINTEGER, out MYCHAR;
  raise info 'NAME is %', MYCHAR;
END
$$;

DO $$
DECLARE
  D_SQL varchar(200);
  MYINTEGER INTEGER;
  MYCHAR   VARCHAR2(20);
BEGIN
  MYINTEGER := 1;
  D_SQL := 'begin select name from test_anonymous_block.test_table_1 where id = :a into :b; end;';
  EXECUTE IMMEDIATE D_SQL USING IN MYINTEGER, MYCHAR;
  raise info 'NAME is %', MYCHAR;
END
$$;
DO $$
DECLARE
  D_SQL varchar(200);
  MYINTEGER INTEGER;
  MYCHAR   VARCHAR2(20);
BEGIN
  MYINTEGER := 1;
  D_SQL := 'begin select name from test_anonymous_block.test_table_1 where id = :b into :a; end;';
  EXECUTE IMMEDIATE D_SQL into MYCHAR USING IN MYINTEGER;
  raise info 'NAME is %', MYCHAR;
END
$$;
drop table test_anonymous_block.test_table_1;

--add testcase for mark_windowagg_stream function
create table test_mark_windowagg_stream_into (
  q1 int,
  q2 int
);
create table test_mark_windowagg_stream (
  q1 int,
  q2 int
);
insert into test_mark_windowagg_stream values(1,2);
insert into test_mark_windowagg_stream values(3,4);
insert into test_mark_windowagg_stream values(2,4);
insert into test_mark_windowagg_stream values(1,3);
insert into test_mark_windowagg_stream values(1,2);
insert into test_mark_windowagg_stream values(3,4);
insert into test_mark_windowagg_stream values(2,4);
insert into test_mark_windowagg_stream values(1,3);
insert into test_mark_windowagg_stream_into (select q1,q2/sum(q2) over (partition by q2::int8) from test_mark_windowagg_stream);
drop table test_mark_windowagg_stream_into;
drop table test_mark_windowagg_stream;

-- add test case for locate_distributekey_from_tlist function
SET CHECK_FUNCTION_BODIES TO ON;
create or replace function llt_func_01
(
  param1 in integer,
  param2 in integer
)
returns integer as $$
declare
  ret integer;
begin
  ret := param1 + param2;
  return ret;
END;
$$ LANGUAGE plpgsql;
drop table if exists llt_locate_distributekey_from_tlist1;
drop table if exists llt_locate_distributekey_from_tlist2;
create table llt_locate_distributekey_from_tlist1 
(
  q1 integer,
  q2 integer
);
create table llt_locate_distributekey_from_tlist2
(
  q1 integer,
  q2 integer
);
insert into llt_locate_distributekey_from_tlist1 values(1,2);
insert into llt_locate_distributekey_from_tlist2 values(3,1);
insert into llt_locate_distributekey_from_tlist2 values(3,0);
analyze llt_locate_distributekey_from_tlist1;
analyze llt_locate_distributekey_from_tlist2;
select * from llt_locate_distributekey_from_tlist1 where q1 in (select llt_func_01(q1,q2) from llt_locate_distributekey_from_tlist2 group by llt_func_01(q1,q2) order by 1);

-- add test case for explain node
\o xml_explain_temp.txt
explain performance select * from llt_locate_distributekey_from_tlist1 where q1 in (select llt_func_01(q1,q2) from llt_locate_distributekey_from_tlist2 group by llt_func_01(q1,q2) order by 1);
explain performance update llt_locate_distributekey_from_tlist1 set q2 = 3 where q1 = 3;
\o
drop function llt_func_01();
drop table llt_locate_distributekey_from_tlist1;
drop table llt_locate_distributekey_from_tlist2;

-- add test case for functions: show_datanode_buffers, show_detail_cpu, show_hash_info
drop table if exists llt_show_datanode_buffers;
drop table if exists llt_show_datanode_buffers_1;
create table llt_show_datanode_buffers(q1 int, q2 int);
create table llt_show_datanode_buffers_1(q1 int, q2 int);
create table llt_show_datanode_buffers_2(q1 int, q2 int);
create table llt_show_datanode_buffers_3(q1 int, q2 int) distribute by replication;
insert into llt_show_datanode_buffers select generate_series(1,1000,9), generate_series(50,500,9) from tmp_tb1_llt_coverage_atc;
insert into llt_show_datanode_buffers select generate_series(100,700,6), generate_series(500,1000,6) from tmp_tb1_llt_coverage_atc;
insert into llt_show_datanode_buffers_1 select generate_series(1,1000,6), generate_series(50,500,6) from tmp_tb1_llt_coverage_atc;
insert into llt_show_datanode_buffers_1 select generate_series(100,700,9), generate_series(500,1000,9) from tmp_tb1_llt_coverage_atc;
set enable_hashjoin=on;
set enable_nestloop=off;
set enable_mergejoin=off;
explain (ANALYZE on, BUFFERS on, TIMING on, detail off, format text) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (ANALYZE on, BUFFERS on, TIMING off, detail off, format JSON ) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (ANALYZE on, BUFFERS on, TIMING off, detail off, format text, cpu on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (ANALYZE on, BUFFERS on, TIMING off, detail off, format JSON, cpu on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (ANALYZE on, BUFFERS on, TIMING on, detail on, format text) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;

--add for llt
explain (cpu on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (detail on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (analyze on, detail on,format xml,costs off) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1;
explain (analyze on, detail on,format xml,timing  on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1 and llt_show_datanode_buffers.q1 > 1000;

explain (analyze on, detail on,format xml,timing  off) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_1 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_1.q1 and llt_show_datanode_buffers.q1 > 1000;


explain (analyze on, detail on,format xml,cpu  on) select * from llt_show_datanode_buffers, llt_show_datanode_buffers_2 where llt_show_datanode_buffers.q2=llt_show_datanode_buffers_2.q1 and llt_show_datanode_buffers.q1 > 1000;

create table coalesce_test 
(
	a	int,
	b	character varying(5)
)distribute by hash(a);
insert into coalesce_test select generate_series(1,50),'ABC' from tmp_tb1_llt_coverage_atc;
insert into coalesce_test select generate_series(1,50),'EGT' from tmp_tb1_llt_coverage_atc;
analyze coalesce_test;

explain select * from coalesce_test where coalesce(btrim(b), '')='ABC';
explain select * from coalesce_test where coalesce(ltrim(b), '')='ABC';
explain select * from coalesce_test where coalesce(rtrim(b), '')='EGT';
explain select * from coalesce_test where rtrim(coalesce(b, ''))='EGT';
explain select * from coalesce_test where coalesce(b, a||'1')='ABC'; 
explain select * from coalesce_test where ltrim(coalesce(b, ''))='ABC';
explain select * from coalesce_test where coalesce(cast('12.3' as text),b)='ABC';
explain select * from coalesce_test where coalesce(cast(b as varchar(10)),'1')='ABC';
explain select * from coalesce_test where coalesce(b::text,'1')='ABC';
drop table  coalesce_test;

set enable_hashjoin=on;
set enable_nestloop=on;
set enable_mergejoin=on;
drop table llt_show_datanode_buffers;
drop table llt_show_datanode_buffers_1;

-- test explain analyze
--create table foo_analyze(c int ,d text,e bool,f numeric);
--PREPARE fooplan (int, text, bool, numeric) AS
--INSERT INTO foo_analyze VALUES($1, $2, $3, $4);
--explain analyze EXECUTE fooplan(1, 'Hunter Valley', 't', 200.00);
--select * from foo_analyze;
--drop table foo_analyze;
drop table tmp_tb1_llt_coverage_atc;
