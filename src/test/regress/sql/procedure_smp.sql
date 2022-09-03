drop schema smp_test cascade;
create schema smp_test;
set current_schema=smp_test;

create table t1(id int, val text);
insert into t1 values(1,'abc'),(2,'bcd'),(3,'dafs');

create procedure p1
is
  v_count int;
begin
  execute 'select count(*) from t1';
  execute 'select count(*) from t1 a, t1 b where a.id=b.id';
end;
/

create function f1(out id1 int, out val1 text)
returns setof record
as $$
declare
  id2 int;
  val2 text;
  row_data record;
  execute_query text;
begin
  execute_query='select id, val from t1';
  for row_data in EXECUTE execute_query loop
    id1 = row_data.id;
	val1 = row_data.val;
	return next;
  end loop;
end;
$$LANGUAGE plpgsql not fenced;

create function f2(out id1 int, out val1 text)
returns setof record
as $$
declare
  id2 int;
  val2 text;
  row_data record;
  execute_query text;
  res int;
begin
  execute_query='select id, val from t1';
  for row_data in EXECUTE execute_query loop  -- first query
    id2 = row_data.id;
    val1 = row_data.val;
	select id from t1 where id=id2 into id1;   -- second query
    return next;
  end loop;
end;
$$LANGUAGE plpgsql not fenced;

create procedure p2
is
  v_count int;
begin
  execute 'select count(*) from t1';
  execute 'select * from f2()';
end;
/

create or replace procedure p5()
is
begin
    create temp table tmp_t1(id int, val text);
	insert into tmp_t1 values(generate_series(1,10000), random()*10000);
	execute 'select count(*) from tmp_t1';
	prepare smp_s2 as select * from t1 where id=$1;
	execute 'execute smp_s2(1)';
	execute 'execute smp_s2(2)';
	execute 'execute smp_s2(4)';
end;
/

create or replace procedure p6()
is
begin
    execute 'select count(*) from t1';
    set query_dop=1002;
    execute 'select count(*) from t1';
end;
/

create or replace procedure p7()
is
begin
    set query_dop=1;
    execute 'select count(*) from t1';
    set query_dop=1004;
    call p6();
    execute 'select count(*) from t1';
    set query_dop=1004;
end;
/

set enable_auto_explain=true;
set auto_explain_level=log; 
set client_min_messages=log;

set query_dop=1004;
set sql_beta_feature='enable_plsql_smp';

set current_schema=smp_test;

select * from p1();
select * from f1();
explain select * from t1,f1() f2 where t1.id=f2.id1;
select * from t1,f1() f2 where t1.id=f2.id1;
select * from f2();
select * from p2();
select f2() from t1;
call p5();
call p7();

set current_schema=public;
set enable_indexscan=off;
set enable_bitmapscan=off;
drop schema smp_test cascade;
