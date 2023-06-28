create schema force_vector_engine;
set current_schema=force_vector_engine;
create table force_vector_test(id int, val int);
insert into force_vector_test values(generate_series(1, 10000), generate_series(1, 1000));
create table force_vector_test1(id int, val int);
insert into force_vector_test1 select * from force_vector_test;
create index on force_vector_test1(id);
analyze force_vector_test;
analyze force_vector_test1;
create table force_vector_test2(id int, val int) with (orientation=column);
insert into force_vector_test2 select * from force_vector_test;
analyze force_vector_test2;

create function func_add_sql(a int, b int)
returns int
AS $$
declare
    res int;
begin
    select a+b into res;
	return res;
end; $$
LANGUAGE plpgsql;

set try_vector_engine_strategy='force';
explain select count(*) from force_vector_test;
select count(*) from force_vector_test;
explain select count(*) from force_vector_test1 where id=2;
select count(*) from force_vector_test1 where id=2;
explain select count(*) from force_vector_test1 where id=2 and val=2;
select count(*) from force_vector_test1 where id=2 and val=2;
set enable_indexscan=off;
explain select count(*) from force_vector_test1 where id=2;
select count(*) from force_vector_test1 where id=2;
explain select count(*) from func_add_sql(1,2);
select count(*) from func_add_sql(1,2);
explain values (1, 'AAAAA', 'read'),(2, 'BBBBB', 'write') order by 1,2,3;
values (1, 'AAAAA', 'read'),(2, 'BBBBB', 'write') order by 1,2,3;
explain select * from force_vector_test where ctid='(0,1)' order by 2;
select * from force_vector_test where ctid='(0,1)' order by 2;
explain select * from force_vector_test t1, force_vector_test2 t2 where t1.id=t2.id order by t1.id limit 10;
select * from force_vector_test t1, force_vector_test2 t2 where t1.id=t2.id order by t1.id limit 10;

set query_dop=1004;
explain select count(*) from force_vector_test;
set enable_seqscan_dopcost = off;
explain select count(*) from force_vector_test;
set enable_seqscan_dopcost = on;
select count(*) from force_vector_test;
set query_dop=1;

create table force_vector_test3(id int, val int) with(storage_type=ustore);
select count(*) from force_vector_test3;
insert into force_vector_test3 select * from force_vector_test;
analyze force_vector_test3;
explain select count(*) from force_vector_test;
select count(*) from force_vector_test;

create table force_vector_test4(c1 int, c2 double precision, c3 double precision, c4 point);
insert into force_vector_test4(c1, c2, c3) values(20, 2.3, 2.3);
select point(c2, c3) from force_vector_test4 where c1 = 20;
-- Do not use vectorization engine
explain select point(c2, c3) from force_vector_test4 where c1 = 20;

create table force_vector_test5(id int, name varchar(1000));
insert into force_vector_test5 values(1, 'apple');
insert into force_vector_test5 values(2, 'pear');
insert into force_vector_test5 values(3, 'apple pear');
-- Using the Vectorization Engine
explain select count(*) from force_vector_test5 where id =1 or to_tsvector('ngram',name)@@to_tsquery('ngram','pear');
select count(*) from force_vector_test5 where id =1 or to_tsvector('ngram',name)@@to_tsquery('ngram','pear');

create table force_vector_test6(a int, b int, c int);
insert into force_vector_test6 values(1,2,3);
alter table force_vector_test6 drop column b;
insert into force_vector_test6 select * from force_vector_test6;

set try_vector_engine_strategy='off';
drop table force_vector_test;
drop table force_vector_test1;
drop table force_vector_test2;
drop table force_vector_test3;
drop table force_vector_test4;
drop table force_vector_test5;
drop table force_vector_test6;
drop function func_add_sql;
drop schema force_vector_engine cascade;

-- test bpcharlen in pg compatibility
create database pg_length_cmpt_db with dbcompatibility 'PG';
\c pg_length_cmpt_db
set try_vector_engine_strategy='force';
create table force_vt_tb1 (a char(10),b varchar(10));
insert into force_vt_tb1 values('零0','零1二3');
insert into force_vt_tb1 values('','');
insert into force_vt_tb1 values('0','0');
explain(costs off) select length(a),a from force_vt_tb1 order by 1;
select length(a),a,length(b),b from force_vt_tb1 order by 1;

set try_vector_engine_strategy='off';
explain(costs off) select length(a),a from force_vt_tb1 order by 1;
select length(a),a,length(b),b from force_vt_tb1 order by 1;

-- test with column table in codegen case
create table force_vt_tb1_col (a char(10),b varchar(10)) with (orientation=column);
insert into force_vt_tb1_col values('零0','零1二3');
insert into force_vt_tb1_col values('','');
insert into force_vt_tb1_col values('0','0');
select length(a),a,length(b),b from force_vt_tb1_col order by 1;
set enable_codegen to true;
set codegen_cost_threshold to 0;
select length(a),a,length(b),b from force_vt_tb1_col order by 1;

drop table force_vt_tb1;
drop table force_vt_tb1_col;
\c regression;
drop database pg_length_cmpt_db;

-- Do not use vectorization engine
create table force_vector_sysdomain(a information_schema.cardinal_number, b int);
insert into force_vector_sysdomain values(10, 20);
select a,b from force_vector_sysdomain;
set try_vector_engine_strategy='force';
select a,b from force_vector_sysdomain;
set try_vector_engine_strategy='optimal';
select a,b from force_vector_sysdomain;
drop table force_vector_sysdomain;
set try_vector_engine_strategy='off';
