create schema distribute_dml;
set current_schema = distribute_dml;
create table src(a int);
insert into src values(1);

-- Prepared data 
create table distribute_source_hash_01(c1 int, c2 numeric, c3 char(10));
create table distribute_target_hash_01(c1 int, c2 numeric, c3 char(10));
create table distribute_source_hash_02(c1 int, c2 numeric, c3 char(10));
create table distribute_target_hash_02(c1 numeric, c2 int, c3 char(10));
create table distribute_source_replication_01(c1 int, c2 int, c3 char(10));
create table distribute_target_replication_01(c1 int, c2 int, c3 char(10));


insert into distribute_source_hash_02 select generate_series(500,1000,10), generate_series(500,1000,10), 'row'|| generate_series(1,51) from src;
insert into distribute_source_hash_01 select generate_series(1,1000), generate_series(1,1000), 'row'|| generate_series(1,1000) from src;
insert into distribute_source_replication_01 select generate_series(500,1000,8), generate_series(500,1000,8), 'row'|| generate_series(1,100) from src;

analyze distribute_source_hash_01;
analyze distribute_source_replication_01;
analyze distribute_source_hash_02;
---------------------------- distribute insert -------------------------------------
--hash source matchs target distribute keys
explain (verbose on, costs off) insert into distribute_target_hash_01 select * from distribute_source_hash_01;
insert into distribute_target_hash_01 select * from distribute_source_hash_01;

-- hash source doesn't match target distribute keys
explain (verbose on, costs off) insert into distribute_target_hash_02 select * from distribute_source_hash_01;
insert into distribute_target_hash_02 select * from distribute_source_hash_01;

-- replicate target, hashed source
explain (verbose on, costs off) insert into distribute_target_replication_01 select * from distribute_source_hash_01;
insert into distribute_target_replication_01 select * from distribute_source_hash_01;

-- replicate source, hashed target
explain (verbose on, costs off) insert into distribute_target_hash_01 select * from distribute_source_replication_01;
insert into distribute_target_hash_02 select * from distribute_source_replication_01;

-- replicate source, replicate target
explain (verbose on, costs off) insert into distribute_target_replication_01 select * from distribute_source_replication_01;
insert into distribute_target_replication_01 select * from distribute_source_replication_01;

-- join source
explain (verbose on, costs off) insert into distribute_target_hash_01 select t1.c1, t1.c2, t1.c3 from distribute_source_hash_01 t1 join distribute_source_hash_02 t2 on t1.c2=t2.c1 where t2.c2=500;
insert into distribute_target_hash_01 select t1.c1, t1.c2, t1.c3 from distribute_source_hash_01 t1 join distribute_source_hash_02 t2 on t1.c2=t2.c1 where t2.c2=500;

create table distribute_table_01
(
	c1 int,
	c2 varchar(20)
);

create table distribute_partition_table_01
(
	c1 int,
	c2 int,
	c3 varchar(20)
);

insert into distribute_table_01 values
		(1, 'fewfw'),
		(2, 'bbbb'),
		(3, 'cccc'),
		(4, 'cccc'),
		(5, 'cccc');

insert into distribute_partition_table_01 values
		(1, 12,'fewfw'),
		(2, 105,'bbbb'),
		(3, 500,'cccc'),
		(4, 10,'cccc'),
		(5, 600,'cccc');

create table distribute_table_02
(
	c1 int,
	c2 varchar(20)
);

create table distribute_partition_table_02
(
	c1 int,
	c2 int,
	c3 varchar(20)
)
partition by range(c2)
(
	partition p1 values less than(100),
	partition p2 values less than(1000)
);

insert into distribute_table_02 select * from distribute_table_01;
insert into distribute_partition_table_02 select * from distribute_partition_table_01;

---------------------------- distribute update -------------------------------------
explain (verbose on, costs off) update distribute_target_hash_02 set c2 = c2 + 10 where c1 > 500;
explain (verbose on, costs off) update distribute_target_replication_01 set c2 = c2 + 10 where c2 > 500;
explain (verbose on, costs off) update distribute_target_hash_01 t1  set t1.c2 = t1.c2 + 10 from distribute_source_hash_01 S1
	where t1.c1 = s1.c2 and s1.c1 < 100;
explain (verbose on, costs off) update distribute_target_hash_01 t1 set (t1.c2, t1.c3) = (t1.c2 + 10 , ' ') from distribute_source_hash_01 s1
	where t1.c1 = s1.c2 and s1.c1 > 600;   
explain (verbose on, costs off) update distribute_target_hash_01 t1  set t1.c2 = t1.c2 + 10 from distribute_source_hash_01 S1
	where t1.c2 = s1.c1 and s1.c1 = 100;
explain (verbose on, costs off) update distribute_target_hash_01 t1  set t1.c2 = t1.c2 + 10
	from distribute_source_hash_01 S1, distribute_source_hash_01 S2
	where t1.c1 = s1.c2 + s2.c2 and s1.c1 < 100 and s1.c1 = s2.c1;
update distribute_target_hash_01 set c1 = c1 + 10 where c1 > 500;
update distribute_target_replication_01 set c2 = c2 + 10 where c2 > 500;
update distribute_target_hash_02 T2  set t2.c1 = t2.c1 + 10 from distribute_source_hash_01 S1
	where t2.c1 = s1.c2 and s1.c1 < 100;

update distribute_target_hash_01 t1 set (t1.c2, t1.c3) = (t1.c2 + 10 , ' ') from distribute_source_hash_01 s1
	where t1.c1 = s1.c2 and s1.c1 > 600;   
update distribute_target_hash_01 t1  set t1.c2 = t1.c2 + 10 from distribute_source_hash_01 S1
	where t1.c2 = s1.c1 and s1.c1 = 100;
update distribute_target_hash_01 t1  set t1.c2 = t1.c2 + 10
	from distribute_source_hash_01 S1, distribute_source_hash_01 S2
	where t1.c1 = s1.c2 + s2.c2 and s1.c1 < 100 and s1.c1 = s2.c1;

-------------------------- distribute delete -------------------------------------
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01;
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 WHERE c1 = 400;
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 WHERE c1 < 50;
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 WHERE c1 in (select c2 from distribute_source_hash_01 where c1 < 50);
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 WHERE c3 in (select c2 from distribute_source_hash_01);
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_source_hash_01.c1 < 200;
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_source_hash_01.c1 < 200;
explain (verbose on, costs off) DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c2=distribute_source_hash_01.c1 and distribute_source_hash_01.c1 = 200;
DELETE FROM distribute_target_hash_01;
DELETE FROM distribute_target_hash_01 WHERE c1 = 400;
DELETE FROM distribute_target_hash_01 WHERE c1 < 50;
DELETE FROM distribute_target_hash_01 WHERE c1 in (select c2 from distribute_source_hash_01 where c1 < 50);
DELETE FROM distribute_target_hash_01 WHERE c3 in (select c2 from distribute_source_hash_01);
DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_source_hash_01.c1 < 200;
DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c2=distribute_source_hash_01.c1 and distribute_source_hash_01.c1 = 200;
DELETE FROM distribute_target_hash_01 USING distribute_source_hash_01 where distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_target_hash_01.c1=distribute_source_hash_01.c2 and distribute_source_hash_01.c1 < 200;

--added for llt
--test tid scan for replication
explain (verbose on, costs off)  select * from  distribute_source_replication_01 where ctid='(0,1)';
explain (verbose on, costs off) select * from pg_class  where ctid='(0,1)';


 --test _outValuesScan
show client_min_messages;
VALUES (1, 'one'), (2, 'two'), (3, 'three');


create table stream_UI_diskey1(b1 int);
insert into stream_UI_diskey1 select generate_series(1,100) from src;
explain (verbose on, costs off) insert into stream_UI_diskey1 select b1 + 100 from stream_UI_diskey1;
insert into stream_UI_diskey1 select b1 + 100 from stream_UI_diskey1;
select count(*) from stream_UI_diskey1 where b1 = 100;
select count(*) from stream_UI_diskey1 where b1+1 = 1+ 110;
select count(*) from stream_UI_diskey1 where b1 = 110;


--insert update delete distriburte keys
create table insert_tb1 (a int, b int, c int);
create table insert_tb2 (a int, b int, c int);
explain (costs off, verbose on) insert into insert_tb1  select * from insert_tb2;
--Need redistribute
explain (costs off, verbose on) insert into insert_tb1 (a, c) select a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (b, c) select a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (a, b, c) select b, a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (b, a, c) select b, a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (b, a, c) select a, b, c from insert_tb2;
drop table insert_tb1;
drop table insert_tb2;

create table insert_tb1 (a int, b int, c int);
create table insert_tb2 (a int, b int, c int);
explain (costs off, verbose on) insert into insert_tb1  select * from insert_tb2;
explain (costs off, verbose on) delete from insert_tb1 where (a, b ,c) in (select * from insert_tb2);
explain (costs off, verbose on) delete from insert_tb1 where (a, b) in (select a, b from insert_tb2);
insert into insert_tb2 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
insert into insert_tb1  select * from insert_tb2;
select count(*) from insert_tb1;
delete from insert_tb1 where (a, b ,c) in (select * from insert_tb2);
select * from insert_tb1;
insert into insert_tb1  select * from insert_tb2;
select count(*) from insert_tb1;
delete from insert_tb1 where (a, b) in (select a, b from insert_tb2);
select count(*) from insert_tb1;
drop table insert_tb1;
drop table insert_tb2;

create table insert_tb1 (a int, b int, c int);
create table insert_tb2 (a int, b int, c int);
explain (costs off, verbose on) insert into insert_tb1  select * from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (a, b, c) select b, a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb1 (b, a, c) select b, a, c from insert_tb2;
explain (costs off, verbose on) insert into insert_tb2 select * from insert_tb1;
drop table insert_tb1;
drop table insert_tb2;

create table delete_t1(a int, b int, c int);
create table delete_t2(a int, b int, c int) ;
insert into delete_t1 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
insert into delete_t2 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
analyze delete_t1;
analyze delete_t2;
delete from delete_t1 where b in (select b from delete_t2);
select * from delete_t1;

drop table delete_t1;
drop table delete_t2;
create table delete_t1(a int, b int, c int);
create table delete_t2(a int, b int, c int);
insert into delete_t1 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
insert into delete_t2 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
analyze delete_t1;
analyze delete_t2;
explain (verbose on, costs off) delete from delete_t1 where (a, b) in (select b, a from delete_t2);
delete from delete_t1 where (a, b) in (select b, a from delete_t2);
select * from delete_t1;
drop table delete_t1;
drop table delete_t2;
--hash and replication
create table delete_t1(a int, b int, c int);
create table delete_t2(a int, b int, c int);
explain (verbose on, costs off) delete from delete_t1 where b in (select b from delete_t2);
insert into delete_t1 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
insert into delete_t2 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from src;
delete from delete_t1 where b in (select b from delete_t2);
select * from delete_t1;
drop table delete_t1;
drop table delete_t2;

CREATE TABLE t1_col(a int, b int)with (orientation = column);
CREATE TABLE t2_col(a int, b int)with (orientation = column);
copy t1_col from stdin DELIMITER  '|';
12|34
100|200
10|20
120|130
|
12|
\.
copy t2_col from stdin DELIMITER  '|';
12|34
100|200
10|20
120|130
|
12|
\.
select * from table_skewness('t1_col', 'a');
select * from table_skewness('t2_col', 'a');
select * from table_skewness('t1_col', 'a') order by 1, 2, 3;
select * from table_skewness('t2_col', 'a') order by 1, 2, 3;
set enable_hashjoin=off;
set enable_mergejoin=off;
select * from t1_col as t1 join t2_col as t2 on(t1.a = t2.a or (t1.a is null and t2.a is null)) where getbucket(row(t1.a, t2.a), 'H') is null;
create table test_parttable(c1 int, c2 float, c3 real, c4 text)
partition by range (c1, c2, c3, c4)
(
        partition altertable_rangeparttable_p1 values less than (10, 10.00, 19.156, 'h'),
        partition altertable_rangeparttable_p2 values less than (20, 20.89, 23.75, 'k'),
        partition altertable_rangeparttable_p3 values less than (30, 30.45, 32.706, 's')
);
\d+ test_parttable
drop table t1_col;
drop table t2_col;
drop table test_parttable;

create table create_columnar_table_033 ( c_smallint smallint null,c_double_precision double precision,
c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,
c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,
c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default '0',
c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,
c_varchar22 varchar2(11621),c_numeric2 numeric null ) 
with (orientation=column , compression=high); 
set plan_mode_seed=1591957696;
explain (verbose on, costs off)
delete from create_columnar_table_033 where c_integer in 
(select min(c_integer) from create_columnar_table_033 where c_smallint<c_bigint);
drop table create_columnar_table_033;
reset plan_mode_seed;

-- sdv core: insert into sys table
create table tmp_description as select * from pg_description limit 5;
explain (verbose on, costs off)
insert into pg_description select * from tmp_description;
drop table tmp_description;

--data distribute skew functions and view 



drop schema distribute_dml cascade;
