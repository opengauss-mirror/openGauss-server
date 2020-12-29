create schema slot_getsomeattrs_1;
set search_path to slot_getsomeattrs_1;

create table t1(a int, b int, c int);
insert into t1 values(generate_series(1,10),generate_series(1,2),generate_series(1,2));
set default_statistics_target=100;
analyze t1;
analyze t1((b,c));

drop table if exists t1;
create table t1(a int, b int, c int);
insert into t1 values(generate_series(1,10),generate_series(1,2),generate_series(1,2));
set default_statistics_target=-2;
analyze t1;
analyze t1((b,c));

drop table t1 cascade;
drop schema slot_getsomeattrs_1 cascade;
reset search_path;


create type type_array as (
id int,
name varchar(50),
score decimal(5,2),
create_time timestamp
);

create table slot_getsomeattrs_2(a serial, b type_array[])
partition by range (a)
(partition p1 values less than(100),partition p2 values less than(maxvalue));

create table slot_getsomeattrs_3(a serial, b type_array[]);

insert into slot_getsomeattrs_2(b) values('{}');
insert into slot_getsomeattrs_2(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
analyze slot_getsomeattrs_2;

insert into slot_getsomeattrs_3(b) values('');
insert into slot_getsomeattrs_3(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
select * from slot_getsomeattrs_3 where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;
update slot_getsomeattrs_2 set b=array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)] where b='{}';

create index i_array on slot_getsomeattrs_2(b) local;
select * from slot_getsomeattrs_2 where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;

alter type type_array add attribute attr bool;
SELECT b, LISTAGG(a, ',') WITHIN GROUP(ORDER BY b DESC)
FROM slot_getsomeattrs_2 group by 1;

drop type type_array cascade;
drop table slot_getsomeattrs_2 cascade;
drop table  slot_getsomeattrs_3 cascade;

create table slot_getsomeattrs_4(col1 int, col2 int, col3 text);
create index islot_getsomeattrs_4 on slot_getsomeattrs_4(col1,col2);
insert into slot_getsomeattrs_4 values (0,0,'test_insert');
insert into slot_getsomeattrs_4 values (0,1,'test_insert');
insert into slot_getsomeattrs_4 values (1,1,'test_insert');
insert into slot_getsomeattrs_4 values (1,2,'test_insert');
insert into slot_getsomeattrs_4 values (0,0,'test_insert2');
insert into slot_getsomeattrs_4 values (2,2,'test_insert2');
insert into slot_getsomeattrs_4 values (0,0,'test_insert3');
insert into slot_getsomeattrs_4 values (3,3,'test_insert3');
insert into slot_getsomeattrs_4(col1,col2) values (1,1);
insert into slot_getsomeattrs_4(col1,col2) values (2,2);
insert into slot_getsomeattrs_4(col1,col2) values (3,3);
insert into slot_getsomeattrs_4 values (null,null,null);
select col1,col2 from slot_getsomeattrs_4 where col1=0 and col2=0 order by col1,col2 for update limit 1;
-- drop table slot_getsomeattrs_4 cascade;
drop table slot_getsomeattrs_4 cascade;



CREATE TEMP TABLE slot_getsomeattrs_5 (s1 int);

INSERT INTO slot_getsomeattrs_5 VALUES (42),(3),(10),(7),(null),(null),(1);
CREATE INDEX si ON slot_getsomeattrs_5 (s1);
SET enable_sort = false;

SELECT * FROM slot_getsomeattrs_5 ORDER BY s1;
drop table slot_getsomeattrs_5 cascade;



set current_schema=information_schema;
create table slot_getsomeattrs_6(a int, b int);
insert into slot_getsomeattrs_6 values(1,2),(2,3),(3,4),(4,5);
\d+ slot_getsomeattrs_6
\d+ sql_features
explain (verbose on, costs off) select count(*) from sql_features;
select count(*) from sql_features;

explain (verbose on, costs off) select * from slot_getsomeattrs_6;
select count(*) from slot_getsomeattrs_6;
drop table slot_getsomeattrs_6;
reset current_schema;



set enable_seqscan=off;
set enable_bitmapscan=off;
set enable_material=off;
set enable_beta_opfusion=on;
set enable_beta_nestloop_fusion=on;
create table t1 (c1 int, c2 numeric, c3 numeric, c4 int, colreal real);
create table t2 (c1 int, c2 numeric, c3 numeric, c4 int, colreal real);
create index idx1 on t1(c2);
create index idx2 on t1(c3);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 select generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000), generate_series(10, 100000);
insert into t1 values (1,2,3,5,0),(1,2,3,6,0),(1,3,2,7,0),(1,3,2,8,0);
insert into t2 select * from t1;
create index on t2(c2);
analyze t1;
analyze t2;
explain (verbose on, costs off) select sum(c1) from t1 group by c2;
explain (verbose on, costs off) select count(c1) from t1 where c2=1;
explain (verbose on, costs off) select sum(colreal) from t1 where c2=1;
explain (verbose on, costs off) select sum(c1) as result from t1 where c2=1 having result !=10;
explain (verbose on, costs off) select sum(c1), sum(c2) from t1 where c3 = 1;
explain (verbose on, costs off) select sum(c1)+1 from t1 where c2=1;
explain (verbose on, costs off) select sum(c1+1) from t1 where c2=1;
explain (verbose on, costs off) select sum(c1) from t1 where c2=1 limit 1;

-- agg fusion
drop index idx2;
-- index t1(c2): indexonlyscan
explain (verbose on, costs off) select sum(c2) from t1 where c2=3;
select sum(c2) from t1 where c2=3;
drop table t1 cascade;
drop table t2 cascade;