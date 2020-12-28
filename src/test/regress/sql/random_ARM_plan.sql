create schema random_plan;
set current_schema = random_plan;
--create table and index
create table random_plan_t1(c1 int, c2 char(10)) distribute by hash(c1);
create table random_plan_t2(c1 int, c2 char(10)) distribute by hash(c1);
create table random_plan_t3(c1 int, c2 int, c3 int, c4 regproc);
create table random_plan_t4(c1 int, c2 int, c3 int, c4 regproc);
create UNIQUE index random_plan_t1_index ON random_plan_t1(c1);
create UNIQUE index random_plan_t2_index ON random_plan_t2(c1); 
--insert record
insert into random_plan_t1 select generate_series(1,10), 'row'|| generate_series(1,10);
insert into random_plan_t2 select generate_series(5,10), 'row'|| generate_series(5,10);
insert into random_plan_t3 select generate_series(1, 100), generate_series(1, 100)%50, generate_series(1, 100)%5, 'sin';
insert into random_plan_t4 select generate_series(1, 100), generate_series(1, 100)%50, generate_series(1, 100)%5, 'sin';
analyze random_plan_t1;
analyze random_plan_t2;
analyze random_plan_t3;
analyze random_plan_t4;
--show current plan_mode_seed
show plan_mode_seed;
--explain optimize plan
explain (verbose on, costs off) select count(*) from (select distinct a.c1,b.c1 from random_plan_t1 a left join random_plan_t2 b on a.c1=b.c1 group by a.c1,b.c1 order by a.c1,b.c1);
--set current plan_mode_seed for random plan
set plan_mode_seed=-1;
--explain random plan
select count(*) from (select distinct a.c1,b.c1 from random_plan_t1 a left join random_plan_t2 b on a.c1=b.c1 group by a.c1,b.c1 order by a.c1,b.c1);
--show last random plan seed
select * from plan_seed(); 
--set current plan_mode_seed is last plan seed for reproduce last plan stable
set plan_mode_seed=2114325363;
--explain last plan
explain (verbose on, costs off) select count(*) from (select distinct a.c1,b.c1 from random_plan_t1 a left join random_plan_t2 b on a.c1=b.c1 group by a.c1,b.c1 order by a.c1,b.c1);
--set current plan_mode_seed
set plan_mode_seed=1137357783;
--explain last plan, query_level==1 and distributed_key!=NULL
explain (verbose on, costs off) select distinct(c1+c2) from random_plan_t3;
--set current plan_mode_seed
set plan_mode_seed=877315715;
--explain last plan, query_level==2 and distributed_key!=NULL
explain (costs off) select t.c1, sum(t.c3) from (select t3.c2 c2, t3.c3 c1, sum(t3.c2+t4.c1) c3 from random_plan_t3 t3 join random_plan_t4 t4 on t3.c2=t4.c3 group by 1, 2) as t group by t.c1;
--set current plan_mode_seed
set plan_mode_seed=1183126883;
--explain last plan, query_level==2 and distributed_key==NULL
explain (costs off) select x from (select c4, sum(c1) x from random_plan_t3 group by 1);
--drop table
drop table random_plan_t1;
drop table random_plan_t2;
drop table random_plan_t3;
drop table random_plan_t4;
--create table

CREATE TABLE fvt_distribute_query_base_tables_04 (
    w_id bigint,
    w_name character varying(20),
    w_zip integer
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY HASH (w_id);

CREATE TABLE fvt_distribute_query_base_tables_06 (
    d_id integer,
    d_street_1 character varying(20),
    d_street_2 character varying(20),
    d_name character varying(10),
    d_w_id integer,
    d_city character varying(20)
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY HASH (d_id);

CREATE TABLE fvt_distribute_query_base_tables_07 (
    c_id integer,
    c_first character varying(20),
    c_middle character(2),
    c_zip character(9),
    c_d_id integer,
    c_street_1 character varying(20),
    c_city character varying(20),
    c_w_id integer,
    c_street_2 character varying(20)
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY HASH (c_id);

CREATE TABLE fvt_distribute_query_base_tables_05 (
    c_w_id integer,
    c_street_1 character varying(20),
    c_city character varying(20),
    c_zip character(9),
    c_d_id integer,
    c_id numeric(10,3)
)
WITH (orientation=column, compression=low)
DISTRIBUTE BY HASH (c_id)
PARTITION BY RANGE (c_w_id)
(
    PARTITION fvt_distribute_query_base_tables_05_p1 VALUES LESS THAN (6),
    PARTITION fvt_distribute_query_base_tables_05_p2 VALUES LESS THAN (8),
    PARTITION fvt_distribute_query_base_tables_05_p3 VALUES LESS THAN (MAXVALUE)
)
ENABLE ROW MOVEMENT;

CREATE TABLE fvt_distribute_query_base_tables_01 (
    w_name character(10),
    w_street_1 character varying(20),
    w_zip character(9),
    w_id integer
)
WITH (orientation=column, compression=high)
DISTRIBUTE BY HASH (w_id)
PARTITION BY RANGE (w_id)
(
    PARTITION fvt_distribute_query_base_tables_01_p1 VALUES LESS THAN (6),
    PARTITION fvt_distribute_query_base_tables_01_p2 VALUES LESS THAN (8),
    PARTITION fvt_distribute_query_base_tables_01_p3 VALUES LESS THAN (MAXVALUE)
)
ENABLE ROW MOVEMENT;
--create view
CREATE VIEW fvt_distribute_query_tables_04 AS 
(SELECT fvt_distribute_query_base_tables_04.w_id, fvt_distribute_query_base_tables_04.w_name, fvt_distribute_query_base_tables_04.w_zip FROM fvt_distribute_query_base_tables_04 WHERE (fvt_distribute_query_base_tables_04.w_id > 10) UNION ALL SELECT fvt_distribute_query_base_tables_04.w_id, fvt_distribute_query_base_tables_04.w_name, fvt_distribute_query_base_tables_04.w_zip FROM fvt_distribute_query_base_tables_04 WHERE ((fvt_distribute_query_base_tables_04.w_id <= 10) OR (fvt_distribute_query_base_tables_04.w_id IS NULL))) UNION SELECT DISTINCT fvt_distribute_query_base_tables_04.w_id, fvt_distribute_query_base_tables_04.w_name, fvt_distribute_query_base_tables_04.w_zip FROM fvt_distribute_query_base_tables_04 LIMIT 100000;
CREATE VIEW fvt_distribute_query_tables_05 AS 
SELECT (__unnamed_subquery__.c_w_id - 1) AS c_w_id, __unnamed_subquery__.c_street_1, __unnamed_subquery__.c_city, __unnamed_subquery__.c_zip, (__unnamed_subquery__.c_d_id + 1000) AS c_d_id, (__unnamed_subquery__.c_id / (2)::numeric) AS c_id FROM (SELECT (fvt_distribute_query_base_tables_05.c_w_id + 1) AS c_w_id, fvt_distribute_query_base_tables_05.c_street_1, fvt_distribute_query_base_tables_05.c_city, fvt_distribute_query_base_tables_05.c_zip, (fvt_distribute_query_base_tables_05.c_d_id - 1000) AS c_d_id, (fvt_distribute_query_base_tables_05.c_id + fvt_distribute_query_base_tables_05.c_id) AS c_id FROM fvt_distribute_query_base_tables_05) __unnamed_subquery__ WHERE (__unnamed_subquery__.c_street_1 IS NOT NULL) UNION SELECT 92892 AS c_w_id, NULL::character varying AS c_street_1, 'xzaeqeoi'::character varying AS c_city, 'ytrcxpo'::bpchar AS c_zip, NULL::integer AS c_d_id, 98318 AS c_id FROM fvt_distribute_query_base_tables_01 WHERE (fvt_distribute_query_base_tables_01.w_id = 1);
CREATE VIEW fvt_distribute_query_tables_06 AS 
SELECT a.d_id, b.d_street_1, a.d_street_2, a.d_name, a.d_w_id, a.d_city FROM (fvt_distribute_query_base_tables_06 a LEFT JOIN fvt_distribute_query_base_tables_06 b ON ((((a.d_id + 1) = (b.d_id + 1)) AND (a.d_w_id = b.d_w_id))));
CREATE VIEW fvt_distribute_query_tables_07 AS 
SELECT a.c_id, a.c_first, a.c_middle, a.c_zip, a.c_d_id, a.c_street_1, a.c_city, a.c_w_id, a.c_street_2 FROM fvt_distribute_query_base_tables_07 a WHERE (NOT (EXISTS (SELECT b.c_id, b.c_first, b.c_middle, b.c_zip, b.c_d_id, b.c_street_1, b.c_city, b.c_w_id, b.c_street_2 FROM fvt_distribute_query_base_tables_07 b WHERE (a.c_id = (0 - b.c_id))))) UNION SELECT fvt_distribute_query_base_tables_07.c_id, fvt_distribute_query_base_tables_07.c_first, fvt_distribute_query_base_tables_07.c_middle, fvt_distribute_query_base_tables_07.c_zip, fvt_distribute_query_base_tables_07.c_d_id, fvt_distribute_query_base_tables_07.c_street_1, fvt_distribute_query_base_tables_07.c_city, fvt_distribute_query_base_tables_07.c_w_id, fvt_distribute_query_base_tables_07.c_street_2 FROM fvt_distribute_query_base_tables_07 WHERE (fvt_distribute_query_base_tables_07.c_w_id > 1);
--two level nestloop and the lower nestloop under Materialize
set plan_mode_seed=1747134384;
set enable_mergejoin=0;
set enable_nestloop=1;
set enable_hashjoin=0;
explain (costs off)  
select table_04.w_name , table_05.c_street_1 , table_05.c_w_id+table_06.d_w_id as t_5_6 , table_06.d_city , table_07.c_middle||table_07.c_d_id as t7 
from fvt_distribute_query_tables_04 as table_04 
inner join fvt_distribute_query_tables_05 as table_05 on table_05.c_w_id=table_04.w_id 
left outer join fvt_distribute_query_tables_06 as table_06 on table_06.d_id=table_05.c_d_id and table_06.d_w_id =table_05.c_w_id and table_05.c_id<200 
right outer join fvt_distribute_query_tables_07 as table_07 on table_07.c_id =table_06.d_id and  table_07.c_w_id=table_06.d_w_id and substring(table_07.c_street_1,1,1)=substring(table_05.c_street_1,1,1) 
order by table_04.w_name , table_05.c_street_1 , t_5_6 , table_06.d_city , t7 NULLS FIRST ;
--drop table
drop view fvt_distribute_query_tables_04;
drop view fvt_distribute_query_tables_05;
drop view fvt_distribute_query_tables_06;
drop view fvt_distribute_query_tables_07;
drop table fvt_distribute_query_base_tables_01;
drop table fvt_distribute_query_base_tables_04;
drop table fvt_distribute_query_base_tables_05;
drop table fvt_distribute_query_base_tables_06;
drop table fvt_distribute_query_base_tables_07;
--reset plan_mode_seed is optimize mode
reset plan_mode_seed;
--drop schema
reset current_schema;
drop schema random_plan cascade;
