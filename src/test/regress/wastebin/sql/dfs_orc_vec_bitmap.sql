set enable_global_stats = true;
/*
 * This file is used to test the function of ExecCstoreIndexAnd()/ExecCstoreIndexOr()
 */
----
--- Create Table and Insert Data
----
create schema vector_bitmap_engine;
set current_schema=vector_bitmap_engine;
set enable_indexscan=off;
set enable_seqscan=off;

create table vector_bitmap_engine.ROW_BITMAP_TABLE_01
(
    col_int1	int
   ,col_int2	int
   ,col_int3	int
   ,col_int4	int
   ,col_int5	int
)distribute by hash(col_int1);

create table vector_bitmap_engine.VECTOR_BITMAP_TABLE_01
(
    col_int1	int
   ,col_int2	int
   ,col_int3	int
   ,col_int4	int
   ,col_int5	int
)
with(orientation=orc) tablespace hdfs_ts distribute by hash(col_int1);
create index bitmap_01_b on vector_bitmap_table_01(col_int2);
create index bitmap_01_c on vector_bitmap_table_01(col_int3);
create index bitmap_01_d on vector_bitmap_table_01(col_int4);
create index bitmap_01_e on vector_bitmap_table_01(col_int5);

insert into row_bitmap_table_01 select generate_series(1, 10000), generate_series(1, 10000)/20, generate_series(1, 10000)/50+2, generate_series(1, 10000)%10+5, generate_series(1,10000)/16;

insert into vector_bitmap_table_01 select * from row_bitmap_table_01;

create table vector_bitmap_engine.ROW_BITMAP_TABLE_02
(
    col_int1	int
   ,col_int2	int
   ,col_int3	int
   ,col_int4	int
   ,col_int5	int
)
distribute by hash(col_int1)
partition by range (col_int1)
(
    partition p1 values less than (2000),
    partition p2 values less than (4000),
    partition p3 values less than (6000),
    partition p4 values less than (8000),
    partition p5 values less than (12000)
);


create table vector_bitmap_engine.VECTOR_BITMAP_TABLE_02
(
    col_int1	int
   ,col_int2	int
   ,col_int3	int
   ,col_int4	int
   ,col_int5	int
)with(orientation=column) 
distribute by hash(col_int1)
partition by range (col_int1)
(
    partition p1 values less than (2000),
    partition p2 values less than (4000),
    partition p3 values less than (6000),
    partition p4 values less than (8000),
    partition p5 values less than (12000)
);
create index bitmap_02_b on vector_bitmap_table_02(col_int2) local;
create index bitmap_02_c on vector_bitmap_table_02(col_int3) local;
create index bitmap_02_d on vector_bitmap_table_02(col_int4) local;
create index bitmap_02_e on vector_bitmap_table_02(col_int5) local;

insert into row_bitmap_table_02 select generate_series(1, 10000), generate_series(1, 10000)/22, generate_series(1, 10000)/50+1, generate_series(1, 10000)%5+22, generate_series(1,10000)/18;

insert into vector_bitmap_table_02 select * from row_bitmap_table_02;

create table vector_bitmap_engine.VECTOR_BITMAP_TABLE_03
(
    col_int1	int
   ,col_num	numeric
   ,col_txt	text
   ,col_int2	int
)with(orientation=orc) tablespace hdfs_ts distribute by hash(col_int1);

create index bitmap_03_1 on vector_bitmap_table_03(col_int1, col_num, col_txt, col_int2);
create index bitmap_03_2 on vector_bitmap_table_03(col_int1, col_num, col_int2);

create table vector_bitmap_engine.vector_bitmap_table_04
( 
    c_id int 
   ,c_d_id	char(20)
   ,c_w_id	varchar
   ,c_first	varchar(16)
   ,c_middle	char(2)
   ,c_last	varchar(16)
   ,c_street_1	varchar(20)
   ,c_street_2	varchar(20)
   ,c_city	varchar(20)
   ,c_zip 	char(9) 
)with (orientation = orc, compression = zlib) tablespace hdfs_ts;

insert into vector_bitmap_engine.vector_bitmap_table_04 ( c_d_id,c_w_id,c_street_1,c_street_2,c_city,c_zip) values (generate_series(1,10),null,'','dfbj','ayl2','11398765');
insert into vector_bitmap_engine.vector_bitmap_table_04 ( c_d_id,c_w_id,c_street_1,c_street_2,c_city,c_zip) values (generate_series(23,25),null,'点击发送','ttpbnmv','ama','2234689');
insert into vector_bitmap_engine.vector_bitmap_table_04 ( c_d_id,c_w_id,c_street_1,c_street_2,c_city,c_zip) values (null,null,'asdfiuopqd','￥%&%&',generate_series(50,100),'332190');
insert into vector_bitmap_engine.vector_bitmap_table_04 ( c_d_id,c_w_id,c_street_1,c_street_2,c_city,c_zip) values (generate_series(70,100),90909,'gohfbjfkn','','aba','');

create table vector_bitmap_engine.row_bitmap_table_05(   
    wd_smallint	smallint
   ,wd_int	integer
   ,wd_bigint	bigint
   ,wd_numeric	numeric
   ,wd_real	real
   ,wd_double	double precision
   ,wd_decimal	decimal
   ,wd_varchar	varchar
   ,wd_char	char(30)
   ,wd_nvarchar2	nvarchar2
   ,wd_text	text
   ,wd_date	date	
);

create table vector_bitmap_engine.vector_bitmap_table_05(   
    wd_smallint	smallint
   ,wd_int	integer
   ,wd_bigint	bigint
   ,wd_numeric	numeric
   ,wd_real	real
   ,wd_double	double precision
   ,wd_decimal	decimal
   ,wd_varchar	varchar
   ,wd_char	char(30)
   ,wd_nvarchar2	nvarchar2
   ,wd_text	text
   ,wd_date	date	
)with (orientation=column,max_batchrow= 30700, compression = high)
distribute by replication 
partition by range (wd_date)
(
partition psort_index_06_1 values less than ('20141201'),
partition psort_index_06_2 values less than ('20201201'),
partition psort_index_06_3 values less than (maxvalue)
);

create index psort_index_05_char on vector_bitmap_table_05(wd_varchar) local;

CREATE OR REPLACE PROCEDURE func_bitmap_table_05()
AS
BEGIN
        FOR I IN 0..300 LOOP
                if i = 19 OR i = 29 OR i = 39 OR i = 99 then
                        INSERT INTO vector_bitmap_engine.row_bitmap_table_05 VALUES(2, i, i+1, i+2.2, i+1.11, i+2.15, i*0.001, 'ama', 'hja', 'lmn2'||i, 'beijing'||i, date'2014-5-14'+i+30*i);
				elsif i < 50 OR i > 250 then
                        INSERT INTO vector_bitmap_engine.row_bitmap_table_05 VALUES(5, i+2, i, i+3.3, i+0.55, i+0.16, i*0.002, 'ayl'||i, 'tpa', 'mjn3'||i, 'shenzhen'||i, date'2014-5-14'+i+30*i);
                        INSERT INTO vector_bitmap_engine.row_bitmap_table_05 VALUES(12, i+1, i, i+3.4, i+0.55, i+0.16, i*0.002, 'bhl'||i, 'aaa'||i, 'mjn3'||i, 'shenzhen'||i, date'2014-5-14'+i+30*i);
                else
                        INSERT INTO vector_bitmap_engine.row_bitmap_table_05 VALUES(3, i+2, i, i+3.3, i+0.55, i+0.16, i*0.002, 'bma'||i, 'aba', 'mjn3'||i, 'shenzhen'||i, date'2014-5-14'+i+30*i);
                end if;
        END LOOP;
END;
/
CALL func_bitmap_table_05();

insert into vector_bitmap_table_05 select * from row_bitmap_table_05;

analyze vector_bitmap_table_01;
analyze vector_bitmap_table_02;
analyze vector_bitmap_table_03;
analyze vector_bitmap_table_04;
analyze vector_bitmap_table_05;

----
--- case 1: Ctid + Heap + And + Or
----
explain (verbose on, costs off)
select count(1) from vector_bitmap_table_01; 
select count(1) from vector_bitmap_table_01; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

set explain_perf_mode = run;
\o vec_bitmap_scan.txt
explain performance
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 
\o
\! rm vec_bitmap_scan.txt
reset explain_perf_mode;

select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

set explain_perf_mode = run;
\o vec_bitmap_scan.txt
explain performance
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 
\o
\! rm vec_bitmap_scan.txt
reset explain_perf_mode;

select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_01 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

----
--- case 2: Partitioned Ctid + Heap + And + Or
----
explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 or col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  and col_int4 < 10 order by 1, 2, 3, 4, 5; 

explain (verbose on, costs off)
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 
select * from vector_bitmap_table_02 where col_int2 < 50 and col_int3 < 5  or col_int4 < 10 order by 1, 2, 3, 4, 5; 

----
--- case 3 : Rescan Ctid + Heap + And + Or
----
set enable_hashjoin = off;
set enable_mergejoin = off;
set enable_material = off;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 and t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 and t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
 
explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 or t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where t1.col_int2 < 10 or t1.col_int3 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_02 t2 on t1.col_int1=t2.col_int1 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int2=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int2=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)                                                                                                                                                                     
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int1=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;
select * from vector_bitmap_table_01 t1 inner join vector_bitmap_table_01 t2 on t1.col_int1=t2.col_int2 where (t1.col_int2 < 10 or t1.col_int3 < 12 or t1.col_int4 < 5) and t2.col_int5 < 12 order by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 limit 50;

explain (verbose on, costs off)
select t1.c_city from vector_bitmap_engine.vector_bitmap_table_04 t1 where t1.c_city like 'a%' and t1.c_city in (select t2.wd_varchar from vector_bitmap_engine.vector_bitmap_table_05 t2 where t2.wd_int < 1000 and t2.wd_char like '%a%');
select t1.c_city from vector_bitmap_engine.vector_bitmap_table_04 t1 where t1.c_city like 'a%' and t1.c_city in (select t2.wd_varchar from vector_bitmap_engine.vector_bitmap_table_05 t2 where t2.wd_int < 1000 and t2.wd_char like '%a%');

----
--- case 4 : Plus Expr
----
explain (verbose on, costs off)                                                                                                                                                                     
select * from vector_bitmap_table_03 where (col_int1 in (2, 9, 21) and col_num in (1.5, 2.3) and length(col_txt) < 10) and col_int2 < 10 order by 1, 2, 3, 4;
select * from vector_bitmap_table_03 where (col_int1 in (2, 9, 21) and col_num in (1.5, 2.3) and length(col_txt) < 10) and col_int2 < 10 order by 1, 2, 3, 4;

----
--- Clean Resource and Tables
----
drop schema vector_bitmap_engine cascade;
