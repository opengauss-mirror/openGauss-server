/*
 * This file is used to test the function of ExecVecAggregation()---(2)
 */
----
--- Create Table and Insert Data
----
create schema vector_agg_engine_second;
set current_schema=vector_agg_engine_second;
set time zone prc;
set time zone prc;
set datestyle to iso;

create table vector_agg_engine_second.VECTOR_AGG_TABLE_04
(
   col_id	integer
  ,col_place	varchar
)with (orientation=column);

COPY VECTOR_AGG_TABLE_04(col_id, col_place) FROM stdin;
12	xian
\N	xian
\N	tianshui
\N	tianshui
6	beijing
6	beijing
4	beijing
8	beijing
\.

create table vector_agg_engine_second.VECTOR_AGG_TABLE_05
(
   col_id	integer
  ,col_place	varchar
)with (orientation=column);

COPY VECTOR_AGG_TABLE_05(col_id, col_place) FROM stdin;
12	tian
\N	tian
\N	xian
\N	tshui
6	beijing
6	beijing
4	beijing
8	beijing
\.

create table vector_agg_engine_second.VECTOR_AGG_TABLE_06
(
   col_int int
  ,col_int2 int8
  ,col_char char(20)
  ,col_varchar varchar(30)
  ,col_date date
  ,col_num numeric(10,2)
  ,col_num2 numeric(10,4)
  ,col_float float4
  ,col_float2 float8
)WITH (orientation=column) distribute by hash (col_int);

analyze vector_agg_table_04;
analyze vector_agg_table_05;
analyze vector_agg_table_06;


----
--- cas4: Test Agg With NULL Table
----
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06;
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06 group by col_num;

select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06;
select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06 group by col_date;

select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06;
select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06 group by col_int;

select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06;
select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06 group by col_num2;

select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06;
select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06 group by col_num2;


select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06;
select count(col_char),count(col_date),count(col_num2) from vector_agg_table_06 group by col_num;

select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06;
select min(col_int),max(col_varchar),min(col_num2) from vector_agg_table_06 group by col_date;

select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06;
select sum(col_int2),sum(col_float),sum(col_num2) from vector_agg_table_06 group by col_int;

select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06;
select avg(col_int2),avg(col_num),avg(col_float) from vector_agg_table_06 group by col_num2;

select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06;
select count(col_num2),min(col_char),max(col_varchar),sum(col_float),avg(col_num2) from vector_agg_table_06 group by col_num2;

select sum(col_int2) + avg(col_num / 1.20) from vector_agg_table_06 group by col_int;


---
---
explain select count(distinct vector_agg_table_04.*) from vector_agg_table_04;
select count(distinct vector_agg_table_04.*) from vector_agg_table_04; --A.*
select count(vector_agg_engine_second.vector_agg_table_04.*) from vector_agg_engine_second.vector_agg_table_04; --A.B.*
select count(regression.vector_agg_engine_second.vector_agg_table_04.*) from regression.vector_agg_engine_second.vector_agg_table_04; --A.B.C.*
select count(distinct AA.*) from (select a.*, b.* from vector_agg_table_04 a, vector_agg_table_05 b where a.col_id = b.col_id) AA;

----
--- depend on lineitem_vec
----
select sum(L_QUANTITY) a ,l_returnflag from vector_engine.lineitem_vec group by L_returnflag order by a;
select avg(L_QUANTITY) a, sum(l_quantity) b , l_returnflag from vector_engine.lineitem_vec group by L_returnflag order by a;
select l_returnflag, l_linestatus, 
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, 
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge 
from vector_engine.lineitem_vec 
group by l_returnflag, l_linestatus order by 1, 2;

explain (verbose on, costs off) 
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	sum_qty
;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	sum_qty
;

explain (verbose on, costs off) 
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	vector_engine.lineitem_vec
where
	l_shipdate <= date '1998-12-01' - interval '3 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;

select count(max(L_ORDERKEY)) over(), min(L_PARTKEY), c1 from (select L_ORDERKEY, L_PARTKEY, count(L_LINENUMBER) as c1 from vector_engine.lineitem_vec group by L_ORDERKEY, L_PARTKEY) 
group by c1 order by 1, 2, 3;

--
--vechashagg, segnum>1
--


create table vector_agg_engine_second.vector_agg_table_07 (a int, b int) with (orientation=column) DISTRIBUTE BY HASH(a);
insert into vector_agg_engine_second.vector_agg_table_07 values(1,1);
insert into vector_agg_engine_second.vector_agg_table_07 values(2,2);
insert into vector_agg_engine_second.vector_agg_table_07 values(2,3);

ANALYZE vector_agg_engine_second.vector_agg_table_07;

update pg_class set reltuples=10000000000 where relname='vector_agg_table_07' ;
update pg_statistic set stadistinct=10000000000 from pg_class where pg_statistic.starelid=pg_class.relfilenode  and pg_class.relname='vector_agg_table_07';
update pg_statistic set stadndistinct=10000000000 from pg_class where pg_statistic.starelid=pg_class.relfilenode  and pg_class.relname='vector_agg_table_07';

set work_mem=6000000;
select sum(vector_agg_table_07.a),count(vector_agg_table_07.b) from vector_agg_table_07 group by vector_agg_table_07.a,vector_agg_table_07.b order by sum;
reset work_mem;

----
--- Clean Resource and Tables
----
drop schema vector_agg_engine_second cascade;
