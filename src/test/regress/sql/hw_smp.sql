--
-- SMP
-- Create @ 2017-7-11
--

--dynamic smp+random plan, plan should be same
set plan_mode_seed = 1485495508;
explain (costs off) select count(node_name) as dncnt
	from pgxc_node where node_type='D'
	group by node_host order by dncnt desc
	limit 1;
explain (costs off) select count(node_name) as dncnt
	from pgxc_node where node_type='D'
	group by node_host order by dncnt desc
	limit 1;
set plan_mode_seed = 0;

create schema hw_smp;
set current_schema = hw_smp;
--The join's left and right tree have different execnodes.
--Merge sort when union two list.
create table smp_node_a
(
    a_sk            integer               not null
)
 with (orientation = column)  ;
insert into smp_node_a values(1);

create table smp_node_b
(
    b_item_sk                integer               not null,
    b_sk           integer
 )
 with (orientation = column)  
;
insert into smp_node_b values(1, 1);

create table smp_node_c
(
    c_sk               integer               not null
 )
 with (orientation = column)  ;
insert into smp_node_c values(1);

with tmp1 as
 (select a_sk
    from smp_node_a
   order by 1)
select count(distinct b_item_sk), c_sk
  from (select b_item_sk, a_sk
          from tmp1
          left join smp_node_b
            on b_sk = a_sk)
 inner join smp_node_c
    on c_sk = a_sk
 where c_sk = 1
 group by 2;

--Plan node execute only on some DN.
create table
smp_dummy_plan(item_sk int,
				date_sk smallint,
				ticket_number bigint
)
with (orientation = column)  ;
insert into smp_dummy_plan values(1, 1, 1);
insert into smp_dummy_plan values(1, 2, 3);
insert into smp_dummy_plan values(0, 2, 1);

select date_sk
	from smp_dummy_plan
	where
	exists
	(
		select max(ticket_number)
		from smp_dummy_plan
	)
and item_sk = 1;

--Parallel for array_agg.
create table smp_array_agg(
 a int,
 b int
);
insert into smp_array_agg values(1, generate_series(1, 100));
select array_agg(b order by b) from smp_array_agg group by a;

--The join's left and right tree have different execnodes.
--Merge sort when union two list.

create table smp_partition
(
	a int,
	b int
 )
 with (orientation = column,compression=high)
  
PARTITION BY Range(b) (
        partition p1 values less than(100),
        partition p2 values less than(200),
        partition p6 values less than(maxvalue)
)
;
insert into smp_partition values (1, 500);
insert into smp_partition values (2, 150);
insert into smp_partition values (3, 50);

select count(1) from smp_partition where b < 200 group by b;

--The join key has different type.
create table smp_jointype_a
( a int)
with (orientation = column)
 
;
create table smp_jointype_b
( b numeric)
with (orientation = column)
 
;
explain (verbose on, costs off) select a from smp_jointype_a join smp_jointype_b on a = b group by a;

--Merge append do not support parallel
create table smp_mergeappend1(a1 int, b1 int)
partition by range (a1)
(
partition p1 values less than (100),
partition p4 values less than (maxvalue)
);

create table smp_mergeappend2(a2 int, b2 int)
partition by range (a2)
(
partition p5 values less than (100),
partition p8 values less than (maxvalue)
);

set enable_hashagg = off;
explain (costs off)
with s1 as materialized (
	select a1, sum(b1) total from smp_mergeappend1 join smp_mergeappend2 on a1 = a2 group by a1
),
s2 as materialized (
	select a1, sum(b1) total from smp_mergeappend1 join smp_mergeappend2 on a1 = a2 group by a1
)
select
	a1, sum(total) total
from(
	select * from s1
	union all
	select * from s2
) tmp1
group by a1
order by a1
;

explain (costs off)
with s1 as(
	select a1, sum(b1) total from smp_mergeappend1 join smp_mergeappend2 on a1 = a2 group by a1
),
s2 as(
	select a1, sum(b1) total from smp_mergeappend1 join smp_mergeappend2 on a1 = a2 group by a1
)
select
	a1, sum(total) total
from(
	select * from s1
	union all
	select * from s2
) tmp1
group by a1
order by a1
;
set enable_hashagg = on;

--PG min/max optimization will create subplan which do not support parallel
create table store_sales_extend_min_1t(ss_item_sk int,
                                  ss_sold_date_sk smallint,
                                  ss_ticket_number bigint,
                                  ss_date date,
                                  ss_time time,
                                  ss_timestamp timestamp,
                                  ss_list_price decimal(7,2)
)
with (orientation = column)  ;


create table store_sales_extend_max_1t(ss_item_sk int,
                                  ss_sold_date_sk smallint,
                                  ss_ticket_number bigint,
                                  ss_date date,
                                  ss_time time,
                                  ss_timestamp timestamp,
                                  ss_list_price decimal(7,2)
)
with (orientation = column)  ;

explain (costs off)
select min((select max(ss_date) from store_sales_extend_min_1t) +
		(select min(ss_time) from store_sales_extend_max_1t))
		from store_sales_extend_min_1t;

--The plan node above Gather can not be parallelized

create table item_inventory
(
    location_id number(15,0) ,
    item_inv_dt date
) with (orientation=column)  ;

create table item_inventory_plan
(
    item_inventory_plan_dt date ,
    location_id number(35,0)
)with (orientation=column)  ;

create  table item_store_sold
(
    product_id decimal(15,5) ,
    location_id number(28,0)
)with (orientation=column)  ;

explain (costs off)
select (54 - 9) c1
from
    item_store_sold t1,
	(
	select location_id
    from item_inventory
    union all
    select location_id
    from item_inventory_plan)
group by c1;


drop table if exists t_bitmapor cascade;
drop table if exists tc_bitmapor cascade;
drop table if exists t cascade;
drop table if exists tc cascade;


set enable_seqscan=off;

--update
create table t(c1 int, c2 int, c3 int);

explain (costs off) update t set c2=2;

-- update column store
create table tc(c1 int, c2 int, c3 int)
WITH (ORIENTATION = COLUMN);

explain (costs off) update tc set c2=2;



-- T_BitmapOr
create table t_bitmapor (c1 int, c2 int, c3 int);
create index t_bitmapor_c1 on t_bitmapor (c1);
create index t_bitmapor_c2 on t_bitmapor (c2);
create index t_bitmapor_c3 on t_bitmapor (c3);

explain (costs off) select * from t_bitmapor where c1=1 and c2=1 or c3=1;

explain (costs off) select /*+ rows(t_bitmapor #2000000) */ * from t_bitmapor where c1=1 and c2=1 or c3=1;


-- T_BitmapOr   column store
create table tc_bitmapor (c1 int, c2 int, c3 int) WITH (ORIENTATION = COLUMN);
create index tc_bitmapor_c1 on tc_bitmapor (c1);
create index tc_bitmapor_c2 on tc_bitmapor (c2);
create index tc_bitmapor_c3 on tc_bitmapor (c3);

explain (costs off) select * from tc_bitmapor where c1=1 and c2=1 or c3=1;

explain (costs off) select /*+ rows(tc_bitmapor #2000000) */ * from tc_bitmapor where c1=1 and c2=1 or c3=1;


create table replication_06 (c_id int, c_d_id character(20));
create table replication_01 (c_id int, c_d_id numeric, c_street_1 text)  ;
create table replication_02 (col_int int)  ;

set explain_perf_mode = pretty;

select 1
   from replication_06 t6
  where t6.c_d_id = 5
    and t6.c_id = 50
 minus all
 select 2
   from replication_01 t1
  inner join replication_02 t2
     on t1.c_id = t2.col_int
  where t1.c_d_id = 8
    and t1.c_id = 800;


drop table if exists t_bitmapor cascade;
drop table if exists tc_bitmapor cascade;
drop table if exists t cascade;
drop table if exists tc cascade;
drop table if exists replication_01;
drop table if exists replication_02;
drop table if exists replication_06;

drop schema hw_smp cascade;
