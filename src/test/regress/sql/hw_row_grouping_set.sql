create schema hw_row_groupingsets;
set search_path=hw_row_groupingsets;

create table row_gstest1(a int, b int, v int);

insert into row_gstest1 values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),
(2,3,15),  (3,3,16), (3,4,17), (4,1,18), (4,1,19);

create table row_gstest2 (a integer, b integer, c integer, d integer,
                           e integer, f integer, g integer, h integer);

copy row_gstest2 from stdin;
1	1	1	1	1	1	1	1
1	1	1	1	1	1	1	2
1	1	1	1	1	1	2	2
1	1	1	1	1	2	2	2
1	1	1	1	2	2	2	2
1	1	1	2	2	2	2	2
1	1	2	2	2	2	2	2
1	2	2	2	2	2	2	2
2	2	2	2	2	2	2	2
\.
create table row_gstest3 (a integer, b integer, c integer, d integer);
copy row_gstest3 from stdin;
1	1	1	1
2	2	2	2
\.
analyze row_gstest1;
analyze row_gstest2;
analyze row_gstest3;
select a, b, grouping(a,b), sum(v), count(*), max(v)
  from row_gstest1 group by rollup (a,b) order by 1, 2, 3, 4, 5, 6;

-- nesting with window functions
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
  from row_gstest2 group by rollup (a,b)  order by 1, 2, 3, 4;

-- nesting with grouping sets
select sum(c) from row_gstest2 group by grouping sets((), grouping sets((), grouping sets(()))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets((), grouping sets((), grouping sets(((a, b))))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(grouping sets(rollup(c), grouping sets(cube(c)))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(a, grouping sets(a, cube(b))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(grouping sets((a, (b)))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(grouping sets((a, b))) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(grouping sets(a, grouping sets(a), a)) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets(grouping sets(a, grouping sets(a, grouping sets(a), ((a)), a, grouping sets(a), (a)), a)) order by 1 desc;
select sum(c) from row_gstest2 group by grouping sets((a,(a,b)), grouping sets((a,(a,b)),a)) order by 1 desc;

create table row_gstest_empty (a integer, b integer, v integer);
select a, b, sum(v), count(*) from row_gstest_empty group by grouping sets ((a,b),a);
select a, b, sum(v), count(*) from row_gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from row_gstest_empty group by grouping sets ((a,b),(),(),());
select a, b, avg(v), sum(v), count(*) from row_gstest_empty group by grouping sets ((a,b),(),(),());
select sum(v), count(*) from row_gstest_empty group by grouping sets ((),(),());

-- empty input with joins tests some important code paths
select t1.a, t2.b, sum(t1.v), count(*) from row_gstest_empty t1, row_gstest_empty t2 group by grouping sets ((t1.a,t2.b),());

-- simple joins, var resolution, GROUPING on join vars
select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a) from row_gstest1 t1, row_gstest2 t2 group by grouping sets ((t1.a, t2.b), ()) order by 1, 2, 3, 4, 5;

select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a) from row_gstest1 t1 join row_gstest2 t2 on (t1.a=t2.a) group by grouping sets ((t1.a, t2.b), ())
order by 1, 2, 3, 4, 5;

select a, b, grouping(a, b), sum(t1.v), max(t2.c) from row_gstest1 t1 join row_gstest2 t2 using (a,b) group by grouping sets ((a, b), ())  order by 1, 2, 3, 4, 5;;

-- check that functionally dependent cols are not nulled
select a, d, grouping(a,b,c) from row_gstest3 group by grouping sets ((a,b), (a,c));

-- Views with GROUPING SET queries
select a, b, grouping(a,b), sum(c), count(*), max(c) from row_gstest2 group by rollup ((a,b,c),(c,d)) order by 1, 2, 3, 4, 5, 6;

select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum from row_gstest2 group by cube (a,b)  order by 1, 2, 3, 4;

-- Agg level check. This query should error out.
select (select grouping(a,b) from row_gstest2) from row_gstest2 group by a,b;
select a, b, grouping(a, b, c) from row_gstest2 group by a, b;

--Nested queries
select a, b, sum(c), count(*) from row_gstest2 group by grouping sets (rollup(a,b),a) order by 1, 2, 3, 4;

create table row_aptest(a int, b varchar(5), c char(5), d text, e numeric(5, 0), f timestamp);
insert into row_aptest values(1, generate_series(1, 2), generate_series(1, 3), generate_series(1, 4), generate_series(1, 6), '2012-12-16 10:11:15');

analyze row_aptest;
select a, b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup(a, b, c, d, e, f) order by 1, 2, 3, 4, 5, 6, 7;
select b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup(b, c, d, e, f) order by 1, 2, 3, 4, 5, 6;
select b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup(b, c, d, e) order by 1, 2, 3, 4, 5, 6;

select a, b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by cube(a, b, c, d, e, f) order by 1, 2, 3, 4, 5, 6, 7;
select b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by cube(b, c, d, e, f) order by 1, 2, 3, 4, 5, 6;
select b, c, sum(a), avg(e), max(d), min(e) from row_aptest group by cube(b, c, d, e) order by 1, 2, 3, 4, 5, 6;

delete from row_aptest;
--test include duplicate columns
insert into row_aptest values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), '2012-12-16 10:11:15');
select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup(a, a, b) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup((a, b), (a, b)) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by rollup((a, b), (c, d)) order by 1, 2, 3, 4, 5, 6;

select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by cube(a, a, b) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by cube((a, b), (a, b)) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from row_aptest group by cube((a, b), (c, d)) order by 1, 2, 3, 4, 5, 6;

--include windowfunc
create table row_t1(a int, b int, c int);
insert into row_t1 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));
analyze row_t1;

explain (verbose on, costs off) select a, rank() over (partition by grouping(a)) from row_t1 group by rollup(a, b, c) order by 1, 2;

--include distinct
explain (verbose on, costs off) select avg(a), count(distinct b), count(distinct c) from row_t1 group by ();
explain (verbose on, costs off) select a, count(distinct b), count(distinct b) from row_t1 group by rollup(a, b);
explain (verbose on, costs off) select a, count(distinct b), count(distinct c) from row_t1 group by rollup(a, b);
select avg(a), count(distinct b), count(distinct c) from row_t1 group by () order by 1, 2, 3;
select a, count(distinct b), count(distinct b) from row_t1 group by rollup(a, b) order by 1, 2, 3;
select a, count(distinct b), count(distinct c) from row_t1 group by rollup(a, b) order by 1, 2, 3;

explain (verbose on, costs off) select min(distinct b), c from row_t1 group by b, rollup(a, c) order by 1, 2;

explain (verbose on, costs off) select a, sum(c), grouping(c) from row_t1 group by rollup(a, b), grouping sets(c) order by 1, 2, 3;
explain (verbose on, costs off) select a, sum(c), grouping(c) from row_t1 group by rollup(a, b), cube(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from row_t1 group by rollup(a, b), grouping sets(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from row_t1 group by rollup(a, b), cube(c) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c), grouping(c) from row_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from row_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c), grouping(c) from row_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from row_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c) from row_t1 group by grouping sets(a);
select a, sum(c)  from row_t1 group by grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from row_t1 group by cube(a, b), grouping sets(a);
select a, sum(c)  from row_t1 group by cube(a, b), grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c) from row_t1 group by cube(a, b), a;
select a, sum(c)  from row_t1 group by cube(a, b), a order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from row_t1 group by rollup(a, b), grouping sets(a);
select a, sum(c)  from row_t1 group by rollup(a, b), grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c) from row_t1 group by rollup(a, b), a;
select a, sum(c)  from row_t1 group by rollup(a, b), a order by 1, 2;

create index row_t1_index on row_t1(a);
explain (verbose on, costs off)select max(a) from row_t1 group by ();
explain (verbose on, costs off)select max(a) from row_t1 group by grouping sets(());
select max(a) from row_t1 group by ();
select max(a) from row_t1 group by grouping sets(());
explain (verbose on, costs off)select max(a) from row_t1 group by grouping sets((), ());

explain (verbose on, costs off)
select a, b, c from row_t1 where b = c group by grouping sets((a, b)), rollup(c) order by 1, 2, 3;

explain (verbose on, costs off)
SELECT a Column_012, 25 Column_014 FROM row_t1 GROUP BY CUBE(Column_012, Column_014) ORDER BY Column_014, Column_012 ASC;

explain (costs off) select distinct a, b, c from row_t1 group by grouping sets((a, b)), rollup(c) order by 1, 2, 3;

explain (costs off) select a, b, c, 10 from row_t1 where a = b group by grouping sets((a, b)), rollup(c) order by 1, 2, 3, 4;

explain (costs off) select a, b, c, 10 from row_t1 where a = b group by grouping sets(a), rollup(b, c) order by 1, 2, 3, 4;

explain (costs off) select a, b, c, 10 as col from row_t1 where a = b group by grouping sets((a, b)), rollup(c, col) order by 1, 2, 3, 4;

explain (costs off) select a, b, c, 10 as col from row_t1 where a = b group by grouping sets(a), rollup(b, c, col) order by 1, 2, 3, 4;


create table location(location_id integer );
create table alert_emails(item_id varchar(20) not null, location_id integer null);
 
insert into alert_emails values ('a',  0);
insert into alert_emails values ('b', null);
insert into alert_emails values ('c',  2);
insert into alert_emails values ('d',  3);

insert into location values (0);
insert into location values (1);
insert into location values (2);
insert into location values (3);
analyze location;
analyze alert_emails;

explain (verbose on, costs off)
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by grouping sets(c1,c2) 
order by 1, 2;
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by grouping sets(c1,c2) 
order by 1, 2;

explain (verbose on, costs off)
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c1 = c2
group by grouping sets(c1,c2) 
order by 1, 2;

select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c1 = c2
group by grouping sets(c1,c2) 
order by 1, 2;

explain (verbose on, costs off)
select loc.location_id as c1, ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c1, c2, grouping sets(c1,c2)
order by 1, 2;

select loc.location_id as c1, ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c1, c2, grouping sets(c1,c2)
order by 1, 2;

explain (verbose on, costs off)
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c1, grouping sets(c1,c2)
order by 1, 2;

select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c1, grouping sets(c1,c2)
order by 1, 2;

explain (verbose on, costs off)
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c2, grouping sets(c1,c2)
order by 1, 2;

select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by c2, grouping sets(c1,c2)
order by 1, 2;

explain (verbose on, costs off)
select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by grouping sets(c1, c2)
order by 1, 2;

select loc.location_id as c1,ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
group by grouping sets(c1, c2)
order by 1, 2;

explain (verbose on, costs off)								  
select c1 from (select loc.location_id as c1, ale.location_id as c2 from location as loc, 
alert_emails ale where c2 = c1 group by c2, grouping sets (c1, c2)) as tt
group by c1 order by c1;

select c1 from (select loc.location_id as c1, ale.location_id as c2 from location as loc, 
alert_emails ale where c2 = c1 group by c2, grouping sets (c1, c2)) as tt
group by c1 order by c1;

explain (costs off) select 1 from row_t1
join
(select loc.location_id as c1, ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
 group by c2, grouping sets(c1,c2)) as tt
on (row_t1.a = tt.c1);

select 1 from row_t1
join
(select loc.location_id as c1, ale.location_id as c2
from location as loc, alert_emails ale
where c2 = c1
 group by c2, grouping sets(c1,c2)) as tt
on (row_t1.a = tt.c1);

-- test replacing dis keys for rollup()
explain (costs off)
SELECT v1.location_id
, v4.location_id
from (
select cast(location_id as text) location_id
from (select loc.location_id, ale.location_id l2
from location loc, alert_emails ale
limit 100)
) as v1,
(select location_id, min(location_id) from location
group by 1) as v4
where v1.location_id=v4.location_id
group by 1, 2, cube(v1.location_id, v1.location_id+5);

explain (costs off)
SELECT v1.location_id
from (
select cast(location_id as text) location_id
from (select location_id, max(location_id)
from location
group by 1)
) as v1,
(select location_id, min(location_id) from location
group by 1) as v4
where v1.location_id=v4.location_id
group by 1, cube(v1.location_id, v1.location_id+5); 

create table location_type
(
    location_type_cd varchar(50) not null ,
    location_type_desc varchar(250) not null 
);
 
create table channel
(
    channel_cd varchar(50) not null ,
    channel_desc varchar(250) null
);

explain (costs off)
SELECT location_type_desc 
FROM location_type , channel                 
WHERE location_type_desc = channel_desc
GROUP BY GROUPING SETS(location_type_desc, location_type_desc);

drop table location_type;
drop table channel;

drop table row_aptest;
drop table row_t1;
drop table row_gstest1;
drop table row_gstest2;
drop table row_gstest3;
drop table row_gstest_empty;
drop table location;
drop table alert_emails;


drop schema hw_row_groupingsets cascade;
