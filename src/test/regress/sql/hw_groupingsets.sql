create schema hw_groupingsets;
set search_path=hw_groupingsets;

create table gstest1(a int, b int, v int)with(orientation = column);

insert into gstest1 values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),
(2,3,15),  (3,3,16), (3,4,17), (4,1,18), (4,1,19);

create table gstest2 (a integer, b integer, c integer, d integer,
                           e integer, f integer, g integer, h integer) with(orientation = column);

copy gstest2 from stdin;
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
create table gstest3 (a integer, b integer, c integer, d integer)with(orientation = column);
copy gstest3 from stdin;
1	1	1	1
2	2	2	2
\.

select a, b, grouping(a,b), sum(v), count(*), max(v) from gstest1 group by rollup (a,b) order by 1, 2, 3, 4, 5, 6;

-- nesting with window functions
select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum from gstest2 group by rollup (a,b)  order by 1, 2, 3, 4;

-- nesting with grouping sets
select sum(c) from gstest2 group by grouping sets((), grouping sets((), grouping sets(()))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets((), grouping sets((), grouping sets(((a, b))))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(grouping sets(rollup(c), grouping sets(cube(c)))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(a, grouping sets(a, cube(b))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(grouping sets((a, (b)))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(grouping sets((a, b))) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(grouping sets(a, grouping sets(a), a)) order by 1 desc;
select sum(c) from gstest2 group by grouping sets(grouping sets(a, grouping sets(a, grouping sets(a), ((a)), a, grouping sets(a), (a)), a)) order by 1 desc;
select sum(c) from gstest2 group by grouping sets((a,(a,b)), grouping sets((a,(a,b)),a)) order by 1 desc;

create table gstest_empty (a integer, b integer, v integer) with(orientation = column);
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),a);
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),());
select a, b, sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
select a, b, avg(v), sum(v), count(*) from gstest_empty group by grouping sets ((a,b),(),(),());
select sum(v), count(*) from gstest_empty group by grouping sets ((),(),());

-- empty input with joins tests some important code paths
select t1.a, t2.b, sum(t1.v), count(*) from gstest_empty t1, gstest_empty t2 group by grouping sets ((t1.a,t2.b),());

-- simple joins, var resolution, GROUPING on join vars
select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a) from gstest1 t1, gstest2 t2 group by grouping sets ((t1.a, t2.b), ()) order by 1, 2, 3, 4, 5;

select t1.a, t2.b, grouping(t1.a, t2.b), sum(t1.v), max(t2.a) from gstest1 t1 join gstest2 t2 on (t1.a=t2.a) group by grouping sets ((t1.a, t2.b), ())
order by 1, 2, 3, 4, 5;

select a, b, grouping(a, b), sum(t1.v), max(t2.c) from gstest1 t1 join gstest2 t2 using (a,b) group by grouping sets ((a, b), ())  order by 1, 2, 3, 4, 5;;

-- check that functionally dependent cols are not nulled
select a, d, grouping(a,b,c) from gstest3 group by grouping sets ((a,b), (a,c));

-- Views with GROUPING SET queries
select a, b, grouping(a,b), sum(c), count(*), max(c) from gstest2 group by rollup ((a,b,c),(c,d)) order by 1, 2, 3, 4, 5, 6;

select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum from gstest2 group by cube (a,b)  order by 1, 2, 3, 4;
select a, b, sum(c) from (values (1,1,10),(1,1,11),(1,2,12),(1,2,13),(1,3,14),(2,3,15),(3,3,16),(3,4,17),(4,1,18),(4,1,19)) v(a,b,c) 
group by rollup (a,b) order by 1, 2, 3;

-- Agg level check. This querys should error out.
select (select grouping(a,b) from gstest2) from gstest2 group by a,b;
select a, grouping(a, b, c) from gstest2 group by cube(a, b);

--Nested queries
select a, b, sum(c), count(*) from gstest2 group by grouping sets (rollup(a,b),a) order by 1, 2, 3, 4;

create table rtest(a int, b varchar(5), c char(5), d text, e numeric(5, 0), f timestamp);
insert into rtest values(1, generate_series(1, 2), generate_series(1, 3), generate_series(1, 4), generate_series(1, 6), '2012-12-16 10:11:15');

create table vec_aptest(a int, b varchar(5), c char(5), d text, e numeric(5, 0), f timestamp)with(orientation = column);
insert into vec_aptest select * from rtest;
select a, b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup(a, b, c, d, e, f) order by 1, 2, 3, 4, 5, 6, 7;
select b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup(b, c, d, e, f) order by 1, 2, 3, 4, 5, 6;
select b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup(b, c, d, e) order by 1, 2, 3, 4, 5, 6;

select a, b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube(a, b, c, d, e, f) order by 1, 2, 3, 4, 5, 6, 7;
select b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube(b, c, d, e, f) order by 1, 2, 3, 4, 5, 6;
select b, c, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube(b, c, d, e) order by 1, 2, 3, 4, 5, 6;

explain (costs off)select a, b, c from vec_aptest where b = '12' 
group by rollup(a), grouping sets(b, c)
order by 1,2,3;

explain (costs off)select a, b, c, d from vec_aptest where d = '12' 
group by rollup(a, b, c), grouping sets(d)
order by 1, 2, 3, 4;

drop table rtest;

delete from vec_aptest;
--test include duplicate columns
insert into vec_aptest values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), '2012-12-16 10:11:15');
select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup(a, a, b) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup((a, b), (a, b)) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by rollup((a, b), (c, d)) order by 1, 2, 3, 4, 5, 6;

select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube(a, a, b) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube((a, b), (a, b)) order by 1, 2, 3, 4, 5, 6;
select a, b, sum(a), avg(e), max(d), min(e) from vec_aptest group by cube((a, b), (c, d)) order by 1, 2, 3, 4, 5, 6;

create table vec_t1(a int, b int, c int)with(orientation = column);
explain (verbose on, costs off) select a, rank() over (partition by grouping(a)) from vec_t1 group by rollup(a, b, c) order by 1, 2;
insert into vec_t1 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));
explain (verbose on, costs off) select avg(a), count(distinct b), count(distinct c) from vec_t1 group by ();
explain (verbose on, costs off) select a, count(distinct b), count(distinct b) from vec_t1 group by rollup(a, b);
explain (verbose on, costs off) select a, count(distinct b), count(distinct c) from vec_t1 group by rollup(a, b);
select avg(a), count(distinct b), count(distinct c) from vec_t1 group by () order by 1, 2, 3; 
select a, count(distinct b), count(distinct b) from vec_t1 group by rollup(a, b) order by 1, 2, 3;
select a, count(distinct b), count(distinct c) from vec_t1 group by rollup(a, b) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c), grouping(c) from vec_t1 group by rollup(a, b), grouping sets(c) order by 1, 2, 3;
explain (verbose on, costs off) select a, sum(c), grouping(c) from vec_t1 group by rollup(a, b), cube(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from vec_t1 group by rollup(a, b), grouping sets(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from vec_t1 group by rollup(a, b), cube(c) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c), grouping(c) from vec_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;
select a, sum(c), grouping(c) from vec_t1 group by cube(a, b), grouping sets(c) order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a);
select a, sum(c)  from vec_t1 group by grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by cube(a, b), grouping sets(a);
select a, sum(c)  from vec_t1 group by cube(a, b), grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by cube(a, b), a;
select a, sum(c)  from vec_t1 group by cube(a, b), a order by 1, 2;

explain (verbose on, costs off) select a, sum(c), avg(b) from vec_t1 group by rollup(a, b), grouping sets(a);
select a, sum(c), avg(b)  from vec_t1 group by rollup(a, b), grouping sets(a) order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by rollup(a, b), a;
select a, sum(c)  from vec_t1 group by rollup(a, b), a order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a, b);

explain (verbose on, costs off) select a, b, sum(c)  from vec_t1 group by grouping sets(a, b) having grouping(a) = 0;
select a, b, sum(c)  from vec_t1 group by grouping sets(a, b) having grouping(a) = 0 order by 1, 2, 3;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a) having grouping(a) = 0;
select a, sum(c)  from vec_t1 group by grouping sets(a) having grouping(a) = 0 order by 1, 2;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a, b) having sum(a) = 1;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a, b) having sum(b) = 1;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by a, grouping sets(b, c) having sum(b) > 10;

explain (verbose on, costs off) select a, sum(c)  from vec_t1 group by grouping sets(a, b) having sum(a)+sum(b) > 1 or grouping(a)=0;

explain (verbose on, costs off) select a, b, sum(c)  from vec_t1 group by rollup(a, b) having b is not null;
select a, b, sum(c)  from vec_t1 group by rollup(a, b) having b is not null order by 1,2,3;

explain (verbose on, costs off) select a, b, sum(c)  from vec_t1 group by a, grouping sets(b, c) having b is not null;
select a, b, sum(c)  from vec_t1 group by a, grouping sets(b, c) having b is not null order by 1,2,3;

SELECT 1 Column_006 GROUP BY cube(Column_006) order by 1 nulls first;

explain (verbose on, costs off) select distinct b from vec_t1 group by a, grouping sets(b, c);

explain (verbose on, costs off) select avg(distinct b) from vec_t1 group by grouping sets(b, c) having avg(distinct b) > 600;

explain (verbose on, costs off) select avg(distinct b) from vec_t1 group by grouping sets(b, c) having sum(distinct b) > 600;

explain (verbose on, costs off) select avg(distinct a) from vec_t1 group by grouping sets(b, c);

explain (verbose on, costs off) select a, b, b from vec_t1 group by rollup(1, 2), 3 order by 1, 2, 3;

analyze vec_t1;
explain (verbose on, costs off) select a, grouping(a) from vec_t1 where 0 = 1 group by a;

-- test AP func with const target list used in setop branch
-- hashagg subplan
explain (costs off, verbose on)
select 1 from vec_t1 minus SELECT 1 Column_006 where 1=0 GROUP BY cube(Column_006);

select 1 from vec_t1 minus SELECT 1 Column_006 where 1=0 GROUP BY cube(Column_006);

-- non-hashagg subplan
explain (costs off, verbose on)
select 1 from vec_t1 minus (SELECT 1 Column_006 where 1=0 GROUP BY cube(Column_006) limit 1);

select 1 from vec_t1 minus (SELECT 1 Column_006 where 1=0 GROUP BY cube(Column_006) limit 1);

-- stream added plan 
explain (costs off, verbose on)
select 1,1 from vec_t1 union SELECT 2 c1, 2 c2 from vec_t1 where 1=0 GROUP BY cube(c1, c2);

select 1,1 from vec_t1 union SELECT 2 c1, 2 c2 from vec_t1 where 1=0 GROUP BY cube(c1, c2) order by 1;

explain (costs off, verbose on)
select 1,1 from vec_t1 union SELECT 2 c1, count(distinct(b)) from vec_t1 where 1=0 GROUP BY cube(a, c1);

select 1,1 from vec_t1 union SELECT 2 c1, count(distinct(b)) from vec_t1 where 1=0 GROUP BY cube(a, c1) order by 1;

-- const column in AP func and non-AP func
explain (costs off, verbose on)
select 1,1,1 from vec_t1 union SELECT 2 c1, 2 c2, 4 c3 from vec_t1 where 1=0 GROUP BY cube(c1, c2);

select 1,1,1 from vec_t1 union SELECT 2 c1, 2 c2, 4 c3 from vec_t1 where 1=0 GROUP BY cube(c1, c2) order by 1;

-- unknown type
explain (costs off, verbose on)
select 1, '1' from vec_t1 union select 1, '243' xx from vec_t1 where 1=0 group by cube(1);

select 1, '1' from vec_t1 union select 1, '243' xx from vec_t1 where 1=0 group by cube(1) order by 1;

create table temp_grouping_table as select a, b, c from vec_t1 group by grouping sets(a, b), rollup(b), cube(c);
\o xml_explain_temp.txt
explain (format xml) select a, b, c from vec_t1 group by rollup(a, b, c);
explain (format JSON) select a, b, c from vec_t1 group by rollup(a, b, c);
explain (format YAML) select a, b, c from vec_t1 group by rollup(a, b, c);
\o
drop table vec_t1;

create table vec_t1(a int)with(orientation = column);
insert into vec_t1 values(1);
select 1 a, 2, count(*) from vec_t1 group by cube(1, 2) having count(*) = 1 order by 1, 2;  
drop table vec_t1;

drop table temp_grouping_table;


drop table vec_aptest;
drop table gstest1;
drop table gstest2;
drop table gstest3;
drop table gstest_empty;

-- adjust distribute key from equivalence class
create table adj_t(a varchar(10), b varchar(10));
drop table adj_t;


create table hash_partition_002
(
    abstime_02 abstime,
    bit_02 bit(2),
    blob_02 blob,
    bytea_02 bytea,
    int2_02 SMALLINT,
    int4_02 INTEGER,
    int8_02 BIGINT,
    nvarchar2_02 NVARCHAR2(100),
    raw_02 raw,
    reltime_02 reltime,
    text_02 text,
    varbit_02 varbit,
    varchar_02 varchar(50),
    bpchar_02 bpchar(60),
    date_02 date,
    timestamp_02 timestamp,
    timestamptz_02 timestamptz
)partition by range(date_02)
(
    partition p1 start('0002-01-01') end ('0006-01-01') every('1 year'),
    partition p2 end(maxvalue)
);
create table hash_partition_003
(
    abstime_03 abstime,
    bit_03 bit(3),
    blob_03 blob,
    bytea_03 bytea,
    int2_03 SMALLINT,
    int4_03 INTEGER,
    int8_03 BIGINT,
    nvarchar2_03 NVARCHAR2(100),
    raw_03 raw,
    reltime_03 reltime,
    text_03 text,
    varbit_03 varbit,
    varchar_03 varchar(50),
    bpchar_03 bpchar(60),
    date_03 date,
    timestamp_03 timestamp,
    timestamptz_03 timestamptz
)partition by range(timestamp_03)
(
    partition p1 start('0003-01-01') end ('0006-01-01') every('1 year'),
    partition p2 end(maxvalue)
);

insert into hash_partition_002 values ('1999-01-01',B'10','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_002 values ('1999-01-01',B'11','9999999999999',E'\\000',32767,2147483647,9223372036854775807,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_002 values ('1999-01-01',B'01','9999999999999',E'\\000',32767,2147483647,9223372036854775807,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_002 values ('1999-01-01',B'11','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_002 values ('1999-01-01',B'10','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_002 values ('1999-01-01',B'11','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');

insert into hash_partition_003 values ('1999-01-01',B'101','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_003 values ('1999-01-01',B'110','9999999999999',E'\\000',32767,2147483647,9223372036854775807,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_003 values ('1999-01-01',B'011','9999999999999',E'\\000',32767,2147483647,9223372036854775807,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_003 values ('1999-01-01',B'010','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_003 values ('1999-01-01',B'001','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');
insert into hash_partition_003 values ('1999-01-01',B'111','9999999999999',E'\\000',-32768,-2147483648,-9223372036854775808,0.115145415,'DEADBEEF',' 2009 days 23:59:59','XXXXXXXXXXXXXXXXX ',B'1101100011','0.222','1212','2011-01-01','0002-09-09 23:59:59','0003-09-09 00:02:01');

SELECT
    nullif(bit_02,varbit_03) as a , 
    varbit_02::bit,
    '01' lx,
    'v_dt' ::VARCHAR AS rq
FROM hash_partition_002 left join hash_partition_003 on bit_02 = varbit_03 or varbit_03>bit_02 
WHERE rq = 'v_dt'
GROUP BY bit_02, varbit_03,varbit_02,
rollup(text_03,int4_03,nvarchar2_02,varchar_02)
having max(abstime_03)::int <> 1
order by 1, 2, 3, 4;

select 
    distinct(count(bit_03)),
    length(varbit_03),
    'ee'::text as a
from hash_partition_002 right join hash_partition_003  on  varbit_03>bit_02  
group by  bit_03, varbit_03,rollup(varbit_03,bit_03,'ee'::text)
HAVING count(1)>0
order by 1, 2, 3;

SELECT
    count(bit_02)
FROM hash_partition_002
GROUP BY bit_02,rollup(text_02)
HAVING max(abstime_02)::int <> 1;


SELECT
    nullif(bit_02,varbit_03) as a , 
    varbit_02::bit,
    '01' lx,
    'v_dt' ::VARCHAR AS rq
FROM hash_partition_002 left join hash_partition_003 on bit_02 = varbit_03 or varbit_03>bit_02 
WHERE rq = 'v_dt'
GROUP BY bit_02, varbit_03,varbit_02,
rollup(text_03,int4_03,nvarchar2_02,varchar_02)
having max(abstime_03)::int <> 1
order by 1, 2, 3, 4;

select 
    distinct(count(bit_03)),
    length(varbit_03),
    'ee'::text as a
from hash_partition_002 right join hash_partition_003  on  varbit_03>bit_02  
group by  bit_03, varbit_03,rollup(varbit_03,bit_03,'ee'::text)
HAVING count(1)>0
order by 1, 2, 3;

SELECT
    bit_02
FROM hash_partition_002
GROUP BY bit_02,rollup(text_02)
HAVING max(abstime_02)::int <> 1
ORDER BY 1;

SELECT
    count(bit_02)
FROM hash_partition_002
GROUP BY bit_02,rollup(text_02)
HAVING max(abstime_02)::int <> 1
ORDER BY 1;

drop schema hw_groupingsets cascade;
