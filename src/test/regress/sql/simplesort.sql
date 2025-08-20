drop table if exists sorttest;
create table sorttest (a int4, b pg_catalog.date, c timestamp, d int8, e numeric(8,4), f text, g char(16), h varchar(16));
insert into sorttest values (-1, '0001-01-01', '0001-01-01 01:01:01', 9, -1111.1111, '0111111111', '0111111111', '0111111111');
insert into sorttest values (-2, '0002-02-02', '0002-02-02 02:02:02', 8, -2222.2222, '0022222222', '0022222222', '0022222222');
insert into sorttest values (-3, '0003-03-03', '0003-03-03 03:03:03', 7, -3333.3333, '0003333333', '0003333333', '0003333333');
insert into sorttest values (-4, '0004-04-04', '0004-04-04 04:04:04', 6, -4444.4444, '0000444444', '0000444444', '0000444444');
insert into sorttest values (-5, '0005-05-05', '0005-05-05 05:05:05', 5, -5555.5555, '0000055555', '0000055555', '0000055555');
insert into sorttest values (-6, '0006-06-06', '0006-06-06 06:06:06', 4, -6666.6666, '0000006666', '0000006666', '0000006666');
insert into sorttest values (-7, '0007-07-07', '0007-07-07 07:07:07', 3, -7777.7777, '0000000777', '0000000777', '0000000777');
insert into sorttest values (-8, '0008-08-08', '0008-08-08 08:08:08', 2, -8888.8888, '0000000088', '0000000088', '0000000088');
insert into sorttest values (-9, '0009-09-09', '0009-09-09 09:09:09', 1, -9999.9999, '0000000009', '0000000009', '0000000009');
insert into sorttest values (0, '0001-01-10', '0001-01-11 00:00:00', 0, 0, '0', '0', '0');
insert into sorttest values (1, '0001-01-11', '0001-01-11 01:01:01', -1, 1111.1111, '1111111111', '1111111111', '1111111111');
insert into sorttest values (1, '0001-01-11', '0001-01-11 01:01:01', -1, 1111.1111, '1111111111', '1111111111', '1111111111');
insert into sorttest values (1, '0001-01-11', '0001-01-11 01:01:01', -1, 1111.1111, '1111111111', '1111111111', '1111111111');
insert into sorttest values (2, '0002-02-12', '0002-02-12 02:02:02', 6, 2222.2222, '2222222222', '2222222222', '2222222222');
insert into sorttest values ('', '', '', '', '', '', '', '');
insert into sorttest values (2147483647, '9999-12-31', '9999-12-31 23:59:59', 9223372036854775807, 0.9999, 'aaa', 'aaa', 'aaa');
insert into sorttest values (2147483646, '9999-12-30', '9999-12-31 23:59:58', 9223372036854775806, 0.9998, 'bbb', 'bbb', 'bbb');
insert into sorttest values (2147483645, '9999-12-29', '9999-12-31 23:59:57', 9223372036854775805, 0.9997, 'ccc', 'ccc', 'ccc');
insert into sorttest values (1000000000, '9998-12-31', '9999-12-31 23:58:59', 1000000000000000000, 'NaN', '00', '00', '00');
insert into sorttest values (1500000000, '9997-12-31', '9999-12-31 23:57:59', 2000000000000000000, -111.111, '11111111111', '11111111111', '11111111111');
insert into sorttest values (2000000000, '9996-12-31', '9999-12-31 23:56:59', -9223372036854775803, -11.11, '9', '9', '9');
insert into sorttest values (444444444, '9999-09-30', '9995-12-31 23:57:59', -9223372036854775808, 4444, 'ABCABC', 'ABCABC', 'ABCABC');
insert into sorttest values (-555555555, '9999-08-30', '0009-12-30 23:56:59', -555555555555555, -5555, 'XYZ', 'XYZ', 'XYZ');
insert into sorttest values (-666666666, '9999-07-30', '9999-11-30 23:55:59', -666666666666666, -6666, 'XYZX', 'XYZX', 'XYZX');
insert into sorttest values (-777777777, '9999-06-30', '9999-10-30 23:54:59', -777777777777777, -7777, 'XYZXY', 'XYZXY', 'XYZXY');
insert into sorttest values (-888888889, '9999-05-30', '6666-09-30 23:53:59', -888888888888888, -8888, 'XYZXYZ', 'XYZXYZ', 'XYZXYZ');
insert into sorttest values (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select a from sorttest order by a;
select b from sorttest order by b;
select c from sorttest order by c;
select d from sorttest order by d;
select e from sorttest order by e;
select f from sorttest order by f;
select g from sorttest order by g;
select h from sorttest order by h;

select a from sorttest order by a nulls first;
select b from sorttest order by b nulls first;
select c from sorttest order by c nulls first;
select d from sorttest order by d nulls first;
select e from sorttest order by e nulls first;
select f from sorttest order by f nulls first;
select g from sorttest order by g nulls first;
select h from sorttest order by h nulls first;

select a from sorttest order by a desc;
select b from sorttest order by b desc;
select c from sorttest order by c desc;
select d from sorttest order by d desc;
select e from sorttest order by e desc;
select f from sorttest order by f desc;
select g from sorttest order by g desc;
select h from sorttest order by h desc;

select a from sorttest order by a desc nulls last;
select b from sorttest order by b desc nulls last;
select c from sorttest order by c desc nulls last;
select d from sorttest order by d desc nulls last;
select e from sorttest order by e desc nulls last;
select f from sorttest order by f desc nulls last;
select g from sorttest order by g desc nulls last;
select h from sorttest order by h desc nulls last;

select f from sorttest order by f collate 'C';
select g from sorttest order by g collate 'C';
select h from sorttest order by h collate 'C';

select a from sorttest order by a limit 10;
select d,a from sorttest order by a,d;
select a,d from sorttest order by d,a;
select a from sorttest order by f,g;

set work_mem='64kB';

insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;

explain analyze select * from sorttest order by a;
explain analyze select * from sorttest order by e;
explain analyze select * from sorttest order by h;

insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;

explain analyze select a from sorttest order by a;
explain analyze select e from sorttest order by e;
explain analyze select h from sorttest order by h;

insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;
insert into sorttest select * from sorttest;

explain analyze select * from sorttest order by h limit 10000;

create table tmp1(a varchar(20));
create table tmp2(a int);
insert into tmp1 values('aaa');
insert into tmp1 values('bbb');
insert into tmp2 values(NULL);

set enable_material=off;
explain analyze select /*+ leading((tt1 tt2)) */ tt1.a, tt2.a from
(select a from tmp1 limit 2) as tt1, (select a from sorttest order by a limit (select a from tmp2 limit 1)) as tt2;

set enable_hashjoin=off;
insert into tmp2 values(1);
insert into tmp2 values(1);
insert into tmp2 values(2);
explain analyze select tt1.a, tt1.b, tt2.a from sorttest as tt1 left join tmp2 as tt2 on tt1.a = tt2.a;

BEGIN;
DECLARE order_cursor CURSOR FOR SELECT h, e FROM sorttest ORDER BY a;
MOVE LAST IN order_cursor;
FETCH BACKWARD 2 FROM order_cursor;
MOVE BACKWARD 3000 IN order_cursor;
FETCH BACKWARD 3 FROM order_cursor;
CLOSE order_cursor;
COMMIT;

set maintenance_work_mem ='1024kB';
alter table sorttest set (parallel_workers=4);

create index sort_idx on sorttest(a);
drop index sort_idx;
create index sort_idx on sorttest(b);
drop index sort_idx;
create index sort_idx on sorttest(c);
drop index sort_idx;
create index sort_idx on sorttest(d);
drop index sort_idx;
create index sort_idx on sorttest(e);
drop index sort_idx;
create index sort_idx on sorttest(f);
drop index sort_idx;
create index sort_idx on sorttest(g);
drop index sort_idx;
create index sort_idx on sorttest(h);
drop index sort_idx;

create index sort_idx on sorttest(g,h);
drop index sort_idx;

create index CONCURRENTLY sort_idx on sorttest(a); 
drop index sort_idx;
create index CONCURRENTLY sort_idx on sorttest(e); 
drop index sort_idx;
create index CONCURRENTLY sort_idx on sorttest(h); 
drop index sort_idx;

drop table tmp1;
drop table tmp2;
drop table sorttest;
