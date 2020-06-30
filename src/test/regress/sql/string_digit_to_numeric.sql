create schema string_digit_to_numeric;
set current_schema = string_digit_to_numeric;

create table test1 (c1 int, c2 varchar) ;
create table test2 (c1 char, c2 varchar, c3 nvarchar2, c4 text) ;
create table test3 (c1 int1, c2 int2, c3 int4, c4 int8, c5 float4, c6 float8, c7 numeric) ;
insert into test1 values (2, '1.1');
insert into test2 values (2, 1.1, 1.01, 1.001);
insert into test3 values (1, 1, 1, 1, 1, 1, 1);
analyze test1;
analyze test2;
analyze test3;

select * from test1 where c2 > 1;
explain (verbose on, costs off) select * from test1 where c2 > 1;

select * from test2 join test3 on test2.c1 > test3.c1;
explain (verbose on, costs off) select * from test2 join test3 on test2.c1 > test3.c1;

select * from test2 join test3 on test2.c2 > test3.c2;
explain (verbose on, costs off) select * from test2 join test3 on test2.c2 > test3.c2;

select * from test2 join test3 on test2.c3 > test3.c3;
explain (verbose on, costs off) select * from test2 join test3 on test2.c3 > test3.c3;

select * from test2 join test3 on test2.c4 > test3.c4;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c4;

select * from test2 join test3 on test2.c4 > test3.c5;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c5;

select * from test2 join test3 on test2.c4 > test3.c6;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c6;

select * from test2 join test3 on test2.c4 > test3.c7;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c7;

create view v1 as select c7 from test2 join test3 on test2.c1 > test3.c1 and test2.c2 > test3.c2 and test2.c3 > test3.c3 and test2.c4 > test3.c4 and test2.c4 > test3.c5 and test2.c4 > test3.c6 and test2.c4 > test3.c7;
select * from v1;
explain (verbose on, costs off) select * from v1;

set behavior_compat_options = 'convert_string_digit_to_numeric';

select * from test1 where c2 > 1;
explain (verbose on, costs off) select * from test1 where c2 > 1;

select * from test2 join test3 on test2.c1 > test3.c1;
explain (verbose on, costs off) select * from test2 join test3 on test2.c1 > test3.c1;

select * from test2 join test3 on test2.c2 > test3.c2;
explain (verbose on, costs off) select * from test2 join test3 on test2.c2 > test3.c2;

select * from test2 join test3 on test2.c3 > test3.c3;
explain (verbose on, costs off) select * from test2 join test3 on test2.c3 > test3.c3;

select * from test2 join test3 on test2.c4 > test3.c4;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c4;

select * from test2 join test3 on test2.c4 > test3.c5;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c5;

select * from test2 join test3 on test2.c4 > test3.c6;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c6;

select * from test2 join test3 on test2.c4 > test3.c7;
explain (verbose on, costs off) select * from test2 join test3 on test2.c4 > test3.c7;

select * from v1;
explain (verbose on, costs off) select * from v1;
create view v2 as select c7 from test2 join test3 on test2.c1 > test3.c1 and test2.c2 > test3.c2 and test2.c3 > test3.c3 and test2.c4 > test3.c4 and test2.c4 > test3.c5 and test2.c4 > test3.c6 and test2.c4 > test3.c7;
select * from v2;
explain (verbose on, costs off) select * from v2;

set behavior_compat_options = '';

select * from v1;
explain (verbose on, costs off) select * from v1;
select * from v2;
explain (verbose on, costs off) select * from v2;

drop view v1;
drop view v2;
drop table test1;
drop table test2;
drop table test3;
drop schema string_digit_to_numeric cascade;
