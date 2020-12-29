create schema vector_distribute_prepare;
set current_schema = vector_distribute_prepare;
create table test_prepare_1(a int, b numeric) ;
create table test_prepare_2(a int, b int) ;
insert into test_prepare_1 values(1, 1),(1, 1),(1, 1);
insert into test_prepare_2 values(1, 1),(1, 1),(1, 1);

create table prepare_table_01(a int, b int)   with (orientation = column) ;
insert into prepare_table_01 select * from test_prepare_1;
create table prepare_table_02(a int, b int)  with (orientation = column) ;
insert into prepare_table_02 select * from test_prepare_2;
analyze prepare_table_01;
analyze prepare_table_02;

prepare p1 as select * from prepare_table_01, prepare_table_02 where prepare_table_01.a = prepare_table_02.a;
prepare p2(int) as select * from prepare_table_01, prepare_table_02 where prepare_table_01.a = prepare_table_02.a and prepare_table_01.b = $1;

prepare p3 as select * from prepare_table_01, prepare_table_02 where prepare_table_01.a = prepare_table_02.a;
prepare p4(int) as select * from prepare_table_01, prepare_table_02 where prepare_table_01.a = prepare_table_02.a and prepare_table_01.b = $1;

--stream plan and stream exec.
explain (costs off, verbose on) execute p1;
execute p1;
execute p1;

explain (costs off, verbose on) execute p2(1);
execute p2(1);
execute p2(1);
execute p2(2);
execute p2(2);

--remote plan and stream exec.
explain (costs off, verbose on) execute p3;
execute p3;

explain (costs off, verbose on) execute p4(1);
execute p4(1);

--stream plan and remote exec.
explain (costs off, verbose on) execute p1;
execute p1;
execute p1;

explain (costs off, verbose on) execute p2(1);
execute p2(1);
execute p2(1);
execute p2(2);

--remote plan and remote exec.
explain (costs off, verbose on) execute p3;
execute p3;

explain (costs off, verbose on) execute p4(1);
execute p4(1);

prepare p5 as select a from prepare_table_01 INTERSECT select a from prepare_table_02 order by 1 limit 1;
explain (costs off, verbose on) execute p5;

prepare p6 as (select * from prepare_table_01) union (select * from prepare_table_02); 
explain (costs off, verbose on) execute p6;

prepare p7 as  select a from prepare_table_01 where current_date<'2013-07-20' ;
explain (costs off, verbose on) execute p7;

drop table test_prepare_1;
drop table test_prepare_2;
drop table prepare_table_01;
drop table prepare_table_02;
drop schema vector_distribute_prepare cascade;