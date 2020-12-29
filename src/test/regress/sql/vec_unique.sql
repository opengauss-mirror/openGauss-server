/*
 * This file is used to test the function of ExecVecUnique()
 */
set current_schema = vector_unique_engine;
----
--- case 1: Basic Cases
----
explain (verbose on, costs off) select distinct col_num from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_num from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_date from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_time from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_vchar from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_timetz from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_interval from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_float + col_decimal from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct A.col_int1 + B.col_int2 from VECTOR_UNIQUE_TABLE_01 A, VECTOR_UNIQUE_TABLE_02 B order by 1;
select distinct A.col_int1 + B.col_int2 from VECTOR_UNIQUE_TABLE_01 A, ROW_UNIQUE_TABLE_02 B order by 1;
select distinct A.col_int1 + B.col_int2 from VECTOR_UNIQUE_TABLE_01 A, VECTOR_UNIQUE_TABLE_02 B where A.col_char = B.col_char order by 1;

----
--- case 2: With NULL
----
delete from VECTOR_UNIQUE_TABLE_01;
insert into ROW_UNIQUE_TABLE_01 values(1, 80, 100, 20000, 'aa', NULL, 0.01, 10.01, 100.01, '2015-02-16', NULL, '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into ROW_UNIQUE_TABLE_01 values(1, 60, 100, 20000, NULL, 'vvvvvvv', 0.01, 10.01, 100.01, '2015-02-16', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into ROW_UNIQUE_TABLE_01 values(1, 80, 100, 20000, 'aa', 'vvvvvvv', 0.01, 10.01, 100.01, NULL, '16:00:38', '1996-2-6 01:00:30+8', NULL);
insert into ROW_UNIQUE_TABLE_01 values(1, 60, NULL, 20000, 'aa', 'vvvvvvv', NULL, 10.01, 100.01, NULL, '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into VECTOR_UNIQUE_TABLE_01 select * from ROW_UNIQUE_TABLE_01;
select distinct col_time from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_date from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct col_float + col_num from VECTOR_UNIQUE_TABLE_01 order by 1;
select distinct A.col_int1 + B.col_int2 from VECTOR_UNIQUE_TABLE_01 A, VECTOR_UNIQUE_TABLE_02 B where A.col_char = B.col_char order by 1;

----
--- case 3: Count(distinct)
----
explain (costs off) select col_int0, count(distinct col_time) from VECTOR_UNIQUE_TABLE_01 group by col_int0;
select col_int0, count(distinct col_time) from VECTOR_UNIQUE_TABLE_01 group by col_int0 order by 1;
explain (costs off) select col_int1, count(distinct col_time) from VECTOR_UNIQUE_TABLE_01 group by col_int1;
select col_int1, count(distinct col_time) from VECTOR_UNIQUE_TABLE_01 group by col_int1 order by 1;
explain (costs off) select count(distinct col_time) from VECTOR_UNIQUE_TABLE_01;
select count(distinct col_time) from VECTOR_UNIQUE_TABLE_01;
explain (costs off) select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01 group by col_int0);
select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01 group by col_int0) order by 1;
explain (costs off) select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01 group by col_int1);
select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01 group by col_int1) order by 1;
explain (costs off) select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01);
select distinct x from (select count(distinct col_time) x from VECTOR_UNIQUE_TABLE_01);

----
--- Clean Table and Resource
----
drop schema vector_unique_engine cascade;
