-- this file contains tests for GROUP BY with combinations of following
-- 1. enable_hashagg = on/off (to force the grouping by sorting)
-- 2. distributed or replicated tables across the datanodes
-- If a testcase is added to any of the combinations, please check if it's
-- applicable in other combinations as well.

-- Since we want to test the plan reduction of GROUP and AGG nodes, disable fast
-- query shipping

-- Combination 1: enable_hashagg on and distributed tables
set enable_hashagg to on;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 gt1_val2, xc_groupby_tab2.val2 gt2_val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by gt1_val2, gt2_val2;
explain (verbose true, costs false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 gt1_val2, xc_groupby_tab2.val2 gt2_val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by gt1_val2, gt2_val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by x;
explain (verbose true, costs false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select val2 from xc_groupby_tab1 group by val2 order by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by val + val2;
explain (verbose true, costs false) select val + val2 from xc_groupby_tab1 group by val + val2 order by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
explain (verbose true, costs false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val gt1_val, xc_groupby_tab2.val2 gt2_val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by gt1_val, gt2_val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val gt1_val, xc_groupby_tab2.val2 gt2_val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by gt1_val, gt2_val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by 1;

-- same group by expressions 
select count(xc_groupby_tab1.val), xc_groupby_tab2.val2, xc_groupby_tab2.val2, count(xc_groupby_tab1.val) from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,3 order by 1,2,3,4;
explain (verbose true, costs false) select count(xc_groupby_tab1.val), xc_groupby_tab2.val2, xc_groupby_tab2.val2, count(xc_groupby_tab1.val) from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,3 order by 1,2,3,4;
select count(xc_groupby_tab1.val), xc_groupby_tab2.val2, max(xc_groupby_tab1.val), xc_groupby_tab2.val2, xc_groupby_tab1.val2, count(xc_groupby_tab1.val) from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,4,5 order by 1,2,3,4,5;
explain (verbose true, costs false) select count(xc_groupby_tab1.val), xc_groupby_tab2.val2, max(xc_groupby_tab1.val), xc_groupby_tab2.val2, xc_groupby_tab1.val2, count(xc_groupby_tab1.val) from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,4,5 order by 1,2,3,4,5;
select max(xc_groupby_tab1.val2), xc_groupby_tab1.val + xc_groupby_tab2.val2, min(xc_groupby_tab2.val), xc_groupby_tab2.val2, max(xc_groupby_tab1.val), xc_groupby_tab1.val + xc_groupby_tab2.val2, sum(xc_groupby_tab2.val), xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,4,6,8 order by 1,2,3,4,5,6,7,8;
explain (verbose true, costs false) select max(xc_groupby_tab1.val2), xc_groupby_tab1.val + xc_groupby_tab2.val2, min(xc_groupby_tab2.val), xc_groupby_tab2.val2, max(xc_groupby_tab1.val), xc_groupby_tab1.val + xc_groupby_tab2.val2, sum(xc_groupby_tab2.val), xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by 2,4,6,8 order by 1,2,3,4,5,6,7,8;

-- same group by references
explain (verbose true, costs false) select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 4,1,xc_groupby_tab1.val order by 1, 4;
select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 4,1,xc_groupby_tab1.val order by 1, 4;
explain (verbose true, costs false) select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 3,1,xc_groupby_tab1.val order by 1, 3;
select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 3,1,xc_groupby_tab1.val order by 1, 3;
explain (verbose true, costs false) select distinct on (3, 1, val) xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 order by 1, 3;
select distinct on (3, 1, val) xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 order by 1, 3;
explain (verbose true, costs false) select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 3,1,xc_groupby_tab1.val order by val>some(select val2 from xc_groupby_tab2), 1, 2;
select xc_groupby_tab1.val, xc_groupby_tab1.val2, xc_groupby_tab1.val2 from xc_groupby_tab1 group by 3,1,xc_groupby_tab1.val order by val>some(select val2 from xc_groupby_tab2), 1, 2;

-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 expr from xc_groupby_tab1 group by 2 * val2 order by expr; 
explain (verbose true, costs false) select sum(val), avg(val), 2 * val2 expr from xc_groupby_tab1 group by 2 * val2 order by expr;
--hashagg distinct is 0
SELECT TIMEOFDAY()  Ch_v_x_p_D FROM xc_groupby_tab1 T_x_t_V_n, xc_groupby_tab2 T_D_w_3_t WHERE (27/28+31) = (66) GROUP BY 1;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;
select b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b from xc_groupby_def group by b order by b;
select b,count(b) from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b,count(b) from xc_groupby_def group by b order by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a order by a;
explain (verbose true, costs false) select sum(a) from xc_groupby_g group by a order by a;
select sum(b) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(b) from xc_groupby_g group by b order by b;
select sum(c) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(c) from xc_groupby_g group by b order by b;

select avg(a) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select avg(a) from xc_groupby_g group by b order by b;
select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(b) from xc_groupby_g group by c order by c;
select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(c) from xc_groupby_g group by c order by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 2, enable_hashagg on and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by c1, c2;
explain (verbose true, costs false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2 c1, xc_groupby_tab2.val2 c2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by c1, c2;
-- aggregates over aggregates
select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by sum;
explain (verbose true, costs false) select sum(y) sum from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by sum;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select val2 from xc_groupby_tab1 group by val2 order by val2;
select val + val2 sum from xc_groupby_tab1 group by val + val2 order by sum;
explain (verbose true, costs false) select val + val2 sum from xc_groupby_tab1 group by val + val2 order by sum;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
explain (verbose true, costs false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by val, val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by sum;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 sum from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by sum;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
explain (verbose true, costs false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by 1;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;

select b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b from xc_groupby_def group by b order by b;
select b,count(b) from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b,count(b) from xc_groupby_def group by b order by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a order by a;
explain (verbose true, costs false) select sum(a) from xc_groupby_g group by a order by a;
select sum(b) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(b) from xc_groupby_g group by b order by b;
select sum(c) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(c) from xc_groupby_g group by b order by b;

select avg(a) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select avg(a) from xc_groupby_g group by b order by b;
select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(b) from xc_groupby_g group by c order by c;
select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(c) from xc_groupby_g group by c order by c;

drop table xc_groupby_def;
drop table xc_groupby_g;
reset enable_hashagg;

-- Combination 3 enable_hashagg off and distributed tables
set enable_hashagg to off;
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
explain (verbose true, costs false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
explain (verbose true, costs false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
explain (verbose true, costs false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select val2 from xc_groupby_tab1 group by val2 order by val2;
select val + val2 from xc_groupby_tab1 group by val + val2;
explain (verbose true, costs false) select val + val2 from xc_groupby_tab1 group by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
explain (verbose true, costs false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;

select b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b from xc_groupby_def group by b order by b;
select b,count(b) from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b,count(b) from xc_groupby_def group by b order by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a order by a;
explain (verbose true, costs false) select sum(a) from xc_groupby_g group by a order by a;
select sum(b) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(b) from xc_groupby_g group by b order by b;
select sum(c) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(c) from xc_groupby_g group by b order by b;

select avg(a) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select avg(a) from xc_groupby_g group by b order by b;
select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(b) from xc_groupby_g group by c order by c;
select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(c) from xc_groupby_g group by c order by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

-- Combination 4 enable_hashagg off and replicated tables.
-- repeat the same tests for replicated tables
-- create required tables and fill them with data
create table xc_groupby_tab1 (val int, val2 int);
create table xc_groupby_tab2 (val int, val2 int);
insert into xc_groupby_tab1 values (1, 1), (2, 1), (3, 1), (2, 2), (6, 2), (4, 3), (1, 3), (6, 3);
insert into xc_groupby_tab2 values (1, 1), (4, 1), (8, 1), (2, 4), (9, 4), (3, 4), (4, 2), (5, 2), (3, 2);
select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*), sum(val), avg(val), sum(val)::float8/count(*), val2 from xc_groupby_tab1 group by val2 order by val2;
-- joins and group by
select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
explain (verbose true, costs false) select count(*), sum(xc_groupby_tab1.val * xc_groupby_tab2.val), avg(xc_groupby_tab1.val*xc_groupby_tab2.val), sum(xc_groupby_tab1.val*xc_groupby_tab2.val)::float8/count(*), xc_groupby_tab1.val2, xc_groupby_tab2.val2 from xc_groupby_tab1 full outer join xc_groupby_tab2 on xc_groupby_tab1.val2 = xc_groupby_tab2.val2 group by xc_groupby_tab1.val2, xc_groupby_tab2.val2 order by xc_groupby_tab1.val2, xc_groupby_tab2.val2;
-- aggregates over aggregates
select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by x;
explain (verbose true, costs false) select sum(y) from (select sum(val) y, val2%2 x from xc_groupby_tab1 group by val2) q1 group by x order by x;
-- group by without aggregate
select val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select val2 from xc_groupby_tab1 group by val2 order by val2;
select val + val2 from xc_groupby_tab1 group by val + val2 order by val + val2;
explain (verbose true, costs false) select val + val2 from xc_groupby_tab1 group by val + val2 order by val + val2;
select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
explain (verbose true, costs false) select val + val2, val, val2 from xc_groupby_tab1 group by val, val2 order by val, val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by xc_groupby_tab1.val, xc_groupby_tab2.val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2, xc_groupby_tab1.val, xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val, xc_groupby_tab2.val2 order by xc_groupby_tab1.val, xc_groupby_tab2.val2;
select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by xc_groupby_tab1.val + xc_groupby_tab2.val2;
explain (verbose true, costs false) select xc_groupby_tab1.val + xc_groupby_tab2.val2 from xc_groupby_tab1, xc_groupby_tab2 where xc_groupby_tab1.val = xc_groupby_tab2.val group by xc_groupby_tab1.val + xc_groupby_tab2.val2 order by xc_groupby_tab1.val + xc_groupby_tab2.val2;
-- group by with aggregates in expression
select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
explain (verbose true, costs false) select count(*) + sum(val) + avg(val), val2 from xc_groupby_tab1 group by val2 order by val2;
-- group by with expressions in group by clause
select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
explain (verbose true, costs false) select sum(val), avg(val), 2 * val2 from xc_groupby_tab1 group by 2 * val2 order by 2 * val2;
drop table xc_groupby_tab1;
drop table xc_groupby_tab2;

-- some tests involving nulls, characters, float type etc.
create table xc_groupby_def(a int, b varchar(25)); 
insert into xc_groupby_def VALUES (NULL, NULL);
insert into xc_groupby_def VALUES (1, NULL);
insert into xc_groupby_def VALUES (NULL, 'One');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (2, 'Two');
insert into xc_groupby_def VALUES (3, 'Three');
insert into xc_groupby_def VALUES (4, 'Three');
insert into xc_groupby_def VALUES (5, 'Three');
insert into xc_groupby_def VALUES (6, 'Two');
insert into xc_groupby_def VALUES (7, NULL);
insert into xc_groupby_def VALUES (8, 'Two');
insert into xc_groupby_def VALUES (9, 'Three');
insert into xc_groupby_def VALUES (10, 'Three');

select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select avg(a), sum(a), count(*), b from xc_groupby_def group by b order by b;

select b from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b from xc_groupby_def group by b order by b;
select b,count(b) from xc_groupby_def group by b order by b;
explain (verbose true, costs false) select b,count(b) from xc_groupby_def group by b order by b;
select count(*) from xc_groupby_def where b is null group by b;
explain (verbose true, costs false) select count(*) from xc_groupby_def where b is null group by b;

create table xc_groupby_g(a int, b float, c numeric);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(1,2.1,3.2);
insert into xc_groupby_g values(2,2.3,5.2);

select sum(a) from xc_groupby_g group by a order by a;
explain (verbose true, costs false) select sum(a) from xc_groupby_g group by a order by a;
select sum(b) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(b) from xc_groupby_g group by b order by b;
select sum(c) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select sum(c) from xc_groupby_g group by b order by b;

select avg(a) from xc_groupby_g group by b order by b;
explain (verbose true, costs false) select avg(a) from xc_groupby_g group by b order by b;
select avg(b) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(b) from xc_groupby_g group by c order by c;
select avg(c) from xc_groupby_g group by c order by c;
explain (verbose true, costs false) select avg(c) from xc_groupby_g group by c order by c;

drop table xc_groupby_def;
drop table xc_groupby_g;

reset enable_hashagg;

create table groupby_b (c_int8 int8,c_int2 int2,c_oid oid,c_int4 int4,c_bool bool,c_int2vector int2vector,c_oidvector oidvector,c_char char(10),c_name name,c_text text,c_bpchar bpchar,c_bytea bytea,c_varchar varchar(20),c_float4 float4,c_float8 float8,c_numeric numeric,c_abstime abstime,c_reltime reltime,c_date date,c_time time,c_timestamp timestamp,c_timestamptz timestamptz,c_interval interval,c_timetz timetz,c_box box,c_money money,c_tsvector tsvector);

create or replace function redis_func_0000(rownums integer) returns boolean as $$ declare 
    i integer;
    star_tsw timestamp without time zone;
    star_date date;
    distnums integer;
begin
    distnums = 30;
    star_tsw = '2012-08-21 12:11:50';
    star_date = '1999-01-21';

    for i in 1..rownums loop
        insert into groupby_b values(i,i-35,i+10,i*3,i%2,cast(''||i+4090||' '||i||' '||i-4090||'' as int2vector),cast(''||i-21||' '||i*2||' '||i+21||' '||i*21||'' as oidvector),'turkey'||i||'','hello'||i*322||'','svn_git'||i*112||'','ad '||i*22||'',cast('mpp'||i||'' as bytea),'abs'||i*98||'','10.'||i+2||'','23.'||i+5||'','1024.'||i+16||'',star_date-i,cast(''||(i%4096)/16||'' as reltime),star_date-666+i,(select timestamp 'epoch' +  (i*2121) * interval '11 second')::time,i+star_tsw,star_date+i,cast(''||i+4090||'.4096' as interval),(select timestamp with time zone 'epoch' +  (i*1024) * interval '21 second')::timetz,cast('('||i+4090||','||i||'),('||i-4090||','||i+1024||')' as box),cast(''||i+4090||'.'||i*21||'' as money),cast(''||i||' mpp db ver '||i+1024||' vrspcb' as tsvector));
    end loop;
    return true;     
end;
$$ language plpgsql;

select redis_func_0000(168);

explain (verbose true, costs false)
select (sum(length(c_varchar)),sum(length(c_char))) from groupby_b group by c_int8,c_numeric,c_varchar order by c_int8;
select (sum(length(c_varchar)),sum(length(c_char))) from groupby_b group by c_int8,c_numeric,c_varchar order by c_int8;

explain (verbose true, costs false)
select (sum(c_int2),avg(c_int2),sum(c_int4),avg(c_int4),sum(c_int8),avg(c_int8),sum(c_numeric),avg(c_numeric),sum(length(c_varchar)),avg(length(c_varchar)),sum(length(c_char)),avg(length(c_char)),sum(length(c_text)),avg(length(c_text)),sum(length(c_varchar)),avg(length(c_varchar)),sum(length(c_varchar)),avg(length(c_varchar)))
from groupby_b 
group by c_int8,c_numeric,c_varchar order by c_int8;
select (sum(c_int2),avg(c_int2),sum(c_int4),avg(c_int4),sum(c_int8),avg(c_int8),sum(c_numeric),avg(c_numeric),sum(length(c_varchar)),avg(length(c_varchar)),sum(length(c_char)),avg(length(c_char)),sum(length(c_text)),avg(length(c_text)),sum(length(c_varchar)),avg(length(c_varchar)),sum(length(c_varchar)),avg(length(c_varchar)))
from groupby_b 
group by c_int8,c_numeric,c_varchar order by c_int8;

drop table groupby_b;
drop function redis_func_0000;

reset enable_hashagg;
create table agg_test(a int, b int, c int, d int);
explain (verbose on, costs off) select a from agg_test group by a;
explain (verbose on, costs off) select a, b from agg_test group by a, b;
explain (verbose on, costs off) select a, b from agg_test group by b, a;
explain (verbose on, costs off) select a, b from agg_test group by a, b, c;
explain (verbose on, costs off) select a, c from agg_test group by a, c;
explain (verbose on, costs off) select b, c from agg_test group by b, c;
explain (verbose on, costs off) select distinct a, b from agg_test;
explain (verbose on, costs off) select distinct a, b, c, d from agg_test;
explain (verbose on, costs off) select distinct b, c, d from agg_test;
explain (verbose on, costs off) select distinct c, d from agg_test;
insert into agg_test values(1, 11, generate_series(1, 30) % 5,  generate_series(1, 30) % 6);
insert into agg_test select * from agg_test;
insert into agg_test select * from agg_test;
insert into agg_test select * from agg_test;
insert into agg_test select * from agg_test;
insert into agg_test select * from agg_test;
analyze agg_test;
--agg choose multiple distribute key
explain (verbose on, costs off)select c , d from agg_test group by c, d;
select c, d from agg_test group by c, d order by 1, 2;
drop table agg_test;
