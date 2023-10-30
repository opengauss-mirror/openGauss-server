--
-- XC_FQS_JOIN
--
-- This file contains testcases for JOINs, it does not test the expressions
-- create the tables first

CREATE TABLE tab1_rep (val int, val2 int);
ALTER TABLE tab1_rep ADD PRIMARY KEY(val, val2);
insert into tab1_rep (select * from generate_series(1, 5) a, generate_series(1, 5) b);
CREATE TABLE tab2_rep as select * from tab1_rep;
CREATE TABLE tab3_rep as select * from tab1_rep;
CREATE TABLE tab4_rep as select * from tab1_rep;
CREATE TABLE tab1_mod as select * from tab1_rep;
CREATE TABLE tab2_mod as select * from tab1_rep;
CREATE TABLE tab3_mod as select * from tab1_rep;
CREATE TABLE single_node_rep_tab as select * from tab1_rep limit 0;
CREATE TABLE single_node_mod_tab as select * from tab1_rep limit 0;
-- populate single node tables specially
insert into single_node_rep_tab values (1, 2), (3, 4);
insert into single_node_mod_tab values (1, 2), (5, 6);

-- Join involving replicated tables only, all of them should be shippable
select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 1 and tab1_rep.val < 4;
explain (costs off, verbose on) select * from tab1_rep, tab2_rep where tab1_rep.val = tab2_rep.val and
										tab1_rep.val2 = tab2_rep.val2 and
										tab1_rep.val > 3 and tab1_rep.val < 5;
select * from tab1_rep natural join tab2_rep 
			where tab2_rep.val > 2 and tab2_rep.val < 5;
explain (costs off, verbose on) select * from tab1_rep natural join tab2_rep
			where tab2_rep.val > 2 and tab2_rep.val < 5;
select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
									where tab1_rep.val > 0 and tab2_rep.val < 3; 
explain (costs off, verbose on) select * from tab1_rep join tab2_rep using (val, val2) join tab3_rep using (val, val2)
							where tab1_rep.val > 0 and tab2_rep.val < 3; 
select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (costs off, verbose on) select * from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- make sure in Joins which are shippable and involve only one node, aggregates
-- are shipped to
select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
explain (costs off, verbose on) select avg(tab1_rep.val) from tab1_rep natural join tab2_rep natural join tab3_rep
			where tab1_rep.val > 0 and tab2_rep.val < 3;
-- the two replicated tables being joined do not have any node in common, the
-- query is not shippable
select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;
explain (costs off, verbose on) select * from tab3_rep natural join tab4_rep
			where tab3_rep.val > 2 and tab4_rep.val < 5;
-- Join involving one distributed and one replicated table, with replicated
-- table existing on all nodes where distributed table exists. should be
-- shippable
select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;
explain (costs off, verbose on) select * from tab1_mod natural join tab1_rep
			where tab1_mod.val > 2 and tab1_rep.val < 4;

-- Join involving one distributed and one replicated table, with replicated
-- table existing on only some of the nodes where distributed table exists.
-- should not be shippable
select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on) select * from tab1_mod natural join tab4_rep
			where tab1_mod.val > 2 and tab4_rep.val < 4;

-- Join involving two distributed tables, never shipped
select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;
explain (costs off, verbose on) select * from tab1_mod natural join tab2_mod
			where tab1_mod.val > 2 and tab2_mod.val < 4;

-- Join involving a distributed table and two replicated tables, such that the
-- distributed table exists only on nodes common to replicated tables, try few
-- permutations
select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on) select * from tab2_rep natural join tab4_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on) select * from tab4_rep natural join tab2_rep natural join tab2_mod
			where tab2_rep.val > 2 and tab4_rep.val < 4;
select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;
explain (costs off, verbose on) select * from tab2_rep natural join tab2_mod natural join tab4_rep
			where tab2_rep.val > 2 and tab4_rep.val < 4;

-- qualifications on distributed tables
-- In case of 2,3,4 datanodes following join should get shipped completely
select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
explain (costs off, verbose on) select * from tab1_mod natural join tab4_rep where tab1_mod.val = 1 order by tab1_mod.val2;
-- following join between distributed tables should get FQSed because both of
-- them reduce to a single node
select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val2;
explain (costs off, verbose on) select * from tab1_mod join tab2_mod using (val2)
		where tab1_mod.val = 1 and tab2_mod.val = 2 order by tab1_mod.val;

-- JOIN involving the distributed table with equi-JOIN on the distributed column
-- with same kind of distribution on same nodes.
select * from tab1_mod, tab3_mod where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;
explain (costs off, verbose on) select * from tab1_mod, tab3_mod
			where tab1_mod.val = tab3_mod.val and tab1_mod.val = 1;

-- JOIN between relations which are results of subqueries should obey same rules
-- as normal tables
-- replicated subqueries 
select * from (select * from tab1_rep) t1 natural join (select * from tab2_rep) t2
			where t1.val > 1 and t1.val < 4
			order by t1.val, t1.val2;
explain (costs off, verbose on)
	select * from (select * from tab1_rep) t1 natural join (select * from tab2_rep) t2
				where t1.val > 1 and t1.val < 4
				order by t1.val, t1.val2;
select * from (select avg(val2), val from tab1_rep group by val) t1 natural join
				(select avg(val2), val from tab2_rep group by val) t2
			order by 1, 2;
explain (costs off, verbose on)
	select * from (select avg(val2), val from tab1_rep group by val) t1 natural join
					(select avg(val2), val from tab2_rep group by val) t2
				order by 1, 2;
-- replicated and distributed subqueries 
select * from (select avg(val2), val from tab1_mod group by val) t1 natural join
				(select avg(val2), val from tab1_rep group by val) t2
			where t1.val = 3;
explain (costs off, verbose on)
	select * from (select avg(val2), val from tab1_mod group by val) t1 natural join
					(select avg(val2), val from tab1_rep group by val) t2
				where t1.val = 3;
-- distributed subqueries
select * from (select avg(val2), val from tab1_mod group by val) t1 natural join
				(select avg(val2), val from tab3_mod group by val) t2
			where t1.val = 3;
explain (costs off, verbose on)
	select * from (select avg(val2), val from tab1_mod group by val) t1 natural join
					(select avg(val2), val from tab3_mod group by val) t2
			where t1.val = 3;

-- OUTER joins, we insert some data in existing tables for testing OUTER join
-- OUTER join between two replicated tables is shippable if they have a common
-- datanode.
insert into tab1_rep values (100, 200);
insert into tab2_rep values (3000, 4000);
select * from tab1_rep left join tab2_rep on (tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2)
			where tab2_rep.val = tab2_rep.val2 or tab2_rep.val is null
			order by tab1_rep.val, tab1_rep.val2;
explain (costs off, verbose on)
select * from tab1_rep left join tab2_rep on (tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2)
			where tab1_rep.val = tab1_rep.val2 or tab2_rep.val is null
			order by tab1_rep.val, tab1_rep.val2;
-- FULL OUTER join
select * from tab1_rep full join tab2_rep on (tab1_rep.val < tab2_rep.val and tab1_rep.val2 = tab2_rep.val2) 
					where tab1_rep.val > 5 or tab2_rep.val > 5
					order by tab1_rep.val, tab2_rep.val, tab1_rep.val2, tab2_rep.val2;
explain (costs off, verbose on)
select * from tab1_rep full join tab2_rep on (tab1_rep.val < tab2_rep.val and tab1_rep.val2 = tab2_rep.val2)
					where tab1_rep.val > 5 or tab2_rep.val > 5
					order by tab1_rep.val, tab2_rep.val, tab1_rep.val2, tab2_rep.val2;
-- OUTER join between two distributed tables is shippable if it's an equi-join
-- on the distribution columns, such that distribution columns are of same type
-- and the relations are distributed on same set of nodes
insert into tab1_mod values (100, 200);
insert into tab3_mod values (3000, 4000);
select * from tab1_mod left join tab3_mod on (tab1_mod.val = tab3_mod.val and tab1_mod.val2 = tab3_mod.val2)
			where tab3_mod.val = tab3_mod.val2 or tab3_mod.val is null
			order by tab1_mod.val, tab1_mod.val2;
explain (costs off, verbose on)
select * from tab1_mod left join tab3_mod on (tab1_mod.val = tab3_mod.val and tab1_mod.val2 = tab3_mod.val2)
			where tab3_mod.val = tab3_mod.val2 or tab3_mod.val is null
			order by  tab1_mod.val, tab1_mod.val2;
-- JOIN condition is not equi-join on distribution column, join is not shippable
select * from tab1_mod left join tab3_mod using (val2)
			where (tab1_mod.val = tab1_mod.val2 and tab3_mod.val = tab3_mod.val2) or tab3_mod.val is null
			order by tab1_mod.val, tab1_mod.val2, tab3_mod.val2;
explain (costs off, verbose on)
select * from tab1_mod left join tab3_mod using (val2)
			where (tab1_mod.val = tab1_mod.val2 and tab3_mod.val = tab3_mod.val2) or tab3_mod.val is null
			order by  tab1_mod.val, tab1_mod.val2, tab3_mod.val2;
-- OUTER join between replicated and distributed tables is shippable if the
-- the replicated table is available on all the datanodes where outer side is
-- distributed
select * from tab1_mod left join tab1_rep on (tab1_mod.val < tab1_rep.val and tab1_mod.val2 = tab1_rep.val2)
			where tab1_mod.val >= 5
			order by tab1_mod.val, tab1_mod.val2, tab1_rep.val, tab1_rep.val2;
explain (costs off, verbose on)
select * from tab1_mod left join tab1_rep on (tab1_mod.val < tab1_rep.val and tab1_mod.val2 = tab1_rep.val2)
			where tab1_mod.val >= 5
			order by tab1_mod.val, tab1_mod.val2, tab1_rep.val, tab1_rep.val2;
-- OUTER side is replicated and inner is distributed, join is not shippable,
-- just check the EXPLAIN outputs.
explain (costs off, verbose on)
select * from tab1_mod right join tab1_rep on (tab1_mod.val > tab1_rep.val and tab1_mod.val2 = tab1_rep.val2)
			where tab1_rep.val >= 5;
explain (costs off, verbose on)
select * from tab1_rep left join tab1_mod on (tab1_mod.val > tab1_rep.val and tab1_mod.val2 = tab1_rep.val2)
			where tab1_rep.val >= 5;
-- Any join involving a distributed and replicated node each located on a single
-- and same node should be shippable
select * from single_node_rep_tab natural full outer join single_node_mod_tab order by val, val2;
explain (costs off, verbose on)
select * from single_node_rep_tab natural full outer join single_node_mod_tab order by val, val2;

-- DMLs involving JOINs are not FQSed
-- We need to just make sure that FQS is not kicking in. But the JOINs can still
-- be reduced by JOIN reduction optimization. Turn this optimization off so as
-- to generate plans independent of number of nodes in the cluster.
explain (costs off, verbose on) update tab1_mod set val2 = 1000 from tab2_mod 
		where tab1_mod.val = tab2_mod.val and tab1_mod. val2 = tab2_mod.val2;
explain (costs off, verbose on) delete from tab1_mod using tab2_mod
		where tab1_mod.val = tab2_mod.val and tab1_mod.val2 = tab2_mod.val2;
explain (costs off, verbose on) update tab1_rep set val2 = 1000 from tab2_rep
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;
explain (costs off, verbose on) delete from tab1_rep using tab2_rep 
		where tab1_rep.val = tab2_rep.val and tab1_rep.val2 = tab2_rep.val2;

drop table tab1_rep;
drop table tab2_rep;
drop table tab3_rep;
drop table tab4_rep;
drop table tab1_mod;
drop table tab2_mod;
drop table tab3_mod;
drop table single_node_mod_tab;
drop table single_node_rep_tab;

-- unshippable qual check during "path" phase

create table t1 (a int, b int, c int, d int);
create table rt1 (a int, b int, c int, d int);

explain (costs off, verbose on)
SELECT a
FROM rt1
WHERE EXISTS (
SELECT 1
);
SELECT a
FROM rt1
WHERE EXISTS (
SELECT 1
);

explain (costs off, verbose on)
SELECT *
FROM t1
LEFT OUTER JOIN (
SELECT a
FROM rt1
WHERE EXISTS (
SELECT 1
)
) dt ON 1 = 1;
SELECT *
FROM t1
LEFT OUTER JOIN (
SELECT a
FROM rt1
WHERE EXISTS (
SELECT 1
)
) dt ON 1 = 1;

explain (costs off, verbose on)
SELECT *
FROM t1
LEFT OUTER JOIN (
SELECT rt1.a
FROM rt1, rt1 as rt2
WHERE EXISTS (
SELECT 1
)
) dt ON 1 = 1;
SELECT *
FROM t1
LEFT OUTER JOIN (
SELECT rt1.a
FROM rt1, rt1 as rt2
WHERE EXISTS (
SELECT 1
)
) dt ON 1 = 1;

drop table t1;
drop table rt1;

--test inner unique + smp
set query_dop=32;
set enable_inner_unique_opt=true;
set enable_material=off;
create table test_a(a int, b int);
create table test_b(c int, d int);
insert into test_a values(1,2),(1,3),(1,4);
insert into test_b values(1,2),(1,2),(1,3),(1,3),(1,4),(1,4);
create unique index test_inner_idx on test_a(b);

--merge join
explain (verbose ,costs off)
select /*+ mergejoin(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d;
select /*+ mergejoin(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d;

--hash join
explain (verbose ,costs off)
select /*+ hashjoin(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d order by a,b;
select /*+ hashjoin(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d order by a,b;

--nestloop
explain (verbose ,costs off)
select /*+ nestloop(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d order by a,b;
select /*+ nestloop(test_a test_b) leading((test_b test_a)) indexscan(test_a test_inner_idx)*/ * from test_a,test_b where b=d order by a,b;

drop table test_a;
drop table test_b;
reset query_dop;
reset enable_inner_unique_opt;
reset enable_material;
