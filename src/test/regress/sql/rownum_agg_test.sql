--
-- rownum_agg_test.sql
-- Regression coverage for ROWNUM across every clause it may legitimately
-- appear in. Validates the Agg-side fix for max/min/sum/avg/count(rownum)
-- and guards the existing scan-level / WHERE->LIMIT / group-by-rownum
-- behaviors against regressions.

-- ========== initlize ==========
drop table if exists rt;

-- ========== start test (create table & insert values) ==========
create table rt(id int, grp int, val int);
insert into rt values (1,1,10),(2,2,20),(3,0,30),(4,1,40),(5,2,50),
                      (6,0,60),(7,1,70),(8,2,80),(9,0,90),(10,1,100);

-- ========== case 1. plain SELECT list ==========
-- expect: (rownum,id) = (1,1),(2,2),(3,3)
select rownum, id from rt where id <= 3 order by id;

-- ========== case 2a. WHERE rownum < N  ->  LIMIT ==========
-- expect: id = 1,2,3
select id from rt where rownum < 4 order by id;

-- ========== case 2b. rownum > N  ->  LIMIT 0 ==========
-- expect: empty
select id from rt where rownum > 5;

-- ========== case 2c. rownum = N  ->  LIMIT 0  ==========
-- expect: empty
select id from rt where rownum = 5;

-- ========== case 3a. aggregate arguments over the full set ==========
-- expect: max=10, min=1, sum=55, count=10
select max(rownum), min(rownum), sum(rownum), count(rownum) from rt;

-- ========== case 3b. with a WHERE filter (post-filter numbering) ==========
-- expect: max=3, count=3   (rows id 1,3,9 -> rownum 1,2,3)
select max(rownum), count(rownum) from rt where id in (1,3,9);

-- ========== case 3c. ==========
-- expect: max=5
select max(rownum) from rt where id between 4 and 8;

-- ========== case 4. rownum inside an expression in an agg arg ==========
-- expect: 20
select max(rownum * 2) from rt;
-- expect: 65  (= 2+3+...+11)
select sum(rownum + 1) from rt;

-- ========== case 5. GROUP BY rownum ==========
-- expect: 1..10
select rownum from rt group by rownum order by 1;

-- ========== case 6a. GROUP BY another column, rownum in agg arg (order-independent) ==========
-- expect: (grp,count) = (0,3),(1,4),(2,3)
select grp, count(rownum) from rt group by grp order by grp;

-- ========== case 6b. order-dependent (seqscan = id order) ==========
-- expect: (grp,sum) = (0,18),(1,22),(2,15)
select grp, sum(rownum) from rt group by grp order by grp;

-- ========== case 7a. GROUP BY rownum + HAVING on the grouping column ==========
-- count(*) is rows-per-group; group by rownum => 1 row per group => count is 1
-- expect: (rownum,count) = (1,1),(2,1)
select rownum, count(*) from rt group by rownum having rownum < 3 order by 1;

-- ========== case 7b. aggregate-of-rownum in HAVING (sum=55 > 50) ==========
-- expect: count = 10   (pre-fix: empty, sum was 10)
select count(*) from rt having sum(rownum) > 50;

-- ========== case 7c. HAVING false ==========
-- expect: empty
select count(*) from rt having sum(rownum) > 60;

-- ========== case 8. ORDER BY rownum ==========
-- expect: (rownum,id) = (10,10),(9,9),(8,8)
select rownum, id from rt order by rownum desc fetch first 3 rows only;

-- ========== case 9a. DISTINCT rownum ==========
-- expect: 1..10
select distinct rownum from rt order by 1;

-- ========== case 9b. count(distinct rownum) ==========
-- expect: 10
select count(distinct rownum) from rt;

-- ========== case 10. ORDERED aggregate (rownum in agg ORDER BY) ==========
-- expect: {1,2,3,4,5,6,7,8,9,10}
select array_agg(id order by rownum) from rt;

-- ========== case 11. FILTER clause ==========
-- expect: 3
select count(rownum) filter (where grp = 0) from rt;
-- expect: 18  (ids 3,6,9 -> rownum 3,6,9)
select sum(rownum) filter (where grp = 0) from rt;

-- ========== case 12. WHERE rownum (NOT rewritten because
--           hasAggs short-circuits preprocess_rownum) + aggregate ==========
-- The scan keeps the first 5 rows (rownum<=5), then the agg sees rownum 1..5.
-- expect: max = 5
select max(rownum) from rt where rownum <= 5;

-- ========== case 13. subquery / CTE / limit-subquery (rownum materialized
--           by the inner node) ==========
-- expect: id = 1,2,3
select id from rt where id in (select rownum from rt where id <= 3) order by id;
-- expect: 5
with s as (select rownum rn, id from rt where id <= 5) select max(rn) from s;
-- expect: 5
select max(rownum) from (select * from rt limit 5) q;

-- ========== case 14. set operation (rownum resets per branch) ==========
-- expect: 1,1,2,2,3
select rownum from rt where id <= 3
union all
select rownum from rt where id <= 2
order by 1;

-- ========== case 15. JOIN (rownum counts join output rows) ==========
-- expect: (rownum,id) = (1,1),(2,2),(3,3)
select rownum, a.id from rt a join rt b on a.id = b.id where a.id <= 3 order by a.id;

-- ========== case 16. WINDOW aggregate (Agg-only patch may not cover WindowAgg) ==========
-- EXPECTED if window-arg rownum is correct: (id,sum) = (1,1),(2,3),(3,6)
--   running sum over id order = 1, 1+2, 1+2+3
select id, sum(rownum) over (order by id) from rt order by id fetch first 3 rows only;

-- ========== case 17. type-compat (NUMERIC) ==========
set behavior_compat_options = 'rownum_type_compat';
-- expect: 10
select max(rownum) from rt;
-- expect: numeric
select pg_typeof(max(rownum)) from rt;
set behavior_compat_options = '';

-- ========== case 18. parse rule: bare rownum over an aggregate is rejected ==========
-- expect: ERROR: ROWNUM must appear in the GROUP BY clause or be used in an aggregate function
select rownum, count(*) from rt;
-- expect: ERROR (same)
select id from rt group by id having rownum < 5;

-- ========== case 19a. rownum carried through a sort (Oracle idiom) ==========
drop table if exists rt2;
create table rt2(id int, amount int);
insert into rt2 values (1, 2), (2, 1), (3, 3);
-- scan/heap order: id1(amt2), id2(amt1), id3(amt3) -> rownum 1,2,3
-- amount desc: id3, id1, id2 -> carried rownum reads 3,1,2

-- ========== case 19b. carry-through idiom with a top-level ORDER BY ==========
-- expect: rownum can not bu alias
select q.rn as rownum, q.id, q.amount
  from (select rownum as rn, t.* from rt2 t) q
 order by q.amount desc;

-- ========== case 19c.window aggregate over the carried column ==========
-- expect: (id,running_rn) = (3,3),(1,4),(2,6)
select q.id, sum(q.rn) over (order by q.amount desc) as running_rn
  from (select rownum as rn, t.* from rt2 t) q
 order by q.amount desc;

-- ========== case 19d. inner rownum with its own order-by sibling still computes in scan order ==========
-- expect: (rn,id) = (2,2),(1,1),(3,3)  (amount asc: id2,id1,id3 -> rn 2,1,3)
select q.rn, q.id
  from (select rownum as rn, t.* from rt2 t) q
 order by q.amount asc;

-- ==========19e. FLAT query, now fixed by carry-through (preprocess_rownum_carrythrough) ==========
-- the bare rownum is materialized at the scan and carried through the sort.
-- expect: (rownum,row_number) = (3,1),(1,2),(2,3)
select rownum, row_number() over (order by amount desc) from rt2 order by amount desc;

-- ========== 19f. plain ORDER BY, no window: same carry-through ==========
-- expect: (rownum,id) = (3,3),(1,1),(2,2)
select rownum, id from rt2 order by amount desc;

-- ========== finish test & drop tables ==========
drop table rt2;
drop table rt;
