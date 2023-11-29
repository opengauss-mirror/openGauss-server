set explain_perf_mode=pretty;
explain (costs false) 
WITH RECURSIVE   x(id) AS
(
	VALUES (1)
	UNION ALL
	SELECT id+1 FROM x WHERE id < 5
), y(id) AS
(
	VALUES (1)
	UNION ALL
	SELECT id+1 FROM x WHERE id < 10
)
SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY 1;

WITH RECURSIVE   x(id) AS
(
	VALUES (1)
	UNION ALL
	SELECT id+1 FROM x WHERE id < 5
), y(id) AS
(
	VALUES (1)
	UNION ALL
	SELECT id+1 FROM x WHERE id < 10
)
SELECT y.*, x.* FROM y LEFT JOIN x USING (id) ORDER BY 1;

explain (costs false) 
WITH recursive t_result AS (
SELECT *
FROM test_rec
WHERE dm = '3'
UNION ALL
SELECT t2.*
FROM t_result t1
JOIN test_rec t2 ON t2.dm = t1.sj_dm
)
SELECT * FROM t_result order by 1;

WITH recursive t_result AS (
SELECT *
FROM test_rec
WHERE dm = '3'
UNION ALL
SELECT t2.*
FROM t_result t1
JOIN test_rec t2 ON t2.dm = t1.sj_dm
)
SELECT * FROM t_result order by 1;


/*
 * verify if recursive cte appears multi times
 */
explain (costs false) 
with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq where id > 5
union all
select id, name from rq where id < 5 order by 1;

explain (costs false) 
with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select t1.* from rq t1, rq t2 where t1.id = t2.id order by 1;

with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select id, name from rq where id > 5
union all
select id, name from rq where id < 5 order by 1;


with recursive rq as
(
  select id, name from  chinamap where id = 11
  union all
  select origin.id, rq.name || ' > ' || origin.name
  from rq join chinamap origin on origin.pid = rq.id
)
select t1.* from rq t1, rq t2 where t1.id = t2.id order by 1;

reset explain_perf_mode;

/*
 * verify if stream is on inner branch will not cause incorrect controller creation
 */
create table test_int(id int,name text,pid int, pname text,time timestamp);
create table test_int_rep(id int,name text,pid int, pname text,time timestamp) ;

create table test_number(id number,name text,pid number, pname text,time timestamp);
create table test_number_hash_par(id number,name text,pid number, pname text,time timestamp)
partition by range(time)
(
partition p0 values less than('2018-01-01 00:00:00'),
partition p1 values less than('2018-02-01 00:00:00'),
partition p2 values less than('2018-03-01 00:00:00'),
partition p3 values less than('2018-04-01 00:00:00'),
partition p4 values less than('2018-05-01 00:00:00'),
partition p5 values less than('2018-06-01 00:00:00'),
partition p6 values less than('2018-07-01 00:00:00'),
partition p7 values less than('2018-08-01 00:00:00'),
partition p8 values less than('2018-09-01 00:00:00'),
partition p9 values less than('2018-10-01 00:00:00'),
partition p10 values less than('2018-11-01 00:00:00'),
partition p11 values less than('2018-12-01 00:00:00'),
partition p12 values less than(maxvalue)
);
create table test_number_rep(id number,name text,pid number, pname text,time timestamp) ;
create table test_number_rep_par(id number,name text,pid number, pname text,time timestamp) 
partition by range(time)
(
partition p0 values less than('2018-01-01 00:00:00'),
partition p1 values less than('2018-02-01 00:00:00'),
partition p2 values less than('2018-03-01 00:00:00'),
partition p3 values less than('2018-04-01 00:00:00'),
partition p4 values less than('2018-05-01 00:00:00'),
partition p5 values less than('2018-06-01 00:00:00'),
partition p6 values less than('2018-07-01 00:00:00'),
partition p7 values less than('2018-08-01 00:00:00'),
partition p8 values less than('2018-09-01 00:00:00'),
partition p9 values less than('2018-10-01 00:00:00'),
partition p10 values less than('2018-11-01 00:00:00'),
partition p11 values less than('2018-12-01 00:00:00'),
partition p12 values less than(maxvalue)
);

set explain_perf_mode=pretty;
explain
with recursive tmp1 as (
select case when id < 1000 then id  else id+1 end  as id,
       case when pid >1000 then pid+1 else pid end as pid,
       count(8)    as count
from test_number
group by 1,2
union
select case when id < 1000 then id  else id+1 end  as id,
       case when pid >1000 then pid+1 else pid end as pid ,count(8)
from test_number_rep
where id < 100
group by 1,2
union
select 1,'',1
from test_number
where id between 1 and 3
except
select 1,id,pid
from test_number
where id between 2 and 4
union all
select case when t1.id < 1000 then t1.id  else t1.id+1 end  as id,
       case when t1.pid >1000 then t1.pid+1 else t1.pid end as pid,substr(t1.pid,-2)::int from tmp1 t1 inner join test_int_rep t2 on floor(t2.pid)::int = (case t1.id when t1.id between 1 and 10 then t1.id end))
select t1.count,t2.name,max(distinct t2.time),count(*),t1.pid
 from tmp1 t1
inner join test_number_rep_par t2
on t1.id = (case when t2.pid  is null then 1 end)
inner join test_number_hash_par t3
on (case when t1.pid is null then 3.60525512 end) =  t3.pid
group by 1,2,5
order by 1,2,3,4,5;

with recursive tmp1 as (
select case when id < 1000 then id  else id+1 end  as id,
       case when pid >1000 then pid+1 else pid end as pid,
       count(8)    as count
from test_number
group by 1,2
union
select case when id < 1000 then id  else id+1 end  as id,
       case when pid >1000 then pid+1 else pid end as pid ,count(8)
from test_number_rep
where id < 100
group by 1,2
union
select 1,'',1
from test_number
where id between 1 and 3
except
select 1,id,pid
from test_number
where id between 2 and 4
union all
select case when t1.id < 1000 then t1.id  else t1.id+1 end  as id,
       case when t1.pid >1000 then t1.pid+1 else t1.pid end as pid,substr(t1.pid,-2)::int from tmp1 t1 inner join test_int_rep t2 on floor(t2.pid)::int = (case t1.id when t1.id between 1 and 10 then t1.id end))
select t1.count,t2.name,max(distinct t2.time),count(*),t1.pid
 from tmp1 t1
inner join test_number_rep_par t2
on t1.id = (case when t2.pid  is null then 1 end)
inner join test_number_hash_par t3
on (case when t1.pid is null then 3.60525512 end) =  t3.pid
group by 1,2,5
order by 1,2,3,4,5;

drop table test_int;
drop table test_int_rep;
drop table test_number;
drop table test_number_rep;
drop table test_number_hash_par;
drop table test_number_rep_par;
