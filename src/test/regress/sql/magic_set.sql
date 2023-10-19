create schema magic_set;
set search_path=magic_set;

create table magic_t1(a int, b varchar, c int, d int);
create table magic_t2(a int, b varchar, c int, d int);
create table magic_t3(a int, b varchar, c int, d int);

insert into magic_t1 select generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000) from public.src;
insert into magic_t2 select generate_series(1, 100), generate_series(1, 200), generate_series(1, 300), generate_series(1, 500) from public.src;
insert into magic_t3 select generate_series(1, 100), generate_series(1, 200), generate_series(1, 300), generate_series(1, 500) from public.src;

analyze magic_t1;
analyze magic_t2;
analyze magic_t3;

show rewrite_rule;
--sublink in where qual
explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b = 10 and 
t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b = 10 and 
t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b < 2 and 
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b < 2 and 
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 and t1.b < 55 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 and t1.b < 55 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 or t1.b < 55 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 or t1.b < 55 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4 limit 50;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
(t1.b > 50 or t1.b < 55) and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
(t1.b > 50 or t1.b < 55) and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4 limit 50;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 and t1.b < 55 and COALESCE(t1.a::varchar, t1.b) = 51 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 50 and t1.b < 55 and COALESCE(t1.a::varchar, t1.b) = 51 and
t1.c < (select max(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select COUNT(*) from magic_t1 as t1, magic_t2 as t2 where t1.b < 100 and 
case when t1.b is null then false else true end and 
t1.c <= (select avg(c) from magic_t2 t2 where t1.a = t2.a);

select COUNT(*) from magic_t1 as t1, magic_t2 as t2 where t1.b < 100 and 
case when t1.b is null then false else true end and 
t1.c <= (select avg(c) from magic_t2 t2 where t1.a = t2.a);

explain (costs off) select COUNT(*) from magic_t1 as t1, magic_t2 as t2 where case when t1.b is null then false else true end
and 
t1.c <= (select avg(c) from magic_t2 t2 where t1.a = t2.a);

explain (costs off)
select COUNT(*) from magic_t1 as t1, magic_t2 as t2 where t1.b < 100 and 
t1.c <= (select avg(c) from magic_t2 t2 where t1.a = t2.a);

select COUNT(*) from magic_t1 as t1, magic_t2 as t2 where t1.b < 100 and 
t1.c <= (select avg(c) from magic_t2 t2 where t1.a = t2.a);

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 10 and t1.c < 12 and t1.b = t1.c and 
t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b > 10 and t1.c < 12 and t1.b = t1.c and 
t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.b > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.b > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a)order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a and t1.b = t2.b);

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a and t1.b = t2.b)order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.b = t2.b and t1.c = t2.c);

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where t1.a > 99 and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.b = t2.b and t1.c = t2.c)
order by 1,2,3,4;
--include or clause
explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a);

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a)order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a and t1.b = t2.b)
order by 1,2,3,4;

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a and t1.b = t2.b)
order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.c = t2.c and t1.b = t2.b and t1.d = t2.d)
order by 1,2,3,4;

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > 999 or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.c = t2.c and t1.b = t2.b and t1.d = t2.d)
order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > t2.b or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a)
order by 1,2,3,4;

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) 
where (t1.a > t2.b or t1.a < 2) and 
t1.c <= (select sum(c) from magic_t2 t2 where t1.a = t2.a)
order by 1,2,3,4;

--sublink in join qual
explain (costs off)
select t2.* from magic_t1 as t1 inner join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b > 50 and t2.b < 55 and t2.c is null
order by 1,2,3,4;

select t2.* from magic_t1 as t1 inner join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b > 50 and t2.b < 55 and t2.c is null
order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 inner join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b in (select c from magic_t3)
order by 1,2,3,4;

select t2.* from magic_t1 as t1 inner join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b in (select c from magic_t3)
order by 1,2,3,4 limit 50;

--out join unsupport 
explain (costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b > 50 and t2.b < 55
order by 1,2,3,4;

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on 
t1.a = t2.a and 
t2.c < (select max(c) from magic_t3 t3 where t2.a = t3.a)
where t2.b > 50 and t2.b < 55
order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1 right join magic_t2 as t2 on 
t1.a = t2.a and 
t1.c < (select max(c) from magic_t3 t3 where t1.a = t3.a)
where t1.b > 50 and t1.b < 55 order by 1,2,3,4;

select t2.* from magic_t1 as t1 right join magic_t2 as t2 on 
t1.a = t2.a and 
t1.c < (select max(c) from magic_t3 t3 where t1.a = t3.a)
where t1.b > 50 and t1.b < 55 order by 1,2,3,4;

explain (costs off)
select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2 inner join magic_t3 as t3 on (1 = 1) where
 (t1.a = t3.a)) 
and
t1.b > 100 order by 1,2,3,4;

select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2 inner join magic_t3 as t3 on (1 = 1) where
 (t1.a = t3.a)) 
and
t1.b > 100 order by 1,2,3,4;

explain (costs off)
select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2, magic_t3 as t4 where t1.a = t4.a) and
t1.b > 100 order by 1,2,3,4;

select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2, magic_t3 as t4 where t1.a = t4.a) and
t1.b > 100 order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
and t1.c < (select sum(a) from magic_t3 as t3 where t1.a = t3.a)
where t1.b > 100 order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
and t1.c < (select sum(a) from magic_t3 as t3 where t1.a = t3.a)
where t1.b > 100 order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
and t1.c <= (select sum(a) from (select a from magic_t3 group by a) as t3 where t1.a = t3.a)
where t1.b >= 100 order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
and t1.c <= (select sum(a) from (select a from magic_t3 group by a) as t3 where t1.a = t3.a)
where t1.b >= 100 order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c <= (select sum(a) from (select a from magic_t3 group by a) as t3 where t1.a = t3.a)
and t1.b >= 100 order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c <= (select sum(a) from (select a from magic_t3 group by a) as t3 where t1.a = t3.a)
and t1.b >= 100 order by 1,2,3,4;

--with more table correlation
explain(costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b = 10 and t2.b = 10 and
t1.c < (select sum(c) from magic_t3 t3 where t1.a = t3.a and t2.a = t3.a)
order by 1,2,3,4;

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.a = t2.a and 
t1.b = 10 and t2.b = 10 and
t1.c < (select sum(c) from magic_t3 t3 where t1.a = t3.a and t2.a = t3.a)
order by 1,2,3,4;

explain (costs off)
select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.b = t2.b and 
t1.b = 10 and t2.c = 10 and COALESCE(t1.a::varchar, t1.b) = 10 
and COALESCE(t2.a::varchar, t2.b) = 10 and
t1.c < (select sum(c) from magic_t3 t3 where t1.a = t3.a and t2.a = t3.a)
order by 1,2,3,4;

select t2.* from magic_t1 as t1, magic_t2 as t2 where t1.b = t2.b and 
t1.b = 10 and t2.c = 10 and COALESCE(t1.a::varchar, t1.b) = 10 
and COALESCE(t2.a::varchar, t2.b) = 10 and
t1.c < (select sum(c) from magic_t3 t3 where t1.a = t3.a and t2.a = t3.a)
order by 1,2,3,4;

explain(costs off)
select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a) 
where t2.b is null;

select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a) 
where t2.b is null;

explain(costs off)
select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a and 
t3.a in (select a from magic_t2 as t2 where t2.b is null)) 
where t2.b is null;

select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a and 
t3.a in (select a from magic_t2 as t2 where t2.b is null)) 
where t2.b is null;

explain (costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a) 
where t1.b > 100 and 
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b) and 
t1.b < 200 and
t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a)
order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a) 
where t1.b > 100 and 
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b) and 
t1.b < 200 and
t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a)
order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a) and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b)order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a) and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b)order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t1.b > 100 and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b)order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t1.b > 100 and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b)order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b) and t1.b > 100
order by 1,2,3,4; 

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t2.a in (select a from magic_t3 as t3 where t2.b = t3.b) and t1.b > 100
order by 1,2,3,4; 

explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t2.a > (select a from magic_t3 as t3 where t2.b = t3.b limit 1) and t1.b > 100
order by 1,2,3,4; 

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(a) from (select a from magic_t3) as t4 where t1.a = t4.a) and
t2.a > (select a from magic_t3 as t3 where t2.b = t3.b limit 1) and t1.b > 100
order by 1,2,3,4; 

--can not pull up
--sublink can not pullup
explain(costs off)
select t2.* from magic_t1 as t1 full join magic_t2 as t2 on 
t1.a = t2.a and 
t1.c < (select max(c) from magic_t3 t3 where t1.a = t3.a)
where t1.b > 50 and t1.b < 55 order by 1,2,3,4;

select t2.* from magic_t1 as t1 full join magic_t2 as t2 on 
t1.a = t2.a and 
t1.c < (select max(c) from magic_t3 t3 where t1.a = t3.a)
where t1.b > 50 and t1.b < 55 order by 1,2,3,4 limit 50;

--not to search join quals, out join can lead to result error
explain (costs off)
select t2.* from magic_t1 as t1 right join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

select t2.* from magic_t1 as t1 right join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

explain(costs off)
select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4;

select t2.* from magic_t1 as t1 left join magic_t2 as t2 on (t1.a = t2.a) and t1.b > 100 
where t1.c < (select sum(c) from magic_t2 t2 where t1.a = t2.a) order by 1,2,3,4 limit 10;

--expr sublink include any_sublink, can not pull
explain(costs off)
select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a and t4.a in (select a from magic_t3)) and
t1.b > 100 order by 1,2,3,4;

select t2.* from magic_t1 as t1 join magic_t2 as t2 on (t1.a = t2.a)
where t1.c < (select sum(c) from magic_t2 as t4 where t1.a = t4.a and t4.a in (select a from magic_t3)) and
t1.b > 100 order by 1,2,3,4;

--expr sublink can not pull up, include left join
explain(costs off)
select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2 left join magic_t3 as t3 on (1 = 1) where
 (t1.a = t3.a)) and
t1.b > 100 order by 1,2,3,4;

select * from magic_t1 as t1
where t1.c < (select sum(t2.a) from magic_t2 as t2 left join magic_t3 as t3 on (1 = 1) where
 (t1.a = t3.a)) and
t1.b > 100 order by 1,2,3,4;

drop table magic_t1;
drop table magic_t2;
drop table magic_t3;

create table magic_t1(a int, b int, c int);
create table magic_t2(a int, b int, c int);
create table magic_t3(a int, b int, c int);
insert into magic_t1 values(1, 1, 1);
insert into magic_t2 values(1, 1, 1);
insert into magic_t3 values(1, 1, 1);

select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a) 
where t2.b is null;

select count(*) from magic_t1 as t1 left join magic_t2 as t2 on t2.c <= (select max(c) from magic_t3 as t3 where t2.a = t3.a and 
t3.a in (select a from magic_t2 as t2 where t2.b is null)) 
where t2.b is null;

drop table magic_t1;
drop table magic_t2;
drop table magic_t3;

create table magic_t1(t1_a int, t1_b varchar, t1_c int, t1_d int);
create table magic_t2(t2_a int, t2_b varchar, t2_c int, t2_d int);
create table magic_t3(t3_a int, t3_b varchar, t3_c int, t3_d int);
create table magic_t4(t4_a int, t4_b varchar, t4_c int, t4_d int);

insert into magic_t1 values(generate_series(1, 100), generate_series(1, 100), generate_series(1, 100), generate_series(1, 100));
insert into magic_t2 values(generate_series(1, 30), generate_series(1, 30), generate_series(1, 30), generate_series(1, 30));
insert into magic_t3 values(generate_series(1, 20), generate_series(1, 20), generate_series(1, 20), generate_series(1, 20));
insert into magic_t4 values(generate_series(1, 10), generate_series(1, 10), generate_series(1, 10), generate_series(1, 10));

analyze magic_t1;
analyze magic_t2;
analyze magic_t3;
analyze magic_t4;

explain(costs off)
select t1_a from magic_t1 join magic_t2 on (t1_a = t2_a)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a);

select t1_a from magic_t1 join magic_t2 on (t1_a = t2_a)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a) order by t1_a;

explain(costs off)
select t2_a from magic_t1 join magic_t2 on (t1_a = t2_a) join magic_t3 on(t2_b = t3_b)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t4_b) from magic_t4 where t4_a = t1_a);

select t2_a from magic_t1 join magic_t2 on (t1_a = t2_a) join magic_t3 on(t2_b = t3_b)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t4_b) from magic_t4 where t4_a = t1_a)order by t2_a;

explain(costs off)
select t1_a from magic_t1 join magic_t2 on (t1_a = t2_a)
where case when t1_b < 10 then true else false end and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a);

select t1_a from magic_t1 join magic_t2 on (t1_a = t2_a)
where case when t1_b < 10 then true else false end and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a) order by t1_a;

explain(costs off)
select t1_a from magic_t1 left join magic_t2 on (t1_a = t2_a)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a);

select t1_a from magic_t1 left join magic_t2 on (t1_a = t2_a)
where t1_b < 10 and t1_c = t2_c and
t1_d >= (select max(t3_b) from magic_t3 where t3_a = t1_a)order by t1_a;

explain(costs off)
select 1 from magic_t1 left join magic_t2 on (t1_a = t2_a and t2_d >=(select max(t3_b) from magic_t3 where t3_a = t2_a))
where t1_b < 10 and t1_c = t2_c;

explain(costs off)
select 1 from magic_t1 right join magic_t2 on (t1_a = t2_a and t1_d >=(select max(t3_b) from magic_t3 where t3_a = t1_a))
where t1_b < 10 and t1_c = t2_c;

explain(costs off)
select t2_b from magic_t1 left join magic_t2 on (t1_a = t2_a) join magic_t3 on 
(t2_a = t3_a and t1_d >=(select max(t4_b) from magic_t4 where t4_a = t1_a))
where t1_b < 10 and t1_c = t2_c;

select t2_b from magic_t1 left join magic_t2 on (t1_a = t2_a) join magic_t3 on 
(t2_a = t3_a and t1_d >=(select max(t4_b) from magic_t4 where t4_a = t1_a))
where t1_b < 10 and t1_c = t2_c order by t2_b;

explain(costs off)
select t1_a from magic_t1 join (select t2_a, t2_b from magic_t2, magic_t3 where t2_a = t3_a) as AA on (t1_a = t2_a)
where t2_a < 10 and
t1_d >=(select max(t4_b) from magic_t4 where t4_a = t2_b);

select t1_a from magic_t1 join (select t2_a, t2_b from magic_t2, magic_t3 where t2_a = t3_a) as AA on (t1_a = t2_a)
where t2_a < 10 and
t1_d >=(select max(t4_b) from magic_t4 where t4_a = t2_b) order by t1_a;

explain (costs off)
select magic_t2.* from magic_t1 join magic_t2 on (1 = 1)
where t1_c < (select sum(t3_a) from magic_t3 where t1_a = t3_a and t1_b = t3_b) and
t2_a > (select max(t4_a) from magic_t4 where t2_b = t4_b and t2_c = t4_c) 
and t1_b > 100 and t1_b = t1_a and t2_a > 10 and t2_d > 20;

select magic_t2.* from magic_t1 join magic_t2 on (1 = 1)
where t1_c < (select sum(t3_a) from magic_t3 where t1_a = t3_a and t1_b = t3_b) and
t2_a > (select max(t4_a) from magic_t4 where t2_b = t4_b and t2_c = t4_c) 
and t1_b > 100 and t1_b = t1_a and t2_a > 10 and t2_d > 20;

explain (costs off)
select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and t1_a = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a = t3_a and t2_a = t3_a);

select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and t1_a = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a = t3_a and t2_a = t3_a);

explain (costs off)
select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and t1_a = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a = t3_a + t3_b);

explain (costs off)
select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a = t3_a + t3_b);

explain (costs off)
select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and t1_a = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a + t2_a = t3_a);

select magic_t2.* from magic_t1, magic_t2 where t1_a = t2_a and
t1_b = 10 and t2_b = 10 and t1_a = 10 and
t1_c < (select sum(t3_c) from magic_t3 where t1_a + t2_a = t3_a);

explain(costs off)
select 1 from magic_t1, magic_t2
where t1_a in 
(select max(t3_a) from magic_t3 where t3_a = t2_a 
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c));

select t1_a from magic_t1, magic_t2
where t1_a in 
(select max(t3_a) from magic_t3 where t3_a = t2_a 
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c)) order by t1_a;

explain(costs off)
select 1 from magic_t1, magic_t2
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c)
union all 
select max(t3_a) from magic_t3 where t3_a = t2_a and t2_a = 1
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c));

select t1_a from magic_t1, magic_t2
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c)
union all 
select max(t3_a) from magic_t3 where t3_a = t2_a and t2_a = 1
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c))
order by t1_a;

explain(costs off)
select 1 from magic_t1 join magic_t2 on(1 = 1)
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c));

select t1_a from magic_t1 join magic_t2 on(1 = 1)
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c))
order by t1_a;

explain(costs off)
select 1 from magic_t1 join magic_t2 on(1 = 1)
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a and t3_b < 100
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c));

select t1_a from magic_t1 join magic_t2 on(1 = 1)
where t1_a in
(select max(t3_a) from magic_t3 where t3_a = t2_a and t3_b < 100
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c))
order by t1_a;

select 1 from magic_t1, magic_t2
where t1_a in
(
select max(t3_a) from magic_t3, magic_t4 where t3_a = t4_a
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c)
union all 
select max(t3_a) from magic_t3 where t3_a = t2_a and t2_a = 1
and t3_b = (select avg(t4_a) from magic_t4 where t3_c = t4_c)) limit 10;
drop table magic_t1;
drop table magic_t2;
drop table magic_t3;
drop table magic_t4;

drop schema magic_set cascade;

