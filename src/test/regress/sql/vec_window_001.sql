
set current_schema=vector_window_engine;

----
--- test 1: Basic Test: Rank()
----
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;

select depname, timeset, rank() over(partition by depname order by timeset) from vector_window_table_01 order by 1, 2;
select depname, timeset, dense_rank() over(partition by depname order by timeset) from vector_window_table_01 order by 1, 2;

SELECT depname, empno, salary, rank() over w FROM vector_window_table_01 window w AS (partition by depname order by salary) order by rank() over w, empno;
SELECT depname, empno, salary, dense_rank() over w FROM vector_window_table_01 window w AS (partition by depname order by salary) order by dense_rank() over w, empno;

select rank() over (partition by depname order by enroll) as rank_1, depname, enroll from vector_window_table_01 order by 2, 3;
select dense_rank() over (partition by depname order by enroll) as rank_1, depname, enroll from vector_window_table_01 order by 2, 3;

select rank() over (partition by depname order by enroll) as rank_1, rank() over (partition by depname order by enroll) as rank_2, depname, enroll from vector_window_table_01 order by 2, 3;
select dense_rank() over (partition by depname order by enroll) as rank_1, dense_rank() over (partition by depname order by enroll) as rank_2, depname, enroll from vector_window_table_01 order by 2, 3;

select  depname, salary, row_number() over(partition by depname order by salary), row_number() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;

select depname, salary, rank() over(partition by depname order by salary), row_number() over(partition by depname order by salary), rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary), row_number() over(partition by depname order by salary), dense_rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2;

select rank() over (order by length('abcd'));

select rank() over (order by 1), count(*) from vector_window_table_01 group by 1;
select dense_rank() over (order by 1), count(*) from vector_window_table_01 group by 1;

select rank() over () from vector_window_table_04 order by 1 ;
select dense_rank() over () from vector_window_table_04 order by 1 ;
select dense_rank() over (), count(*) over ()from vector_window_table_04 order by 1 ;

select col_timetz, col_interval, rank() over(partition by col_timetz order by col_interval) from vector_window_table_06 order by 1, 2;

select col_interval, col_tinterval, rank() over(partition by col_interval order by col_tinterval) from vector_window_table_06 order by 1, 2; 

select col_interval, col_tinterval, rank() over(partition by col_tinterval order by col_interval) from vector_window_table_06 order by 1, 2; 

select c1,c3,rank() over(partition by c1,c2 order by sum_total) from ( select c1,c2,c3,c4,c6,sum(c3+c4)as sum_total  from vector_window_table_07 where c1=to_date('20180620') group by 1,2,3,4,5);

--test case for window agg
select depname,salary,empno,count(*) over(partition by depname ), sum(empno) over(partition by depname),  rank() over(partition by depname) from vector_window_table_01 order by 1, 2, 3;
select depname,salary,empno,count(*) over(partition by depname ), sum(empno) over(partition by depname),  dense_rank() over(partition by depname) from vector_window_table_01 order by 1, 2, 3;

select depname,salary,empno,count(*) over(partition by depname order by salary), sum(empno) over(partition by depname order by salary),  rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2, 3;

select depname,salary,empno,count(*) over(partition by depname order by salary), avg(empno) over(partition by depname order by salary),  rank() over(partition by depname order by salary) from vector_window_table_01 order by 1, 2, 3 limit 100;

select depname, timeset, count(depname) over(partition by depname) ,rank() over(partition by depname) from vector_window_table_01 order by 1, 2;

SELECT depname, empno, salary, sum(1) over (partition by depname),rank() over w FROM vector_window_table_01 window w AS (partition by depname) order by rank() over w, empno;


select rank() over (partition by depname order by enroll) as rank_1, depname, enroll , count(1) over (partition by depname) from vector_window_table_01 order by 2, 3;

select rank() over (partition by depname) as rank_1, depname, enroll , count(1) over (partition by depname) from vector_window_table_01 order by 2, 3;


select count(*) over (partition by depname) as count, sum(salary) over (partition by depname) as sum, depname, enroll from vector_window_table_01 order by 2, 3;

select count(*) over (partition by depname) as count, avg(salary) over (partition by depname) as sum, depname, enroll from vector_window_table_01 order by 2, 3 limit 100;

select  depname, salary, count(*) over(partition by depname),sum(empno) over(partition by depname) from vector_window_table_01 order by 1, 2;
select  depname, salary, count(*) over(order by depname),sum(empno) over(order by depname),row_number() over(order by depname) from vector_window_table_01 order by 1, 2;

select  depname, salary, count(*) over(partition by depname order by depname),sum(empno) over(partition by depname order by depname),row_number() over(partition by depname order by depname) from vector_window_table_01 order by 1, 2;

select depid, rank() over (), count(1) over (), sum(1) over() from vector_window_table_04 order by 1 ;

select c1,c3, count(*) over(partition by c1,c2 order by sum_total) from ( select c1,c2,c3,c4,c6,sum(c3+c4)as sum_total from vector_window_table_07 where c1=to_date('20180620') group by 1,2,3,4,5);

----
--- test 2: Basic Test: Rank() with big amount;
----
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

select depname, salary, enroll, rank() over(partition by depname order by salary, enroll desc) from vector_window_table_02 order by 1, 2, 3;
select depname, salary, enroll, dense_rank() over(partition by depname order by salary, enroll desc) from vector_window_table_02 order by 1, 2, 3;

select salary, rank() over(order by salary) from vector_window_table_02 order by 1, 2;
select salary, dense_rank() over(order by salary) from vector_window_table_02 order by 1, 2;

select salary, rank() over() from vector_window_table_02 order by 1, 2;
select salary, dense_rank() over() from vector_window_table_02 order by 1, 2;

--test case for window agg
select depname, salary,  count(*) over(partition by depname) ,  sum(salary) over(partition by depname) from vector_window_table_02 order by 1, 2;

select depname, salary,  count(*) over(partition by depname order by salary) ,  sum(salary) over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;
select depname, salary,  count(*) over(partition by depname order by salary) ,  avg(salary) over(partition by depname order by salary) from vector_window_table_02 order by 1, 2 limit 100;
select depname, salary,  count(*) over(partition by depname order by salary) ,  min(salary) over(partition by depname order by salary) from vector_window_table_02 order by 1, 2 limit 100;
select depname, salary,  count(*) over(partition by depname order by salary) ,  max(salary) over(partition by depname order by salary) from vector_window_table_02 order by 1, 2 limit 100;
select depname, salary, enroll, rank() over(partition by depname),  sum(1) over(partition by depname) from vector_window_table_02 order by 1, 2, 3;

select depname, salary, enroll, rank() over(partition by depname order by enroll),  sum(1) over(partition by depname order by enroll) from vector_window_table_02 order by 1, 2, 3;

select salary, rank() over(), sum(1) over(), count(*) over() from vector_window_table_02 order by 1, 2;

--test windowagg rescan function
explain (costs off)
select * from vector_window_table_01 t1 where exists (select count(*) over() from vector_window_table_01 t2 where t1.empno=t2.empno);

select * from vector_window_table_01 t1 where exists (select count(*) over() from vector_window_table_01 t2 where t1.empno=t2.empno) order by 1,2,3,4,5;

explain (costs off)
select * from vector_window_table_02 t1 where exists (select depname, salary,  count(*) over(partition by depname order by salary) ,  sum(salary) over(partition by depname order by salary) from vector_window_table_02 t2  where t1.salary=t2.salary order by 1, 2);

select * from vector_window_table_02 t1 where exists (select depname, salary,  count(*) over(partition by depname order by salary) ,  sum(salary) over(partition by depname order by salary) from vector_window_table_02 t2  where t1.salary=t2.salary order by 1, 2) order by 1,2,3 limit 10;

select * from vector_window_table_02 t1 where exists (select depname, salary, enroll, rank() over(partition by depname order by enroll),  sum(1) over(partition by depname order by enroll) from vector_window_table_02  t2 where t1.salary=t2.salary order by 1, 2, 3) order by 1,2,3 limit 10;


delete from vector_window_table_02 where vector_window_table_02.salary = 4200;
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

select depname, salary, rank() over(partition by depname) , sum(salary) over(partition by depname),  sum(salary) over(partition by depname) from vector_window_table_02 order by 1, 2;

select depname, salary, rank() over(partition by depname order by salary) , sum(salary) over(partition by depname order by salary),  sum(salary) over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

delete from vector_window_table_02 where vector_window_table_02.depname = 'develop' or vector_window_table_02.depname = 'sales';
select depname, salary, rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary) from vector_window_table_02 order by 1, 2;

select depname, salary, rank() over(partition by depname), count(1) over(partition by depname), count(depname) over(partition by depname) from vector_window_table_02 order by 1, 2;
select depname, salary, row_number() over(order by depname , salary), count(1) over(order by depname, salary), sum(1) over(order by depname, salary) from vector_window_table_03 order by 1, 2;

select depname, salary, row_number() over(partition by depname order by salary), count(1) over(partition by depname order by salary), avg(salary) over(partition by depname order by salary) from vector_window_table_03 order by 1, 2 limit 100;

select depname, salary, row_number() over(partition by depname order by salary), count(1) over(partition by depname order by salary), sum(1) over(partition by depname order by salary) from vector_window_table_03 order by 1, 2;

select depname, salary, rank() over(partition by depname order by salary, enroll) from vector_window_table_03 order by 1, 2;
select depname, salary, dense_rank() over(partition by depname order by salary, enroll) from vector_window_table_03 order by 1, 2;

select depname, salary, row_number() over(partition by depname order by salary, enroll) from vector_window_table_03 order by 1, 2;


explain (costs off)
select * from vector_window_table_02 t1 where exists (select depname, salary, rank() over(partition by depname), count(1) over(partition by depname), count(depname) over(partition by depname) from vector_window_table_02 t2  where t1.salary=t2.salary order by 1, 2);

select * from vector_window_table_02 t1 where exists (select depname, salary, rank() over(partition by depname), count(1) over(partition by depname), count(depname) over(partition by depname) from vector_window_table_02 t2  where t1.salary=t2.salary order by 1, 2) order by 1,2,3 limit 10;

select * from vector_window_table_03 t1 where exists (select depname, salary, row_number() over(partition by depname order by salary), count(1) over(partition by depname order by salary), sum(1) over(partition by depname order by salary) from vector_window_table_03 t2 where t1.salary=t2.salary order by 1, 2) order by 1,2,3 limit 10;

--test stddev_samp
select depname, salary,  stddev_samp(salary) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;
select depname, salary,  stddev_samp(salary::int2) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;
select depname, salary,  stddev_samp(salary::int8) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;
select depname, salary,  stddev_samp(salary::float4) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;
select depname, salary,  stddev_samp(salary::float8) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;
select depname, salary,  stddev_samp(salary::numeric) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 100;

--test sttdev
select depname, salary,  stddev_samp(salary) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
select depname, salary,  stddev_samp(salary::int2) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
select depname, salary,  stddev_samp(salary::int8) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
select depname, salary,  stddev_samp(salary::float4) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
select depname, salary,  stddev_samp(salary::float8) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
select depname, salary,  stddev_samp(salary::numeric) over(partition by depname order by salary) from vector_window_table_03 order by 1,2 limit 10;
