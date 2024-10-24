create database keep_func_adb with dbcompatibility = 'A';
\c keep_func_adb
CREATE TABLE employees (department_id INT,manager_id INT,last_name varchar(50),hiredate varchar(50),SALARY INT);
INSERT INTO employees VALUES(30, 100, 'Raphaely', '2017-07-01', 1700);
INSERT INTO employees VALUES(30, 100, 'De Haan', '2018-05-01', 11000);
INSERT INTO employees VALUES(40, 100, 'Errazuriz', '2017-07-21', 1400);
INSERT INTO employees VALUES(50, 100, 'Hartstein', '2019-10-05', 14000);
INSERT INTO employees VALUES(50, 100, 'Raphaely', '2017-07-22', 1700);
INSERT INTO employees VALUES(50, 100, 'Weiss', '2019-10-05', 13500);
INSERT INTO employees VALUES(90, 100, 'Russell', '2019-07-11', 13000);
INSERT INTO employees VALUES(90, 100, 'Partners', '2018-12-01', 14000);
explain (verbose on, costs off) SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT department_id, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees GROUP BY department_id ORDER BY 1 DESC;
SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",  
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best" 
               FROM employees ORDER BY department_id, salary, last_name;
explain (verbose on, costs off) SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",  
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best" 
               FROM employees ORDER BY department_id, salary, last_name;
-- test keep for agg and window agg.
SELECT stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
--not first/last
SELECT department_id, max(salary) KEEP (DENSE_RANK ORDER BY HIREDATE) FROM employees GROUP BY department_id;
-- syntax error
-- test multi order by
SELECT stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id ORDER BY salary) FROM employees;
-- test vector executore unsupport
set try_vector_engine_strategy=force;
SELECT stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
set try_vector_engine_strategy=off;
-- test var_pop unsupport
SELECT var_pop(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees;
--test include null and  order by xxx nulls first/last
create table keep_table(id1 int, id2 int, id3 int);
insert into keep_table values (1, 11, 21);
insert into keep_table values (2, 12, 22);
insert into keep_table values (NULL, NULL, 23);
select min(id1) keep (DENSE_RANK FIRST ORDER BY id2 asc nulls first) from keep_table;
select min(id1) keep (DENSE_RANK LAST ORDER BY id2 asc nulls first) from keep_table;

select min(id1) keep (DENSE_RANK FIRST ORDER BY id2 desc nulls first) from keep_table;
select min(id1) keep (DENSE_RANK LAST ORDER BY id2 desc nulls first) from keep_table;

select min(id1) keep (DENSE_RANK FIRST ORDER BY id2 nulls first) from keep_table;
select min(id1) keep (DENSE_RANK FIRST ORDER BY id2 nulls last) from keep_table;

insert into keep_table values (7, NULL, 24);
insert into keep_table values (8, NULL, 24);
select sum(id1) keep (DENSE_RANK FIRST ORDER BY id2 asc nulls first) from keep_table;
select count(id1) keep (DENSE_RANK FIRST ORDER BY id2 asc nulls first) from keep_table;
\c regression;
drop database if exists keep_func_adb;
--only A_FORMAT support keep func
create database keep_func_bdb with dbcompatibility = 'B';
\c keep_func_bdb
CREATE TABLE employees (department_id INT,manager_id INT,last_name varchar(50),hiredate varchar(50),SALARY INT);
INSERT INTO employees VALUES(30, 100, 'Raphaely', '2017-07-01', 1700);
INSERT INTO employees VALUES(30, 100, 'De Haan', '2018-05-01', 11000);
INSERT INTO employees VALUES(40, 100, 'Errazuriz', '2017-07-21', 1400);
INSERT INTO employees VALUES(50, 100, 'Hartstein', '2019-10-05', 14000);
INSERT INTO employees VALUES(50, 100, 'Raphaely', '2017-07-22', 1700);
INSERT INTO employees VALUES(50, 100, 'Weiss', '2019-10-05', 13500);
INSERT INTO employees VALUES(90, 100, 'Russell', '2019-07-11', 13000);
INSERT INTO employees VALUES(90, 100, 'Partners', '2018-12-01', 14000);
explain (verbose on, costs off) SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT department_id, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees GROUP BY department_id ORDER BY 1 DESC;
SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best"
               FROM employees ORDER BY department_id, salary, last_name;
explain (verbose on, costs off) SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best"
               FROM employees ORDER BY department_id, salary, last_name;
-- test keep for agg and window agg.
SELECT stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
\c regression;
drop database if exists keep_func_bdb;
create database keep_func_pgdb with dbcompatibility = 'PG';
\c keep_func_pgdb
CREATE TABLE employees (department_id INT,manager_id INT,last_name varchar(50),hiredate varchar(50),SALARY INT);
INSERT INTO employees VALUES(30, 100, 'Raphaely', '2017-07-01', 1700);
INSERT INTO employees VALUES(30, 100, 'De Haan', '2018-05-01', 11000);
INSERT INTO employees VALUES(40, 100, 'Errazuriz', '2017-07-21', 1400);
INSERT INTO employees VALUES(50, 100, 'Hartstein', '2019-10-05', 14000);
INSERT INTO employees VALUES(50, 100, 'Raphaely', '2017-07-22', 1700);
INSERT INTO employees VALUES(50, 100, 'Weiss', '2019-10-05', 13500);
INSERT INTO employees VALUES(90, 100, 'Russell', '2019-07-11', 13000);
INSERT INTO employees VALUES(90, 100, 'Partners', '2018-12-01', 14000);
explain (verbose on, costs off) SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees;
SELECT department_id, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) "Worst", SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) "Best" FROM employees GROUP BY department_id ORDER BY 1 DESC;
SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best"
               FROM employees ORDER BY department_id, salary, last_name;
explain (verbose on, costs off) SELECT last_name,department_id,salary, SUM(salary) KEEP (DENSE_RANK FIRST ORDER BY salary) OVER (PARTITION BY department_id) "Worst",
               SUM(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) OVER (PARTITION BY department_id) "Best"
               FROM employees ORDER BY department_id, salary, last_name;
-- test keep for agg and window agg.
SELECT stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, stddev(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, variance(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, min(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, max(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
SELECT count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) over(partition by department_id) FROM employees;
SELECT department_id, count(salary) KEEP (DENSE_RANK LAST ORDER BY HIREDATE) FROM employees GROUP BY department_id;
\c regression;
drop database if exists keep_func_pgdb;
