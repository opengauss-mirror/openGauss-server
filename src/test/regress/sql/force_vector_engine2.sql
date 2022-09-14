create schema test_force_vector2;
set current_schema=test_force_vector2;
create table force_vector_test(id int, val1 int, val2 numeric(10,5));
insert into force_vector_test values(generate_series(1, 10000), generate_series(1, 1000), generate_series(1, 2000));
analyze force_vector_test;
-- partition table
create table force_vector_partition(id int, val1 int, val2 text)
partition by range(id) (
  partition force_vector_p1 values less than (2001),
  partition force_vector_p2 values less than (4001),
  partition force_vector_p3 values less than (6001),
  partition force_vector_p4 values less than (8001),
  partition force_vector_p5 values less than (MAXVALUE)
);
insert into force_vector_partition values(generate_series(1, 10000), generate_series(1, 2000), generate_series(1, 5000));
analyze force_vector_partition;

explain (analyze on, timing off) select /*+ set(try_vector_engine_strategy force) */ id, val1*2, val2+val1 as val3 from force_vector_test where id < 5000 and val1 < 500 order by id limit 10;
explain (analyze on, timing off) select /*+ set(try_vector_engine_strategy force) */ id, avg(val1), sum(val2) from force_vector_partition group by id order by id limit 10;

create table t1(id int, val1 name, val2 macaddr, val3 uuid, val4 unknown);
insert into t1 values(1, 'abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd123', '08:01:04:03:05:01', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'abcd'),(2, 'abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd124', '08:01:04:03:05:02', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A12', 'abcc');

set try_vector_engine_strategy=force;
select val1, count(id) from t1 group by val1;
select val2, count(id) from t1 group by val2;
select val3, count(id) from t1 group by val3;
select val4, count(id) from t1 group by val4;

create table force_tb1(c1 int,c2 int);
insert into force_tb1 values(1,1);
insert into force_tb1 values(2,2);
create incremental materialized view v_force as select * from force_tb1;
select * from v_force order by 1;

CREATE TABLE force_vector_dept(deptNO INT PRIMARY KEY,DNAME VARCHAR(14),LOC VARCHAR(13));
INSERT INTO force_vector_dept VALUES (20,'RESEARCH','DALLAS');

CREATE TABLE force_vector_emp(EMPNO INT PRIMARY KEY,ENAME VARCHAR(10),JOB VARCHAR(9),MGR numeric,HIREDATE DATE,SAL numeric,COMM numeric,deptNO INT, FOREIGN KEY(deptNO) REFERENCES force_vector_dept(deptNO));
INSERT INTO force_vector_emp VALUES(7369,'SMITH','CLERK',7902,'1980-12-17',800,NULL,20);

explain plan for select e.empno,e.ename,e.sal,d.dname from force_vector_emp e inner join force_vector_dept d on d.deptNO= e.deptNO;
select id,operation,options,object_name,object_type,projection from plan_table order by 1;

set try_vector_engine_strategy=off;

drop table force_vector_emp;
drop table force_vector_dept;
drop table force_vector_test;
drop schema test_force_vector2 cascade;
