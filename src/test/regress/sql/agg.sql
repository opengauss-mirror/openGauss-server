create schema aggregate;
set current_schema='aggregate';
create table t1 (a int , b int);
insert into t1 values(1,2);

explain (costs off)
select count(*) from (
  select row_number() over(partition by a, b) as rn,
      first_value(a) over(partition by b, a) as fv,
      * from t1
  )
where rn = 1;
set qrw_inlist2join_optmode = 'disable';
explain  (costs off)
select count(*) from (
  select row_number() over(partition by a, b) as rn,
      first_value(a) over(partition by b, a) as fv,
      * from t1
  )
where rn = 1;
reset qrw_inlist2join_optmode;

set enable_hashagg = off;
--force hash agg, if used sort agg will report error.
select a , count(distinct  generate_series(1,2)) from t1 group by a;
explain (verbose, costs off)
select a , count(distinct  generate_series(1,2)) from t1 group by a;
set query_dop = 2;
select a , count(distinct  generate_series(1,2)) from t1 group by a;
reset query_dop;

--test const-false agg
CREATE TABLE bmsql_item (
i_id int4 NoT NULL,i_name varchar(24),i_price numeric(5,2),i_data varchar( 50),i_im_id int4,
coNSTRAINT bmsql_item_pkey PRIMARY KEY (i_id)
);
insert into bmsql_item values ('1','sqltest_varchar_1','0.01','sqltest_varchar_1','1');
insert into bmsql_item values ('2','sqltest_varchar_2','0.02','sqltest_varchar_2','2');
insert into bmsql_item values ('3','sqltest_varchar_3','0.03','sqltest_varchar_3','3');
insert into bmsql_item values ('4','sqltest_varchar_4','0.04','sqltest_varchar_4','4');
insert into bmsql_item values ('5');

CREATE TABLE bmsql_new_order (
no_w_id int4 NOT NULL,
no_d_id int4 NOT NULL,no_o_id int4 NOT NULL
);

insert into bmsql_new_order values('1','1','1');
insert into bmsql_new_order values('2','2','2');
insert into bmsql_new_order values('3','3','3');
insert into bmsql_new_order values('4','4','4');
insert into bmsql_new_order values('5','5','5');

SELECT
   (avg(alias24.alias17)>2) AS alias32
FROM

    bmsql_item,
    (
        SELECT alias12.alias5 AS alias17
		FROM ( SELECT sin(bmsql_new_order.no_o_id)alias5 FROM bmsql_new_order )alias12
	)alias24
	GROUP BY bmsql_item.i_im_id HAVING 1>2
UNION
SELECT TRUE FROM  bmsql_item;

explain (verbose,costs off)
SELECT
   (avg(alias24.alias17)>2) AS alias32
FROM

    bmsql_item,
    (
        SELECT alias12.alias5 AS alias17
		FROM ( SELECT sin(bmsql_new_order.no_o_id)alias5 FROM bmsql_new_order )alias12
	)alias24
	GROUP BY bmsql_item.i_im_id HAVING 1>2
UNION
SELECT TRUE FROM  bmsql_item;

create table test_agg_false(a int, b varchar(20),c text, d numeric(5,2));
explain (verbose ,costs off) select sum(a),sum(b) , d from test_agg_false where 0=1 group by d;
select sum(a),sum(b) , d from test_agg_false where 0=1 group by d;

explain (verbose, costs off) select sum(a)+sum(b) , d from test_agg_false where 0=1 group by d;
select sum(a)+sum(b) , d from test_agg_false where 0=1 group by d;

explain (verbose, costs off) select sin(sum(a)+sum(b)) , d from test_agg_false where 0=1 group by d;
select sin(sum(a)+sum(b)) , d from test_agg_false where 0=1 group by d;

explain (verbose ,costs off) select sum(a)+sum(b) , d , 1 from test_agg_false where 0=1 group by d;
select sum(a)+sum(b) , d ,1 from test_agg_false where 0=1 group by d;

CREATE TABLE test_table (column_x double precision , column_y double precision);
INSERT INTO test_table (column_x, column_y) VALUES (55, 38);
INSERT INTO test_table (column_x, column_y) VALUES (46, 29);
INSERT INTO test_table (column_x, column_y) VALUES (41, 24);
INSERT INTO test_table (column_x, column_y) VALUES (48, 33);
INSERT INTO test_table (column_x, column_y) VALUES (51, 39);
INSERT INTO test_table (column_x, column_y) VALUES (49, 32);

SELECT corr_s(column_x, column_y) FROM test_table;
SELECT corr_s(column_x, column_y, 'COEFFICIENT') FROM test_table;
SELECT corr_s(column_x, column_y, 'ONE_SIDED_SIG') FROM test_table;
SELECT corr_s(column_x, column_y, 'ONE_SIDED_SIG_POS') FROM test_table;
SELECT corr_s(column_x, column_y, 'ONE_SIDED_SIG_NEG') FROM test_table;
SELECT corr_s(column_x, column_y, 'TWO_SIDED_SIG') FROM test_table;

SELECT corr_k(column_x, column_y) FROM test_table;
SELECT corr_k(column_x, column_y, 'COEFFICIENT') FROM test_table;
SELECT corr_k(column_x, column_y, 'ONE_SIDED_SIG') FROM test_table;
SELECT corr_k(column_x, column_y, 'ONE_SIDED_SIG_POS') FROM test_table;
SELECT corr_k(column_x, column_y, 'ONE_SIDED_SIG_NEG') FROM test_table;
SELECT corr_k(column_x, column_y, 'TWO_SIDED_SIG') FROM test_table;

CREATE TABLE null_table1 (column_x double precision, column_y double precision);
INSERT INTO null_table1 (column_x, column_y) VALUES (null, null);
INSERT INTO null_table1 (column_x, column_y) VALUES (null, null);
SELECT corr_s(column_x, column_y, 'COEFFICIENT') FROM null_table1;

CREATE TABLE null_table2 (column_x double precision, column_y double precision);
INSERT INTO null_table2(column_x, column_y) VALUES (null, 38);
INSERT INTO null_table2(column_x, column_y) VALUES (null, 29);
SELECT corr_s(column_x, column_y, 'COEFFICIENT') FROM null_table2;

CREATE TABLE null_table3 (column_x double precision, column_y double precision);
INSERT INTO null_table3(column_x, column_y) VALUES (55, 38);
INSERT INTO null_table3(column_x, column_y) VALUES (null, 29);
INSERT INTO null_table3(column_x, column_y) VALUES (41, 24);
INSERT INTO null_table3(column_x, column_y) VALUES (48, 33);
INSERT INTO null_table3(column_x, column_y) VALUES (51, 39);
INSERT INTO null_table3(column_x, column_y) VALUES (49, 32);
SELECT corr_s(column_x, column_y, 'COEFFICIENT') FROM null_table3;

create table customers1(customer_id number,id number, cust_last_name varchar2(50));
insert into customers1 values(001,1,'张生');
insert into customers1 values(002,2,'刘生');
insert into customers1 values(001,3,'李生');

select corr_k(customer_id,id,'ONE_SIDED_SIG') from customers1;
select corr_k(customer_id,id,'ONE_SIDED_SIG_POS') from customers1;
select corr_k(customer_id,id,'ONE_SIDED_SIG_NEG') from customers1;
select corr_k(customer_id,id,'TWO_SIDED_SIG') from customers1;

CREATE TABLE EMP
(EMPNO NUMBER(4) NOT NULL,
ENAME VARCHAR2(10),
JOB VARCHAR2(9),
MGR NUMBER(4),
HIREDATE DATE,
SAL NUMBER(7, 2),
COMM NUMBER(7, 2),
DEPTNO NUMBER(2));

INSERT INTO EMP VALUES
(7369, 'SMITH', 'CLERK', 7902,
TO_DATE('17-DEC-1980', 'DD-MON-YYYY'), 800, NULL, 20);
INSERT INTO EMP VALUES
(7499, 'ALLEN', 'SALESMAN', 7698,
TO_DATE('20-FEB-1981', 'DD-MON-YYYY'), 1600, 300, 30);
INSERT INTO EMP VALUES
(7521, 'WARD', 'SALESMAN', 7698,
TO_DATE('22-FEB-1981', 'DD-MON-YYYY'), 1250, 500, 30);
INSERT INTO EMP VALUES
(7566, 'JONES', 'MANAGER', 7839,
TO_DATE('2-APR-1981', 'DD-MON-YYYY'), 2975, NULL, 20);
INSERT INTO EMP VALUES
(7654, 'MARTIN', 'SALESMAN', 7698,
TO_DATE('28-SEP-1981', 'DD-MON-YYYY'), 1250, 1400, 30);
INSERT INTO EMP VALUES
(7698, 'BLAKE', 'MANAGER', 7839,
TO_DATE('1-MAY-1981', 'DD-MON-YYYY'), 2850, NULL, 30);
INSERT INTO EMP VALUES
(7782, 'CLARK', 'MANAGER', 7839,
TO_DATE('9-JUN-1981', 'DD-MON-YYYY'), 2450, NULL, 10);
INSERT INTO EMP VALUES
(7788, 'SCOTT', 'ANALYST', 7566,
TO_DATE('09-DEC-1982', 'DD-MON-YYYY'), 3000, NULL, 20);
INSERT INTO EMP VALUES
(7839, 'KING', 'PRESIDENT', NULL,
TO_DATE('17-NOV-1981', 'DD-MON-YYYY'), 5000, NULL, 10);
INSERT INTO EMP VALUES
(7844, 'TURNER', 'SALESMAN', 7698,
TO_DATE('8-SEP-1981', 'DD-MON-YYYY'), 1500, 0, 30);
INSERT INTO EMP VALUES
(7876, 'ADAMS', 'CLERK', 7788,
TO_DATE('12-JAN-1983', 'DD-MON-YYYY'), 1100, NULL, 20);
INSERT INTO EMP VALUES
(7900, 'JAMES', 'CLERK', 7698,
TO_DATE('3-DEC-1981', 'DD-MON-YYYY'), 950, NULL, 30);
INSERT INTO EMP VALUES
(7902, 'FORD', 'ANALYST', 7566,
TO_DATE('3-DEC-1981', 'DD-MON-YYYY'), 3000, NULL, 20);
INSERT INTO EMP VALUES
(7934, 'MILLER', 'CLERK', 7782,
TO_DATE('23-JAN-1982', 'DD-MON-YYYY'), 1300, NULL, 10);

SELECT CORR_K(sal, comm, 'COEFFICIENT') coefficient,
CORR_K(sal, comm, 'TWO_SIDED_SIG') two_sided_p_value
FROM EMP;

CREATE TABLE test_data (
id NUMBER PRIMARY KEY,
value1 DOUBLE PRECISION,
value2 DOUBLE PRECISION
);
INSERT INTO test_data (id, value1, value2) VALUES (1, 10, 20);
INSERT INTO test_data (id, value1, value2) VALUES (2, 20, 40);
INSERT INTO test_data (id, value1, value2) VALUES (3, 30, 60);
INSERT INTO test_data (id, value1, value2) VALUES (4, 40, 80);

SELECT CORR_K(value1, value2) FROM test_data;
SELECT CORR_S(value1, value2) FROM test_data;

CREATE TABLE test_data1 (
value1 DOUBLE PRECISION,
value2 DOUBLE PRECISION,
value3 DOUBLE PRECISION,
value4 DOUBLE PRECISION,
value5 DOUBLE PRECISION,
value6 DOUBLE PRECISION
);

INSERT INTO test_data1 (value1, value2, value3, value4, value5, value6) VALUES (1.0, 2.0,'', 4.0, NULL, NULL);
INSERT INTO test_data1 (value1, value2, value3, value4, value5, value6) VALUES (5.0, 8.0, 5.0, NULL, NULL, NULL);
INSERT INTO test_data1 (value1, value2, value3, value4, value5, value6) VALUES (NULL, 6.0, 4.0, '', NULL, NULL);
INSERT INTO test_data1 (value1, value2, value3, value4, value5, value6) VALUES (2.0, NULL, 1.0, 3.0, NULL, NULL);
INSERT INTO test_data1 (value1, value2, value3, value4, value5, value6) VALUES (13.0, 4.0, 4.0, '', NULL, NULL);

SELECT corr_s(value1, value2) AS corr_s_value1_value2,
corr_s(value1, value3) AS corr_s_value1_value3,
corr_s(value1, value4) AS corr_s_value1_value4,
corr_s(value2, value3) AS corr_s_value2_value3,
corr_s(value2, value4) AS corr_s_value2_value4,
corr_s(value3, value4) AS corr_s_value3_value4,
corr_s(value4, value6) AS corr_s_value4_value6,
corr_s(value5, value6) AS corr_s_value5_value6
FROM test_data1;

SELECT corr_k(value1, value2) AS corr_k_value1_value2,
corr_k(value1, value3) AS corr_k_value1_value3,
corr_k(value1, value4) AS corr_k_value1_value4,
corr_k(value2, value3) AS corr_k_value2_value3,
corr_k(value2, value4) AS corr_k_value2_value4,
corr_k(value2, value4) AS corr_k_value2_value4,
corr_k(value4, value6) AS corr_k_value4_value6,
corr_k(value5, value6) AS corr_k_value5_value6
FROM test_data1;

CREATE TABLE test_data2 (
id NUMBER PRIMARY KEY,
value1 DOUBLE PRECISION,
value2 DOUBLE PRECISION
);
INSERT INTO test_data2 (id, value1, value2) VALUES (1, 10, 20);
INSERT INTO test_data2 (id, value1, value2) VALUES (2, 20, 40);
INSERT INTO test_data2 (id, value1, value2) VALUES (3, 30, 60);

SELECT CORR_K(value1, value2) AS K1,
CORR_K(value1, value2,'COEFFICIENT') AS K2,
CORR_K(value1, value2,'ONE_SIDED_SIG') AS K3,
CORR_K(value1, value2,'ONE_SIDED_SIG_POS') AS K4,
CORR_K(value1, value2,'ONE_SIDED_SIG_NEG') AS K5,
CORR_K(value1, value2,'TWO_SIDED_SIG') AS K6 FROM test_data2;

SELECT CORR_S(value1, value2) AS S1,
CORR_S(value1, value2,'COEFFICIENT') AS S2,
CORR_S(value1, value2,'ONE_SIDED_SIG') AS S3,
CORR_S(value1, value2,'ONE_SIDED_SIG_POS') AS S4,
CORR_S(value1, value2,'ONE_SIDED_SIG_NEG') AS S5,
CORR_S(value1, value2,'TWO_SIDED_SIG') AS S6 FROM test_data2;

drop table test_table;
drop table null_table1;
drop table null_table2;
drop table null_table3;
drop table customers1;
drop table EMP;
drop table test_data;
drop table test_data1;
drop table test_data2;
drop table t1;
drop schema aggregate CASCADE;