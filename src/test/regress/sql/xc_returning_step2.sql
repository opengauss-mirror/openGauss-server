--
--FOR BLACKLIST FEATURE: INHERITS、SEQUENCE、RULE is not supported.
--

set client_min_messages=warning;
-- Lets not reuse int4_tbl & int8_tbl
-- so that this test could be run independently

create table xc_int8_tbl(q1 int8, q2 int8);
INSERT INTO xc_int8_tbl VALUES('  123   ','  456');
INSERT INTO xc_int8_tbl VALUES('123   ','4567890123456789');
INSERT INTO xc_int8_tbl VALUES('4567890123456789','123');
INSERT INTO xc_int8_tbl VALUES(+4567890123456789,'4567890123456789');
INSERT INTO xc_int8_tbl VALUES('+4567890123456789','-4567890123456789');

create table xc_int4_tbl(f1 int4);
INSERT INTO xc_int4_tbl(f1) VALUES ('   0  ');
INSERT INTO xc_int4_tbl(f1) VALUES ('123456     ');
INSERT INTO xc_int4_tbl(f1) VALUES ('    -123456');
INSERT INTO xc_int4_tbl(f1) VALUES ('2147483647');
INSERT INTO xc_int4_tbl(f1) VALUES ('-2147483647');

-- The tables have to be created on a well defined set of nodes
-- independent of the nodes available in the cluster
-- so that ctid returning tests produce predictable tests

CREATE TABLE rep_foo(a int, b int);
ALTER TABLE rep_foo ADD PRIMARY KEY(a, b);



CREATE TABLE tp (f1 serial, f2 text, f3 int default 42);
CREATE TABLE tc (fc int);



CREATE TABLE ta1 (v1 int, v2 int);
CREATE TABLE ta2 (v1 int, v2 int);

CREATE TABLE sal_emp (name text, pay_by_quarter integer[], schedule text[][]);

CREATE TABLE products(product_id serial PRIMARY KEY ,product_name varchar(150),price numeric(10,2) );

CREATE TABLE my_tab(f1 int, f2 text, f3 int);
CREATE TABLE my_tab2(f1 int, f2 text, f3 int);

create or replace function fn_immutable(integer) RETURNS integer
    AS 'SELECT f3+$1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
create or replace function fn_volatile(integer) RETURNS integer
    AS 'SELECT f3+$1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    VOLATILE
    RETURNS NULL ON NULL INPUT;
create or replace function fn_stable(integer) RETURNS integer
    AS 'SELECT $1 from my_tab2 where f1=1;'
    LANGUAGE SQL
    STABLE
    RETURNS NULL ON NULL INPUT;

CREATE TABLE numbers(a int, b varchar(255), c int);

CREATE TABLE test_tab(a int, b varchar(255), c varchar(255), d int);

------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------
-- INSERT Returning
------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- insert returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2)
returning b, a, b, b, b+a, b-a, ctid;

with t as 
(
  insert into rep_foo values(3,4), (5,6), (7,8) 
  returning b, a, b, b, b+a, b-a, ctid
) select * from t order by 1, 2;

truncate table rep_foo;





-------------------------------------------------------------
-- insert returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

with t as
(
INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9) returning ctid, f3, f2, f1, f3-f1
)
select * from t order by 4,3,2;

INSERT INTO tc VALUES(123,'child',999,-123)
returning ctid, f3, f2, f1, f3-f1, fc;

truncate table tc;
truncate table tp;

-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

with t as
(
insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33) returning b,c,a, 234, c-2, a+3, ctid
)
select * from t order by 3,2,1;

insert into numbers values(4,'four',44)
returning b,c,a,a+1,22,upper(b),c-a, ctid;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']])
returning pay_by_quarter[3], schedule[1,2][1,1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']])
returning pay_by_quarter[3], schedule[1,2][1,1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

with t as
(
INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00)
returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab2 values(1,'One',11) returning *;
insert into my_tab2 values(2,'Two',22) returning *;
insert into my_tab2 values(3,'Three',33) returning *;

insert into my_tab values(1,'One',11) returning f3,f2,f1,f3-f1,fn_immutable(f3);
insert into my_tab values(2,'Two',22) returning f3,f2,f1,f3-f1, fn_volatile(f3);
insert into my_tab values(3,'Three',33) returning f3,f2,f1,f3-f1, fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1) returning (a::bool) OR (c::bool);

insert into numbers values(0,'zero',1) returning (a::bool) AND (c::bool);

insert into numbers values(0,'zero',1) returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

with t as
(
insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33) returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

with t as
(
insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2)
 returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;

--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------
-- UPDATE Returning
--------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- update returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2),(3,4), (5,6);

update rep_foo set b=b+1 where b = 2 returning b, a, b, b, b+a, b-a, ctid;

with t as
(
update rep_foo set b=b+1 returning b, a, b, b, b+a, b-a, ctid
)
select * from t order by 1,2;

truncate table rep_foo;








-------------------------------------------------------------
-- returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

INSERT INTO tc VALUES(123,'child',999,-123);

update tc set fc = fc + 23 from xc_int8_tbl i  WHERE tc.f1 = i.q1 returning *;

update tp set f3 = f3 + 23 from xc_int8_tbl i  WHERE tp.f1 = i.q1 returning *;

truncate table tc;
truncate table tp;



-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

insert into numbers values(4,'four',44);
update numbers set c=a where a=4 returning b,c,a,a+1,22,upper(b),c-a, ctid;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']]);
INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]);

update sal_emp set pay_by_quarter[3] = pay_by_quarter[2]  WHERE pay_by_quarter[1] <> pay_by_quarter[2] 
returning pay_by_quarter[3], schedule[1,2][1,1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00);

with t as
(
update products set price = price + 1.0 WHERE product_id = ANY('{1,4,5}'::int[]) returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab values(1,'One',11);
insert into my_tab values(2,'Two',22);
insert into my_tab values(3,'Three',33);

insert into my_tab2 values(1,'One',11);
insert into my_tab2 values(2,'Two',22);
insert into my_tab2 values(3,'Three',33);

update my_tab set f3=2*f3 where f1=1 returning f3,f2,f1,fn_immutable(f3);

update my_tab set f3=2*f3 where f1=2 returning f3,f2,f1,fn_volatile(f3);

update my_tab set f3=2*f3 where f1=3 returning f3,f2,f1,fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1);
update numbers set c=c-a where a=0 returning (a::bool) OR (c::bool);

update numbers set c=c-a where a=0 returning (a::bool) AND (c::bool);

update numbers set c=c-a where a=0 returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);
with t as
(
update numbers set c=c-a returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2);
with t as
(
update test_tab set d=d-a returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;



-------------------------------------------------------------
-- Another case of returning from both the tables in case of an update using from clause
-- This is a very important test case, it uncovered a bug in remote executor
-- where eof_underlying was not being set to false despite HandleDataRow getting
-- a data row from the datanode and copying it in RemoteQueryState
-------------------------------------------------------------

insert into ta1 values(1,2),(2,3),(3,4);

insert into ta2 values(1,2),(2,3),(3,4);

with t as
(
update ta1 t1 set v2=t1.v2+10 from ta1 t2 where t2.v2<3 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2, t2.v1 t2_v1,t2.v2 t2_v2,t1.v1*t2.v2
)
select * from t order by 2,3;

select * from ta1 order by v1;

with t as
(
update ta1 t1 set v2=t1.v2+10 from ta2 t2 where t2.v2<=13 returning t1.ctid,t1.v1 t1_v1, t1.v2 t1_v2
)
select * from t order by 2,3;

select * from ta1 order by v1;

truncate table ta1;
truncate table ta2;

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
-- DELETE Returning
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

-------------------------------------------------------------
-- delete returning from a replicated table
-------------------------------------------------------------

insert into rep_foo values(1,2),(3,4), (5,6);

delete from rep_foo where a = 1 returning b, a, b, b, b+a, b-a;

with t as
(
delete from rep_foo returning b, a, b, b, b+a, b-a
)
select * from t order by 1,2;

truncate table rep_foo;










-------------------------------------------------------------
-- delete returning in case parent and child reside on different Datanodes
-------------------------------------------------------------

INSERT INTO tp (f2,f3) VALUES ('test', DEFAULT), ('More', 11), (upper('more'), 7+9);

INSERT INTO tc VALUES(123,'child',999,-123);

DELETE FROM tc  USING xc_int8_tbl i  WHERE tc.f1 = i.q1 returning *;

truncate table tc;
truncate table tp;



-------------------------------------------------------------
-- scalars in returning
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

insert into numbers values(4,'four',44);
delete from numbers where a=4 returning b,c,a,a+1,22,upper(b),c-a;

truncate table numbers;

-------------------------------------------------------------
-- Array notation in returning
-------------------------------------------------------------

INSERT INTO sal_emp VALUES ('Bill', ARRAY[10000, 10000, 10000, 10000], ARRAY[['meeting', 'lunch'], ['training', 'presentation']]);
INSERT INTO sal_emp VALUES ('Carol', ARRAY[20000, 25000, 25000, 25000], ARRAY[['breakfast', 'consulting'], ['meeting', 'lunch']]);

delete from sal_emp WHERE pay_by_quarter[1] <> pay_by_quarter[2] 
returning pay_by_quarter[3], schedule[1,2][1,1], array_lower(schedule, 1), array_length(schedule, 1), array_dims(schedule);

truncate table sal_emp;

-------------------------------------------------------------
-- ANY in returning
-------------------------------------------------------------

INSERT INTO products(product_name, price) VALUES  ('apple', 0.5) ,('cherry apple', 1.25) ,('avocado', 1.5),('octopus',20.50) ,('watermelon',2.00);

with t as
(
DELETE FROM products WHERE product_id = ANY('{1,4,5}'::int[]) returning product_id, product_id = ANY('{1,4,5}'::int[])
)
select * from t order by 1;

truncate table products;

alter sequence products_product_id_seq restart with 1;

-------------------------------------------------------------
-- functions in returning
-------------------------------------------------------------

insert into my_tab values(1,'One',11);
insert into my_tab values(2,'Two',22);
insert into my_tab values(3,'Three',33);

insert into my_tab2 values(1,'One',11);
insert into my_tab2 values(2,'Two',22);
insert into my_tab2 values(3,'Three',33);

delete from my_tab where f1=1 returning f3,f2,f1,fn_immutable(f3);

delete from my_tab where f1=2 returning f3,f2,f1,fn_volatile(f3);

delete from my_tab where f1=3 returning f3,f2,f1,fn_stable(f3);

truncate table my_tab;
truncate table my_tab2;

-------------------------------------------------------------
-- boolean operator in returning
-------------------------------------------------------------

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning (a::bool) OR (c::bool);

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning (a::bool) AND (c::bool);

insert into numbers values(0,'zero',1);
delete from numbers where a=0 returning NOT(a::bool);

truncate table numbers;

-------------------------------------------------------------
-- use of case in returning clause 
-------------------------------------------------------------

insert into numbers values(1,'one',11),(2,'Two',22),(3,'Three',33);

with t as
(
delete from numbers returning case when a=1 then 'First' when a=2 then 'Second' when a=3 then 'Third' else 'nth' end
)
select * from t order by 1;

truncate table numbers;

-------------------------------------------------------------
-- use of conditional expressions in returning clause 
-------------------------------------------------------------

insert into test_tab values(0,'zero', NULL, 1), (1,NULL,NULL,23),(2,'Two','Second',NULL),(3,'Three','Third', 2);

with t as
(
delete from test_tab returning *, COALESCE(c,b), NULLIF(b,c), greatest(a,d), least(a,d), b IS NULL, C IS NOT NULL
)
select * from t order by 1,2,3,4;

truncate table test_tab;


-------------------------------------------------------------
-- A few tests to verify # of tuples processed.
-------------------------------------------------------------
\set QUIET false


-------------------------------------------------------------
-- Number of rows processed for query having RETURNING.
-- The UPDATE runs twice, but for the second row it does not actually update
-- the row and hence returns NULL. The UPDATE tag should show UPDATE 1,
-- as against UPDATE 2.
-------------------------------------------------------------

CREATE TABLE xcreturn_foo (c1 int, c2 int);
INSERT INTO xcreturn_foo VALUES (1,2), (3,4);
CREATE TABLE xcreturn_bar(c3 int, c4 int);
INSERT INTO xcreturn_bar VALUES(123,456);
INSERT INTO xcreturn_bar VALUES(123,789);
update xcreturn_foo set c2=c2*2 from xcreturn_bar b WHERE xcreturn_foo.c1+122 = b.c3 RETURNING *;

drop table xcreturn_foo, xcreturn_bar;

-------------------------------------------------------------
-- Number of rows processed query having WITH and RETURNING.
-- Following WITH query updates runs so should show UPDATE 2.
-------------------------------------------------------------

create table xcreturn_tab1 (id int, v varchar);
ALTER TABLE xcreturn_tab1 ADD PRIMARY KEY(id);
create table xcreturn_tab2 (id int, v varchar);
ALTER TABLE xcreturn_tab2 ADD PRIMARY KEY(id);
insert into xcreturn_tab1 values (1, 'firstrow'), (2, 'secondrow');
WITH wcte AS ( INSERT INTO xcreturn_tab2 VALUES (999, 'opop'), (333, 'sss') , ( 42, 'new' ), (55, 'ppp') RETURNING id AS newid )
UPDATE xcreturn_tab1 SET id = id + newid FROM wcte;

drop table xcreturn_tab1, xcreturn_tab2;

CREATE TABLE RETURN_REL_TEST(A INT PRIMARY KEY, B INT);
INSERT INTO RETURN_REL_TEST VALUES(1, 1) RETURNING *;
UPDATE RETURN_REL_TEST SET A = 2 RETURNING *;
DELETE FROM RETURN_REL_TEST RETURNING *;
DROP TABLE RETURN_REL_TEST;

\set QUIET true


-------------------------------------------------------------
-- clean up
-------------------------------------------------------------

drop table test_tab;

drop table numbers;

drop function fn_immutable(integer);
drop function fn_volatile(integer);
drop function fn_stable(integer);

drop table my_tab;
drop table my_tab2;

drop table products;

drop table sal_emp;

drop table ta1;
drop table ta2;



drop table tc;
drop table tp;


drop table rep_foo;



reset client_min_messages;
