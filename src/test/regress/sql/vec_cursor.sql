/*
 * This file is used to test the function of Vector Cursor
 */
----
--- Create Table and Insert Data
----
create schema vector_cursor_engine;
set current_schema=vector_cursor_engine;

create table vector_cursor_engine.VECTOR_CURSOR_TABLE_01
(
	a	int
)with (orientation=column);

COPY VECTOR_CURSOR_TABLE_01(a) FROM stdin; 
8
15
0
9
11
5
3
22
\.

create table vector_cursor_engine.VECTOR_CURSOR_TABLE_02
(
	a	int
)with (orientation=column);

COPY VECTOR_CURSOR_TABLE_02(a) FROM stdin; 
0
5
3
\.

create table vector_cursor_engine.VECTOR_CURSOR_TABLE_03
(
	a	int
)with (orientation=column);

COPY VECTOR_CURSOR_TABLE_03(a) FROM stdin; 
1
2
3
4
5
\.

create table vector_cursor_engine.VECTOR_CURSOR_TABLE_04
(
	a	int
)with (orientation=column);

insert into VECTOR_CURSOR_TABLE_04 select * from VECTOR_CURSOR_TABLE_03;

analyze vector_cursor_table_01;
analyze vector_cursor_table_02;
analyze vector_cursor_table_03;
analyze vector_cursor_table_04;

----
--- test 1: rescan of Unique
----
set enable_material=off;
start transaction;
cursor cur1 with hold for select a from vector_cursor_table_01 union select a from vector_cursor_table_02 order by a;
fetch from cur1;
end;

fetch all from cur1;
fetch backward all from cur1;

close cur1;

start transaction;
cursor cur1 for select a from vector_cursor_table_01 order by a;
fetch 1 from cur1;
fetch all from cur1;
end;

set enable_material=on;
set enable_hashagg=on;

----
--- test 2: forward scan
----
start transaction;
cursor cur2 for select * from vector_cursor_table_01,vector_cursor_table_02,vector_cursor_table_03, vector_cursor_table_04 where vector_cursor_table_01.a=vector_cursor_table_02.a and vector_cursor_table_03.a = vector_cursor_table_04.a  order by 1, 2, 3, 4;
fetch forward 6 from cur2;
close cur2;
end;

----
--- test 3: scroll
----
start transaction;
cursor foo scroll for select * from vector_cursor_table_03  order by a offset 1 limit 3;
end;


---
---

 CREATE TABLE APPEND_BATCH_5000_036_1(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(19,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE
, PARTIAL CLUSTER KEY(C_INT))WITH (ORIENTATION=COLUMN) DISTRIBUTE BY replication;
 CREATE OR REPLACE PROCEDURE APPEND_BATCH_5000_036_1()
AS
BEGIN
       FOR I IN 1..202 LOOP
         INSERT INTO APPEND_BATCH_5000_036_1  VALUES('Z', 'Uext_'||i,'ORCHAR_'||i,'d', 'extDA_'||i,'ARC_'||i,i,10000+i,i,1.012+i,2.01+i,3.01+i,'2010-10-10','2018-01-01 15:59:59');
       END LOOP;
END;
/
CALL APPEND_BATCH_5000_036_1();

START TRANSACTION;
CURSOR APPEND_CUR_036 FOR SELECT * FROM APPEND_BATCH_5000_036_1 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14 ;
fetch 100 from APPEND_CUR_036;
CLOSE APPEND_CUR_036;
END;

explain (verbose on, costs off)   select 'datanode1'::name,c_char_1 , c_char_2 from APPEND_BATCH_5000_036_1 group by 1,2,3 order by 1,2,3;
--test expr
select 'datanode1'::name,c_char_1 , c_char_2 from APPEND_BATCH_5000_036_1 group by 1,2,3 order by 1,2,3 limit 20;
---test group by
select nameout( name(c_char_2::text)), c_char_1 , c_char_2 from  APPEND_BATCH_5000_036_1 group by  2,3 order by 2,3 limit 5;
---test hashtable
select  name(t1.c_char_2::text) from APPEND_BATCH_5000_036_1 t1 intersect select name(t2.c_char_2::text) from APPEND_BATCH_5000_036_1 t2  order by 1 limit 5;
---test mergejoin
set enable_hashjoin=off;
set enable_nestloop=off;
select count(*) from APPEND_BATCH_5000_036_1 t1 join APPEND_BATCH_5000_036_1 t2 on name(t1.c_char_2::text) = name(t2.c_char_2::text);
reset enable_hashjoin;
reset enable_nestloop;
--test windowagg
select name(c_char_2::text) var1, name(c_char_1::text) var2 , rank() over(partition by name(c_char_2::text)  order by   name(c_char_1::text) )  from APPEND_BATCH_5000_036_1 order by 1,2 limit 5;

--with hold cursors
create table t_subplan1(a1 int, b1 int, c1 int, d1 int) with (orientation = column) distribute by hash(a1, b1);
create table t_subplan2(a2 int, b2 int, c2 int, d2 int) with (orientation = column) distribute by hash(a2, b2);
insert into t_subplan1 select generate_series(1, 100)%98, generate_series(1, 100)%20, generate_series(1, 100)%13, generate_series(1, 100)%6;
insert into t_subplan2 select generate_series(1, 50)%48, generate_series(1, 50)%28, generate_series(1, 50)%12, generate_series(1, 50)%9;

begin;
cursor foo with hold for select  a1, count(*) cnt
from t_subplan1
where
		c1 >
             (select (avg (d1))
              from t_subplan1 t1
              )
group by a1
order by a1
limit 10;
fetch from foo;
end;
fetch all from foo;
close foo;

begin;
cursor foo with hold for select a1, rank() over(partition by a1)  = some (select a2  from t_subplan2) 
		from t_subplan1 order by 1,2 limit 5;
fetch from foo;
end;
fetch all from foo;
close foo;

begin;
cursor foo with hold for select * from t_subplan1 order by 1, 2;
fetch from foo;
fetch from foo;
fetch absolute 5 from foo;
end;
fetch absolute 99 from foo;
fetch from foo;
close foo;


----
--- Clean Resources and Tables
----
drop schema vector_cursor_engine cascade;
