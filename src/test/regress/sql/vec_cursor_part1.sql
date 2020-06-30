/*
 * This file is used to test the function of Vector Cursor
 */
----
--- Create Table and Insert Data
----
create schema vector_cursor_engine_part1;
set current_schema=vector_cursor_engine_part1;

create table vector_cursor_engine_part1.VECTOR_CURSOR_TABLE_01
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

create table vector_cursor_engine_part1.VECTOR_CURSOR_TABLE_02
(
	a	int
)with (orientation=column);

COPY VECTOR_CURSOR_TABLE_02(a) FROM stdin; 
0
5
3
\.

create table vector_cursor_engine_part1.VECTOR_CURSOR_TABLE_03
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

create table vector_cursor_engine_part1.VECTOR_CURSOR_TABLE_04
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

----
--- Clean Resources and Tables
----
drop schema vector_cursor_engine_part1 cascade;
