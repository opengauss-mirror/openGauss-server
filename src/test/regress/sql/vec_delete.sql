/*
 * This file is used to test the function of ExecVecUnique()
 */
----
--- Create Table and Insert Data
----
create schema vector_delete_engine;
set current_schema = vector_delete_engine;

create table vector_delete_engine.ROW_DELETE_TABLE_01
(
	col_int0	int4
   ,col_int		int
   ,col_bint	bigint
   ,col_serial	int
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
)distribute by hash(col_int);

create table vector_delete_engine.VECTOR_DELETE_TABLE_01
(
	col_int0	int4
   ,col_int		int
   ,col_bint	bigint
   ,col_serial	int	
   ,col_char	char(25)
   ,col_vchar	varchar(35)
   ,col_text	text
   ,col_num		numeric(10,4)
   ,col_decimal	decimal
   ,col_float	float
   ,col_date	date
   ,col_time	time
   ,col_timetz	timetz
   ,col_interval	interval
)with(orientation=column) distribute by hash(col_int);

insert into ROW_DELETE_TABLE_01 values(1, 10, 100, 2147483647, 'aa', 'aaaaaa', 'lkjhggdh', 0.01, 10.01, 1100.01, '2015-02-14', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(11, 20, 100, -2147483647, 'aa', 'gggggg', 'ljghjagh', 0.01, 10.01, 1100.01, '2015-03-14', '16:02:38', '1996-2-8 01:00:30+8', '2 day 13:56:56');
insert into ROW_DELETE_TABLE_01 values(1, 30, 100, -2146483647, 'bb', 'aaaaaa', 'jgdajhgdj', 0.01, 10.01, 100.01, '2015-04-15', '16:02:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(11, 40, 100, 2147483647, 'bb', 'aaaaaa', 'ahgdjheien', 0.04, 10.01, 100.01, '2015-05-16', '16:00:38', '1996-2-8 01:00:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(1, 50, 200, 2119483647, 'cc', 'aaaaaa', 'dgadgdgh', 0.01, 10.11, 100.01, '2015-02-18', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:56:56');
insert into ROW_DELETE_TABLE_01 values(11, 10, 100, 2105788455, 'dd', 'hhhhhh', 'dkjjgkdjao', 0.01, 10.01, 100.01, '2015-08-09', '16:00:38', '1996-2-8 01:00:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(1, 20, 100, 1158745898, 'dd', 'aaaaaa', 'dakjgdkjae', 0.01, 10.01, 100.01, '2015-10-06', '16:05:38', '1996-2-6 01:00:30+8', '8 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(11, 30, 100, 1198754521, 'ee', 'ffffff', 'ajgeaaghgd', 0.08, 10.01, 1100.01, '2015-12-02', '16:05:38', '1996-2-6 01:30:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(1, 40, 200, -1246521526, 'ee', 'aaaaaa', 'ajkgeajkjkdj', 0.08, 10.01, 1100.01, '2015-06-16', '16:00:38', '1996-2-8 01:00:30+8', '10 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(11, 50, 100, 2024856154, 'gg', 'ffffff', 'akjgiejj', 0.01, 10.11, 100.01, '2015-05-20', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');

insert into vector_delete_table_01 select * from row_delete_table_01;

create table vector_delete_engine.VECTOR_DELETE_TABLE_02
(
   col_int	int
  ,col_date	date
)with(orientation=column) distribute by hash(col_int);

create table vector_delete_engine.VECTOR_DELETE_TABLE_03
(
   col_int	int
  ,col_date	date
)with(orientation=column) distribute by hash(col_int);

COPY VECTOR_DELETE_TABLE_02(col_int, col_date) FROM stdin;
1	2015-02-26
\.

COPY VECTOR_DELETE_TABLE_03(col_int, col_date) FROM stdin;
2	2015-02-26
1	2015-02-26
2	2015-01-26
\.

analyze vector_delete_table_01;
analyze vector_delete_table_02;
analyze vector_delete_table_03;

----
--- case 1: Bascic Test
----
select * from vector_delete_table_01 order by 1, 2, 3;
select * from vector_delete_table_01 where col_num = 0.01 order by 1,2,3,4,5;
explain (verbose on, costs off) delete from vector_delete_table_01 where col_num = 0.01;
delete from vector_delete_table_01 where col_num = 0.01;
select count(*) from vector_delete_table_01;
delete from vector_delete_table_01 where col_timetz = '1996-2-6 01:00:30+8';
select * from vector_delete_table_01 where col_timetz = '1996-2-6 01:00:30+8' order by 1,2,3,4,5;
delete from vector_delete_table_01 where col_vchar = 'ffffff' and col_time = '16:05:38';
select * from vector_delete_table_01 where col_vchar = 'ffffff' and col_time = '16:05:38' order by 1,2,3,4,5;

----
--- case 2: With Null
----
delete from vector_delete_table_01;
insert into ROW_DELETE_TABLE_01 values(1, NULL, 200, -1246521526, NULL, 'aaaaaa', 'ajkgeajkjkdj', 0.08, 10.01, 1100.01, '2015-06-16', '16:00:38', '1996-2-8 01:00:30+8', NULL);
insert into ROW_DELETE_TABLE_01 values(11, 50, -1246521526,100, 'gg', 'ffffff', 'akjgiejj',NULL, 10.11, 100.01, '2015-05-20', '16:00:38', NULL, '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(1, 20, 100, 1158745898, 'dd', 'aaaaaa', NULL, 0.01, 10.01, 100.01, NULL, '16:05:38', '1996-2-6 01:00:30+8', '8 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(NULL, 30, 100, 1198754521, 'ee', 'ffffff', 'ajgeaaghgd', 0.08, NULL, 1100.01, '2015-12-02', '16:05:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');
insert into ROW_DELETE_TABLE_01 values(NULL,NULL,NULL,1198754521,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
insert into vector_delete_table_01 select * from row_delete_table_01;
analyze vector_delete_table_01;
delete from vector_delete_table_01 where col_int0=1 and col_date='2015-04-15';
select count(*) from  vector_delete_table_01 where col_int0=1 and col_date='2015-04-15';
delete from vector_delete_table_01 where col_date is NULL ;
delete from vector_delete_table_01 where col_decimal is not NULL; 
delete from vector_delete_table_01;
select count(*) from  vector_delete_table_01;

----
--- case 3: With Big Amount
----
CREATE OR REPLACE PROCEDURE func_insert_tbl_delete_01()
AS
BEGIN
    FOR I IN 0..2000 LOOP
        insert into ROW_DELETE_TABLE_01 values(11, 50, 100, 2024856154, 'gg', 'ffffff', 'testdelete'||i, 0.01, 10.11, 100.01, '2015-05-20', '16:00:38', '1996-2-6 01:00:30+8', '2 day 13:24:56');     
    END LOOP;
END;
/
CALL func_insert_tbl_delete_01();
insert into vector_delete_table_01 select * from row_delete_table_01;

delete from vector_delete_table_01 where col_char = 'gg' and col_vchar = 'ffffff';
select count(*) from  vector_delete_table_01;

----
--- case 4: non-correlated subquery
----
explain (verbose on, costs off) delete from vector_delete_table_03 where exists(select col_date from vector_delete_table_02);
delete from vector_delete_table_03 where exists(select col_date from vector_delete_table_02);

----
--- Clean Table and Resource
----
drop schema vector_delete_engine cascade;


