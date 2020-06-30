/*
 * This file is used to test the function of ExecVecHashJoin()
 */
----
--- Create Table and Insert Data
----
create schema vector_limit_engine;
set current_schema=vector_limit_engine;

create table vector_limit_engine.ROW_LIMIT_TABLE_01
(
   col_int int
  ,col_char char(25)
  ,col_vchar varchar(35)
  ,col_date date
  ,col_num numeric(10,4)
  ,col_float1 float4
  ,col_float2 float8
  ,col_interval interval
);

create table vector_limit_engine.VECTOR_LIMIT_TABLE_01
(
   col_int int
  ,col_char char(25)
  ,col_vchar varchar(35)
  ,col_date date
  ,col_num numeric(10,4)
  ,col_float1 float4
  ,col_float2 float8
  ,col_interval interval
) with(orientation = column);

CREATE OR REPLACE PROCEDURE func_insert_tbl_limit_01()
AS  
BEGIN  
        FOR I IN 1..500 LOOP 
                if i = 20 OR i = 30 OR i = 60 OR i = 70 OR i = 130 OR i = 1026 then
                        INSERT INTO vector_limit_engine.row_limit_table_01 VALUES(i,NULL,NULL, '2015-01-01',9.12+i,99.123+i,999.1234+i,'6 day 12:25:56');
				elsif i = 75 OR i = 150 OR i = 1200 then
						INSERT INTO vector_limit_engine.row_limit_table_01 VALUES(i,'testveclimitchar'||i,'testveclimitvarchar'||i,'2015-01-01',9.12+i,99.123+i,999.1234+i,'4 day 12:25:56');
				elsif i = 186 OR i = 706 then
						INSERT INTO vector_limit_engine.row_limit_table_01 VALUES(i,'testveclimitchar'||i,'testveclimitvarchar'||i,'2015-01-01',9.12+i,99.123+i,999.1234+i,'8 day 08:15:25');
                else
                        INSERT INTO vector_limit_engine.row_limit_table_01 VALUES(i,'testveclimitchar'||i,'testveclimitvarchar'||i,'2015-01-01',9.12+i,99.123+i,999.1234+i,'1 day 12:34:56');
                end if; 
        END LOOP; 
END;
/
CALL func_insert_tbl_limit_01();

insert into VECTOR_LIMIT_TABLE_01 select * from ROW_LIMIT_TABLE_01;

analyze VECTOR_LIMIT_TABLE_01;

SET DateStyle = 'ISO, MDY';

----
--- test 1: Test VecLimit
----
explain (verbose on, costs off) select A.col_int, A.col_vchar, A.col_interval from vector_limit_table_01 A order by 1 offset 0 limit 1000;
select A.col_int, A.col_vchar, A.col_interval from vector_limit_table_01 A order by 1, 2, 3 offset 0 limit 1000;
select * from vector_limit_table_01 offset 0 limit 0;
select * from vector_limit_table_01 order by 1 offset 0 limit 1;
select * from vector_limit_table_01 order by 1 offset 0 limit 1000;
select * from vector_limit_table_01 order by 1 offset 0 limit 1001;

select * from vector_limit_table_01 offset 1 limit 0;
select * from vector_limit_table_01 order by 1 offset 1 limit 1;
select * from vector_limit_table_01 order by 1 offset 1 limit 1001;

select * from vector_limit_table_01 order by 1 offset 500 limit 0;
select * from vector_limit_table_01 order by 1 offset 500 limit 1;
select * from vector_limit_table_01 order by 1 offset 500 limit 501;
select * from vector_limit_table_01 order by 1 offset 500 limit 1001;
select * from vector_limit_table_01 order by 1 offset 500;
 
select * from vector_limit_table_01 order by 1 offset 1000 limit 0;
select * from vector_limit_table_01 order by 1 offset 1000 limit 1000;
select * from vector_limit_table_01 order by 1 offset 1000;

----
--- test 2: Test Functions(vupper/vlower/vrtrim/vtextlike/vtextnlike)
----
select upper(col_vchar) from vector_limit_table_01 where col_int = 2 and col_char = 'testveclimitchar2';
select upper(col_vchar) from vector_limit_table_01 where col_int = 2;
select upper(col_vchar) from vector_limit_table_01 where col_int = 3 or col_int = 20 or col_int = 30 order by 1;

select lower(col_vchar) from vector_limit_table_01 where col_int = 3 and col_char = 'testveclimitchar3';
select lower(col_vchar) from vector_limit_table_01 where col_int = 3;
select lower(col_vchar) from vector_limit_table_01 where col_int = 3 or col_int = 20 or col_int = 30 order by 1;

select rtrim(col_vchar) from vector_limit_table_01 where col_int = 3;
select rtrim(col_vchar) from vector_limit_table_01 where col_int = 3 or col_int = 20 or col_int = 30 order by 1;

select col_vchar from vector_limit_table_01 where col_vchar like 'testveclimitvarchar3';
select col_vchar from vector_limit_table_01 where col_vchar like 'testveclimitvarchar3' and col_int = 3;
select col_vchar from vector_limit_table_01 where col_vchar like 'testveclimitvarchar%' and col_int = 20;
select col_vchar from vector_limit_table_01 where col_vchar like 'testveclimitvarchar3' or col_int = 20 or col_int = 30 order by 1;

select col_vchar from vector_limit_table_01 where col_vchar not like 'testveclimitvarchar3' and col_int = 4;
select col_vchar from vector_limit_table_01 where col_vchar not like 'testveclimitvarchar%' and col_int = 20;
select col_vchar from vector_limit_table_01 where col_vchar not like 'testveclimitvarchar%';

----
--- Clean Table and Resource
----
drop schema vector_limit_engine cascade;
