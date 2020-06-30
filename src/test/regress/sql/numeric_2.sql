-- TO_NUMBER()
--
SELECT '' AS to_number_1,  to_number('-34,338,492', '99G999G999');
SELECT '' AS to_number_2,  to_number('-34,338,492.654,878', '99G999G999D999G999');
SELECT '' AS to_number_3,  to_number('<564646.654564>', '999999.999999PR');
SELECT '' AS to_number_4,  to_number('0.00001-', '9.999999S');
SELECT '' AS to_number_5,  to_number('5.01-', 'FM9.999999S');
SELECT '' AS to_number_5,  to_number('5.01-', 'FM9.999999MI');
SELECT '' AS to_number_7,  to_number('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9');
SELECT '' AS to_number_8,  to_number('.01', 'FM9.99');
SELECT '' AS to_number_9,  to_number('.0', '99999999.99999999');
SELECT '' AS to_number_10, to_number('0', '99.99');
SELECT '' AS to_number_11, to_number('.-01', 'S99.99');
SELECT '' AS to_number_12, to_number('.01-', '99.99S');
SELECT '' AS to_number_13, to_number(' . 0 1-', ' 9 9 . 9 9 S');

--
-- Input syntax
--

CREATE TABLE num_input_test (n1 numeric);

-- good inputs
INSERT INTO num_input_test(n1) VALUES (' 123');
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
INSERT INTO num_input_test(n1) VALUES ('  -93853');
INSERT INTO num_input_test(n1) VALUES ('555.50');
INSERT INTO num_input_test(n1) VALUES ('-555.50');
INSERT INTO num_input_test(n1) VALUES ('NaN ');
INSERT INTO num_input_test(n1) VALUES ('        nan');

-- bad inputs
INSERT INTO num_input_test(n1) VALUES ('     ');
INSERT INTO num_input_test(n1) VALUES ('   1234   %');
INSERT INTO num_input_test(n1) VALUES ('xyz');
INSERT INTO num_input_test(n1) VALUES ('- 1234');
INSERT INTO num_input_test(n1) VALUES ('5 . 0');
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
INSERT INTO num_input_test(n1) VALUES ('');
INSERT INTO num_input_test(n1) VALUES (' N aN ');

SELECT * FROM num_input_test ORDER BY n1;

--
-- Test some corner cases for multiplication
--

select 4790999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999;

select 4789999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999;

select 4770999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999;

select 4769999999999999999999999999999999999999999999999999999999999999999999999999999999999999 * 9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999;

--
-- Test some corner cases for division
--

select 999999999999999999999::numeric/1000000000000000000000;
select div(999999999999999999999::numeric,1000000000000000000000);
select mod(999999999999999999999::numeric,1000000000000000000000);
select div(-9999999999999999999999::numeric,1000000000000000000000);
select mod(-9999999999999999999999::numeric,1000000000000000000000);
select div(-9999999999999999999999::numeric,1000000000000000000000)*1000000000000000000000 + mod(-9999999999999999999999::numeric,1000000000000000000000);
select mod (70.0,70) ;
select div (70.0,70) ;
select 70.0 / 70 ;
select 12345678901234567890 % 123;
select 12345678901234567890 / 123;
select div(12345678901234567890, 123);
select div(12345678901234567890, 123) * 123 + 12345678901234567890 % 123;

--
-- cast varchar(bpchar) to float4/float8 : first we should check pg_cast
--
select * from pg_cast where castfunc in (4196,4197,4198,4199);
CREATE TABLE num_cast(a varchar(15), b bpchar(15));
insert into num_cast values('12500','175800');
select to_number((num_cast.a::float4)/(10^2)) from num_cast;
select to_number((num_cast.a::float8)/(10^2)) from num_cast;
select to_number((num_cast.b::float4)/(10^2)) from num_cast;
select to_number((num_cast.b::float8)/(10^2)) from num_cast;
drop table num_cast;

--
-- varchar + int (convert to numeric + numeric in C format)
--
create database icbc template template0 encoding 'SQL_ASCII' dbcompatibility 'C';
\c icbc
create table test(a int, b varchar(10), c varbit(10), d bool, e date, f interval);
insert into test values(5, '13.8', '010101', true, '1980-01-01', interval '1' year);
select a+b, b+10, d=true, e+f from test;
select * from test where c='01001';
\c regression
drop database icbc;


--test stddev_samp
  create table stddev_samp_104(
  id               integer                 ,
  COL_SMALLINT     smallint
 )
 WITH(ORIENTATION=COLUMN); 

 insert into stddev_samp_104 values(1, 5);
 insert into stddev_samp_104 values(1, 2); 
 insert into stddev_samp_104 values(1, 3);
 insert into stddev_samp_104 values(2, -5);
 insert into stddev_samp_104 values(2, -2);
 insert into stddev_samp_104 values(2, -3);

 select ID, stddev_samp(COL_SMALLINT) as RESULT from stddev_samp_104 group by ID order by 1,2;
 
 drop table stddev_samp_104;
