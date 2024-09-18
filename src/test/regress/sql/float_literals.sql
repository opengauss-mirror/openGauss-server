-- test float literals overflow in dbcompatibility A
create database float_literals dbcompatibility 'A';
\c float_literals

SELECT 0.0;
SELECT -0.0;
SELECT 3.142596;
SELECT -3.142596;
SELECT 1.79E+400;
SELECT 1.79E-400;
SELECT -1.79E+400;
SELECT 1E-307;
SELECT 1E-308;

SELECT '0.0';
SELECT '-0.0';
SELECT '3.142596';
SELECT '-3.142596';
SELECT '1.79E+400';
SELECT '1.79E-400';
SELECT '-1.79E+400';
SELECT '1E-307';
SELECT '1E-308';

SELECT '0.0'::float8;
SELECT '-0.0'::float8;
SELECT '3.142596'::float8;
SELECT '-3.142596'::float8;
SELECT '1.79E+400'::float8;
SELECT '1.79E-400'::float8;
SELECT '-1.79E+400'::float8;
SELECT '1E-307'::float8;
SELECT '1E-308'::float8;

SELECT TO_BINARY_FLOAT(3.14 DEFAULT y ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(-3.14 DEFAULT + ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(3.14 DEFAULT - ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(-3.14 DEFAULT * ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(3.14 DEFAULT / ON CONVERSION ERROR);

SELECT TO_BINARY_FLOAT('3.14' DEFAULT y ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('-3.14' DEFAULT + ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('3.14' DEFAULT - ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('-3.14' DEFAULT * ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('3.14' DEFAULT / ON CONVERSION ERROR);

SELECT TO_BINARY_FLOAT(1.79E+400 DEFAULT y ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(1.79E+400 DEFAULT + ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(1.79E+400 DEFAULT - ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(1.79E+400 DEFAULT * ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT(1.79E+400 DEFAULT / ON CONVERSION ERROR);

SELECT TO_BINARY_FLOAT('1.79E+400' DEFAULT y ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('1.79E-400' DEFAULT + ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('-1.79E+400' DEFAULT - ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('-1.79E-400' DEFAULT * ON CONVERSION ERROR);
SELECT TO_BINARY_FLOAT('1.79E+400' DEFAULT / ON CONVERSION ERROR);

CREATE TABLE t_float_literals (id int, c1 float8);
INSERT INTO t_float_literals VALUES (1, 0.0);
INSERT INTO t_float_literals VALUES (2, 3.14);
INSERT INTO t_float_literals VALUES (3, 3.14E+40);
INSERT INTO t_float_literals VALUES (4, -3.14E+40);
INSERT INTO t_float_literals VALUES (5, '3.14E+40'::float8);
INSERT INTO t_float_literals VALUES (6, '-3.14E+40'::float8);
INSERT INTO t_float_literals VALUES (7, 3.14E+400);
INSERT INTO t_float_literals VALUES (8, 3.14E-400);
INSERT INTO t_float_literals VALUES (9, -3.14E+400);
INSERT INTO t_float_literals VALUES (10, '3.14E+400'::float8);
INSERT INTO t_float_literals VALUES (11, '3.14E-400'::float8);
INSERT INTO t_float_literals VALUES (12, '-3.14E+400'::float8);
SELECT * FROM t_float_literals ORDER bY id;

UPDATE t_float_iterals SET c1 = 1.79E+400 WHERE id = 1;
UPDATE t_float_iterals SET c1 = '1.79E+400'::float8 WHERE id = 2;
UPDATE t_float_iterals SET c1 = 1.79E+40 WHERE id = 3;
UPDATE t_float_iterals SET c1 = '1.79E+40'::float8 WHERE id = 4;
SELECT * FROM t_float_iterals ORDER BY c1;

create table llvm_enh
(
	l_bool boolean default false,
	l_tint tinyint default 255,
	l_sint smallint default 32767,
	l_int  integer  default 2147483647,
	l_bint bigint default 9223372036854775807,
	l_num1 numeric(18,0) default 999999999999999999,
	l_num2 numeric(19,19) default 0.9223372036854775807,
	l_num3 numeric(38,0) default 99999999999999999999999999999999999999,
	l_flo1 float4       default 999999,
	l_flo2 float8       default 1E-307,
	l_char char(39)      default '170141183460469231731687303715884105728',
	l_vchar varchar(40)   default '-170141183460469231731687303715884105728',
	l_text text          default '-170141183460469231731687303715884105729',
	l_date date          default '2016-10-18',
	l_time time          default '21:21:21',
	l_times timestamp    default '2003-04-12 04:05:06',
	l_timez timestamp with time zone default '2003-04-12 04:05:06 pst',
	l_oid oid  default 12345
) ;
insert into llvm_enh(l_bool) values(true);
insert into llvm_enh(l_tint) values(0);
insert into llvm_enh(l_sint) values(-32768);
insert into llvm_enh(l_int)  values(-2147483648);
insert into llvm_enh(l_bint) values(-9223372036854775808);
insert into llvm_enh(l_num1) values(-999999999999999999);
insert into llvm_enh(l_num2) values(-0.9223372036854775808);
insert into llvm_enh(l_num3) values(-99999999999999999999999999999999999999);
insert into llvm_enh(l_flo1) values(-999999);
insert into llvm_enh(l_flo2) values(1E+308);

select l_flo2,l_flo1 from llvm_enh where l_flo2 < l_flo1-999998 order by 1,2;

drop table t_float_literals;
drop table llvm_enh;
\c regression
drop database float_literals;