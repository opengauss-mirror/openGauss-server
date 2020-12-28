-- int1shr, int1shl
select int1shr(1,-1);
select int1shl(1,-1);

/*
 * instr
 */
-- instr(text,text)
select instr(null,null) ;
select instr(null,'bc') ;
select instr('abcd12abc',null) ;
select instr('abcd12abc','bc') ;
select instr('abcd12abc','ff') ;

-- instr(text,text,integer)
select instr(null,null,1) ;
select instr(null,'bc',1) ;
select instr('abcd12abc',null,-9) ;

select instr('abcd12abc','bc',0) ;
select instr('abcd12abc','bc',1) ;
select instr('abcd12abc','bc',9) ;
select instr('abcd12abc','bc',10) ;
select instr('abcd12abc','bc',-1) ;
select instr('abcd12abc','bc',5) ;

-- instr(text,text,integer,integer)
select instr('abcd12abc','bc',1,-1) ;
select instr('abcd12abc','bc',1,0) ;
select instr('abcd12abc','bc',1,1) ;
select instr('abcd12abc','bc',1,2) ;
select instr('abcd12abc','bc',1,3) ;

select instr('abcd12abc','bc',-1,0) ;
select instr('abcd12abc','bc',-1,1) ;
select instr('abcd12abc','bc',-1,2) ;
select instr('abcd12abc','bc',-1,3) ;

select instr('abcd12abc','bc',2,1) ;
select instr('abcd12abc','bc',2,2) ;
select instr('abcd12abc','bc',2,3) ;
SELECT INSTR('corporate floor','or', 3);
SELECT INSTR('corporate floor','or',-3,2);

/*
 * multiply
 */
--multiply(text,float8)

select multiply('abcd',45123.12134::float8);

select multiply(null,45123.12134::float8);
select multiply('1E-307',1E-307::float8);
select multiply('1E+308',1E+308::float8);
select multiply('5678.1234',1234.5678::float8);

--multiply(float8,text)
select multiply(45123.12134::float8,'abcd');
select multiply(45123.12134::float8,null);
select multiply(1E-307::float8,'1E-307');
select multiply(1E+308::float8,'1E+308');
select multiply(1234.5678::float8,'5678.1234');

/*
 * texteq
 */
--texteq(numeric,text)
select texteq(null,null);
select texteq(922337.999999::numeric,null);
select texteq(922337.999999::numeric,'123');
select texteq(922337.999999::numeric,'922337.999999');

--texteq(text,numeric)
select texteq(null,922337.999999::numeric);
select texteq('123',922337.999999::numeric);
select texteq('922337.999999',922337.999999::numeric);

/*
 * numtodsinterval
 */
select numtodsinterval(240,'SECOND');
select numtodsinterval(2,'hour');
select numtodsinterval(2,'day');

select numtodsinterval(240.5623,'SECOND');
select numtodsinterval(2.5623,'hour');
select numtodsinterval(2.5623,'day');


select 1/3;
select 4/3;

/*
 * mod
 */
--mod zero
select mod(3,0) from dual;
 
--mod(float8,numeric)
select mod(0.0::float8,0.0::numeric) from dual;
select mod(0.0::float8,1234.5678::numeric) from dual;
select mod(1234.5678::float8,0.0::numeric) from dual;
select mod(1234.5678::float8,0.0::numeric) from dual;
select mod(11234.5678::float8,9638.5632::numeric) from dual;

--mod(numeric,float8)
select mod(0.0::numeric,0.0::float8) from dual;
select mod(1234.5678::numeric,0.0::float8) from dual;
select mod(0.0::numeric,1234.5678::float8) from dual;
select mod(0.0::numeric,1234.5678::float8) from dual;
select mod(11234.5678::numeric,9638.5632::float8) from dual;

/*
 * to_char
 */
select to_char('2012-08-02 20:38:40.2365'::timestamp without time zone) from dual;
select to_char('2012-08-02 20:38:40.2365'::timestamp with time zone) from dual;

select to_char(1234::int2) from dual;
select to_char(1234::int4) from dual;
select to_char(1234::int8) from dual;
select to_char(1234::float4) from dual;
select to_char(1234::float8) from dual;
select to_char(1234::numeric) from dual;
select to_char(null) from dual;
select to_char('hello') from dual;

select to_char(1234::int2,'xxxx') from dual;
select to_char(1234::int2,'XXXX') from dual;
select to_char(1234::int4,'xxxx') from dual;
select to_char(1234::int4,'XXXX') from dual;
select to_char(1234::int8,'xxxx') from dual;
select to_char(1234::int8,'XXXX') from dual;

select to_char(1234.234::float4,'xxxx') from dual;
select to_char(1234.534::float4,'xxxx') from dual;
select to_char(1234.234::float4,'XXXX') from dual;
select to_char(1234.534::float4,'XXXX') from dual;
select to_char(1234.234::float8,'xxxx') from dual;
select to_char(1234.534::float8,'xxxx') from dual;
select to_char(1234.234::float8,'XXXX') from dual;
select to_char(1234.534::float8,'XXXX') from dual;
select to_char(1234.234::numeric,'xxxx') from dual;
select to_char(1234.534::numeric,'XXXX') from dual;

select to_char(3, 'RN') as result from dual;
select to_char(3, 'FMRN') as result from dual;
select to_char(5, 'RN') as result from dual;
select to_char(5, 'FMRN') as result from dual;
select to_char(485, 'RN') as result from dual;
select to_char(485, 'FMRN') as result from dual;

/* adapt A db's date/time format 'FF'/'ff' */
select to_char('2012-08-10 15:09:40.123456789'::timestamp without time zone,'HH24:MI:SS.FF') from dual;
select to_char('2012-08-10 15:09:40.123456789'::timestamp without time zone,'HH24:MI:SS.ff') from dual;
select to_char('2012-08-10 15:09:40.123456789'::timestamp with time zone,'HH24:MI:SS.FF') from dual;
select to_char('2012-08-10 15:09:40.123456789'::timestamp with time zone,'HH24:MI:SS.ff') from dual;

/*
 * to_number
 */
select to_number(null,'XXXX') from dual;
select to_number('127.532','XXXX') from dual;
select to_number('1234567890abcdefABCDEF','XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') from dual;

--to_number only support 16 bytes hex to decimal conversion or plen less than 0
select to_number('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF','XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') from dual;
select to_number('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF','xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx') from dual;
select to_number('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF','XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') from dual;

create table TONUM_006(COL_NUM numeric);
insert into TONUM_006 values(to_number('A0', 'X'));
insert into TONUM_006 values(to_number('A0', 'XX'));
insert into TONUM_006 values(to_number('A0', 'XXX'));
select COL_NUM as RESULT from TONUM_006;
drop table TONUM_006;

select to_number('-ABF1','-xxxx')from dual;
select to_number('AA','XXXZ') from dual;
select to_number('-AA','-XX') from dual;
select to_number('AA','-XX') from dual;
select to_number('-AA','XX') from dual;
select to_number('+AA','XX') from dual;
select to_number('+AA','XX') from dual;
select to_number('AA','X') from dual;
select to_number('aa','x') from dual;
select to_number('AA.A','XXX.X') from dual;
select to_number('A0','XX X') from dual;
select to_number('A0','XX.X') from dual;
select to_number('A0','+xxx') from dual;
select to_number('A0','-xxx') from dual;

/*
 * nvl 
 */
select nvl(null, null) from dual;
select nvl(null, 'other') from dual;
select nvl('something','other') from dual;
select nvl(null,321) from dual;
select nvl(123,'321') from dual;
select nvl(123::int2,321::int2) from dual;
select nvl(123::int4,321::int4) from dual;
select nvl(123::int8,321::int8) from dual;
select nvl(123::float4,321::float4) from dual;
select nvl(123::float8,321::float8) from dual;
select nvl('2001-01-01 01:01:01.3654'::date,'2012-08-02 15:57:54.6365'::date) from dual;
select nvl('2001-01-01 01:01:01.3654'::timestamp,'2012-08-02 15:57:54.6365'::timestamp) from dual;

-- modify NVL display to A db's style "NVL" instead of "coalesce"
CREATE TABLE NVL_01(COL_INTEGER1 INTEGER,COL_INTEGER2 INTEGER);
INSERT INTO NVL_01 VALUES(1,2);
SELECT NVL(COL_INTEGER1,COL_INTEGER2) FROM NVL_01;
DROP TABLE NVL_01;
SELECT COALESCE('1234',5678) FROM DUAL;

--nvl(unknown, int)
SELECT NVL('j', 1) FROM DUAL;
--nvl(int, unknown)
SELECT NVL(1, 'k') FROM DUAL;
SELECT NVL(1, '6') FROM DUAL;
--nvl(char, int)
SELECT NVL('o'::char, 1) FROM DUAL;
--nvl(int, char)
SELECT NVL(1, 'o'::char) FROM DUAL;
--nvl(text, int2)
SELECT NVL('hello!'::text, '100'::int2) FROM DUAL;
SELECT NVL('100'::int2, 'hello!'::text) FROM DUAL;
--nvl(text, int4)
SELECT NVL('hello!'::text, '100'::int4) FROM DUAL;
SELECT NVL('100'::int4, 'hello!'::text) FROM DUAL;
--nvl(text, int8)
SELECT NVL('hello!'::text, '100'::int8) FROM DUAL;
SELECT NVL('100'::int8, 'hello!'::text) FROM DUAL;
--nvl(text, float4)
SELECT NVL('hello!'::text, '100.001'::float4) FROM DUAL;
SELECT NVL('100.001'::float4, 'hello!'::text) FROM DUAL;
--nvl(text, float8)
SELECT NVL('hello!'::text, '100.001'::float8) FROM DUAL;
SELECT NVL('100.001'::float8, 'hello!'::text) FROM DUAL;
--nvl(text, number)
SELECT NVL('hello!'::text, '100.001'::number) FROM DUAL;
SELECT NVL('100.001'::number, 'hello!'::text) FROM DUAL;

--null
SELECT NVL('j', '') FROM DUAL;
SELECT NVL('j', null) FROM DUAL;
SELECT NVL('', 'k') FROM DUAL;
SELECT NVL(null, 'k') FROM DUAL;
SELECT NVL(''::text, '100'::int2) FROM DUAL;
SELECT NVL(''::int2, 'hello!'::text) FROM DUAL;
SELECT NVL('hello!'::text, ''::int2) FROM DUAL;
SELECT NVL('100'::int2, ''::text) FROM DUAL;
SELECT NVL(''::text, '100.001'::float8) FROM DUAL;
SELECT NVL(''::float8, 'hello!'::text) FROM DUAL;
SELECT NVL('hello!'::text, ''::float8) FROM DUAL;
SELECT NVL('100.001'::float8, ''::text) FROM DUAL;
SELECT NVL(''::text, '100.001'::number) FROM DUAL;
SELECT NVL(''::number, 'hello!'::text) FROM DUAL;
SELECT NVL('hello!'::text, ''::number) FROM DUAL;
SELECT NVL('100.001'::number, ''::text) FROM DUAL;

--NVL(char, int)
CREATE OR REPLACE PROCEDURE SP_HW_CM_INS_TBL_TRUNKGROUP(psoutnoclir IN CHAR)
AS
	SUN CHAR:= psoutnoclir;
BEGIN
	--RAISE INFO 'PSOUTNOCLIR is %', PSOUTNOCLIR;
END;
/
CREATE OR REPLACE PROCEDURE SP_HW_ADDNO7TRUNKGROUP(iv_psOutNoClir IN CHAR)
AS
	v_OutNoClir    CHAR := iv_psOutNoClir;
BEGIN
	--RAISE INFO 'function start!';
	SP_HW_CM_INS_TBL_TRUNKGROUP(NVL(v_OutNoClir, 0));
	--RAISE INFO 'function end!';
END;
/
CALL SP_HW_ADDNO7TRUNKGROUP('');
CALL SP_HW_ADDNO7TRUNKGROUP(null);
CALL SP_HW_ADDNO7TRUNKGROUP('0');
CALL SP_HW_ADDNO7TRUNKGROUP('Y');
DROP PROCEDURE SP_HW_ADDNO7TRUNKGROUP;
DROP PROCEDURE  SP_HW_CM_INS_TBL_TRUNKGROUP;

--NVL(number, int)
CREATE OR REPLACE PROCEDURE SP_HW_CM_INS_TBL_TRUNKGROUP(psoutnoclir IN number DEFAULT 0)
AS
	SUN number := psoutnoclir;
BEGIN
	--RAISE INFO 'PSOUTNOCLIR is %', PSOUTNOCLIR;
END;
/
CREATE OR REPLACE PROCEDURE SP_HW_ADDNO7TRUNKGROUP(iv_psOutNoClir IN number DEFAULT 0)
AS
	v_OutNoClir    number := iv_psOutNoClir;
BEGIN
	--RAISE INFO 'function start!';
	SP_HW_CM_INS_TBL_TRUNKGROUP(NVL(v_OutNoClir, 0));
	--RAISE INFO 'function end!';
END;
/
CALL SP_HW_ADDNO7TRUNKGROUP();
CALL SP_HW_ADDNO7TRUNKGROUP(null);
CALL SP_HW_ADDNO7TRUNKGROUP('0');
CALL SP_HW_ADDNO7TRUNKGROUP(10000);
DROP PROCEDURE SP_HW_ADDNO7TRUNKGROUP;
DROP PROCEDURE SP_HW_CM_INS_TBL_TRUNKGROUP;

--NVL(text, int)
CREATE OR REPLACE PROCEDURE SP_HW_CM_INS_TBL_TRUNKGROUP(psoutnoclir IN text DEFAULT '000')
AS
	SUN text := psoutnoclir;
BEGIN
	--RAISE INFO 'PSOUTNOCLIR is %', PSOUTNOCLIR;
END;
/
CREATE OR REPLACE PROCEDURE SP_HW_ADDNO7TRUNKGROUP(iv_psOutNoClir IN text DEFAULT '000')
AS
	v_OutNoClir    text := iv_psOutNoClir;
BEGIN
	--RAISE INFO 'function start!';
	SP_HW_CM_INS_TBL_TRUNKGROUP(NVL(v_OutNoClir, 0));
	--RAISE INFO 'function end!';
END;
/
CALL SP_HW_ADDNO7TRUNKGROUP();
CALL SP_HW_ADDNO7TRUNKGROUP(null);
CALL SP_HW_ADDNO7TRUNKGROUP('0');
CALL SP_HW_ADDNO7TRUNKGROUP('zhangyangeng');
DROP PROCEDURE SP_HW_ADDNO7TRUNKGROUP;
DROP PROCEDURE SP_HW_CM_INS_TBL_TRUNKGROUP;




/*
 * substr
 */
--substr(text, integer)
--boundary test
select substr(null,1) from dual;
select substr('12345',0) from dual;
select substr('12345',1) from dual;
select substr('12345',5) from dual;
select substr('12345',6) from dual;
select substr('12345',-1) from dual;
select substr('12345',-5) from dual;
select substr('12345',-6) from dual;
--normal
select substr('12345',2) from dual;
select substr('12345',-2) from dual;

--substr(text,integer,integer)
--boundary test
select substr(null,1,1) from dual;
select substr('12345',0,4) from dual;
select substr('12345',1,4) from dual;
select substr('12345',5,4) from dual;
select substr('12345',6,4) from dual;
select substr('12345',-1,4) from dual;
select substr('12345',-5,4) from dual;
select substr('12345',-6,4) from dual;
select substr('12345',1,-1) from dual;
select substr('12345',1,5) from dual;
select substr('12345',1,6) from dual;
--normal
select substr('12345',2,3) from dual;
select substr('12345',-4,3) from dual;

--substr(bytea, integer)
--boundary test
select substr(null::bytea,1) from dual;
select substr('12345'::bytea,0) from dual;
select substr('12345'::bytea,1) from dual;
select substr('12345'::bytea,5) from dual;
select substr('12345'::bytea,6) from dual;
select substr('12345'::bytea,-1) from dual;
select substr('12345'::bytea,-5) from dual;
select substr('12345'::bytea,-6) from dual;
--normal
select substr('12345'::bytea,2) from dual;
select substr('12345'::bytea,-2) from dual;

--substr(bytea,integer,integer)
--boundary test
select substr(null,1,1) from dual;
select substr('12345'::bytea,0,4) from dual;
select substr('12345'::bytea,1,4) from dual;
select substr('12345'::bytea,5,4) from dual;
select substr('12345'::bytea,6,4) from dual;
select substr('12345'::bytea,-1,4) from dual;
select substr('12345'::bytea,-5,4) from dual;
select substr('12345'::bytea,-6,4) from dual;
select substr('12345'::bytea,1,-1) from dual;
select substr('12345'::bytea,1,5) from dual;
select substr('12345'::bytea,1,6) from dual;
select substr('abcd'::bytea,2,0) from dual;
--normal
select substr('12345'::bytea,2,3) from dual;
select substr('12345'::bytea,-4,3) from dual;
--select more than one parameter in update
create table test_update_more (a int, b int);
insert into test_update_more values(1, 2);
create table tbl (col1 int, col2 int) ;
ALTER TABLE tbl ADD PRIMARY KEY(col1, col2);
insert into tbl values(10, 20);
insert into tbl values(100, 200);
update tbl set(col1, col2) = (select a, b from test_update_more) where col1 = 10;
select * from tbl;
drop table test_update_more;
drop table tbl;
create table test_reg(mc varchar(60));
insert into test_reg values('112233445566778899');
insert into test_reg values('22113344 5566778899');
insert into test_reg values('33112244 5566778899');
insert into test_reg values('44112233 5566 778899');
insert into test_reg values('5511 2233 4466778899');
insert into test_reg values('661122334455778899');
insert into test_reg values('771122334455668899');
insert into test_reg values('881122334455667799');
insert into test_reg values('991122334455667788');
insert into test_reg values('aabbccddee');
insert into test_reg values('bbaaaccddee');
insert into test_reg values('ccabbddee');
insert into test_reg values('ddaabbccee');
insert into test_reg values('eeaabbccdd');
insert into test_reg values('ab123');
insert into test_reg values('123xy');
insert into test_reg values('007ab');
insert into test_reg values('abcxy');
insert into test_reg values('The final test_reg is is is how to find duplicate words.');
select * from test_reg where regexp_like(mc,'^a{1,3}') order by mc ;
select * from test_reg where regexp_like(mc,'a{1,3}') order by mc ;
select * from test_reg where regexp_like(mc,'^a.*e$') order by mc ;
select * from test_reg where regexp_like(mc,'^[[:lower:]]|[[:digit:]]') order by mc ;
select * from test_reg where regexp_like(mc,'^[[:lower:]]') order by mc ;
Select mc FROM test_reg Where REGEXP_LIKE(mc,'[^[:digit:]]') order by mc ;
Select mc FROM test_reg Where REGEXP_LIKE(mc,'^[^[:digit:]]') order by mc ;
select * from test_reg where regexp_like(mc,'B') order by mc ;
select * from test_reg where regexp_like(mc,'B','i') order by mc ;
create table TESTREGEXP_LIKE(COL_STR1 varchar2(100));
insert into TESTREGEXP_LIKE values('you
are
so
beautiful');
insert into TESTREGEXP_LIKE values('hello\ngauss');
insert into TESTREGEXP_LIKE values('
my
world
');
insert into TESTREGEXP_LIKE values('

can
you
feel

');


select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^s','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'l$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^so$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^so
b','m') order by COL_STR1 ;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^so
beautiful$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^are
so
beautiful$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'so','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'eau','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'are
so','m') order by COL_STR1;

select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^e','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'k$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^g','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'o$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^eau$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^o
beautiful$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'o
beau$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^are
o
beau$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^feel$','m') order by COL_STR1;

select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^m','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'y$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'orl','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^my
world$','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^m
w','m') order by COL_STR1;

select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^
my
','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^world
','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^
my
world$','m')order by COL_STR1;

select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^
world
','m')order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^
w
','m') order by COL_STR1;
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'
^my
world$
','m')order by COL_STR1;
--other parameter,return error information.
select * from TESTREGEXP_LIKE where regexp_like(COL_STR1,'^s','k') order by COL_STR1;

drop table TESTREGEXP_LIKE;
/*
 * regexp_substr
 */
SELECT REGEXP_SUBSTR(mc,'[a-z]+') FROM test_reg order by mc;
SELECT REGEXP_SUBSTR(mc,'[0-9]+') FROM test_reg order by mc;
SELECT REGEXP_SUBSTR('aababcde','^a.*b') order by mc;

drop table test_reg;
show enforce_a_behavior;
select regexp_substr('week', '(a|e)');
select regexp_substr('week', '((a)|(e))');
select regexp_substr('week', '(a)|(e)');
set enforce_a_behavior=false;
show enforce_a_behavior;
select regexp_substr('week', '((a)|(e))');
select regexp_substr('week', '(a)|(e)');

--length and lengthb test. Assume that the database encoding is utf-8. 
SELECT length('a'), length('a '), length(' a ');
select lengthb('abc'), length('abc '), lengthb('高斯');

create schema hw_function_p_1;
set search_path to hw_function_p_1;

--test parameters of function 
create function test_fun_0() returns integer
as $$
begin
    raise '%', 'test para is 0';
    return 0;
end;
$$language plpgsql;
select test_fun_0();
--drop function test_fun_0;

create function test_fun_1(a varchar) returns integer
as $$
begin
    raise info '%', a;
    return 1;
end;
$$language plpgsql;
select test_fun_1('test para is 1');
--drop function test_fun_1;

create function test_fun_2(a integer[]) returns integer
as $$
begin
    raise info '%', a;
    return 3;
end;
$$language plpgsql;
select test_fun_2('{1,2,3}');
--drop function test_fun_2;

create function test_fun_3(a int, b varchar(10), c integer[], d integer[][], e text[][], f int, g int, h int, i int, j int)returns integer
as $$
begin
    raise info '%', a;
    raise info '%', b;
    raise info '%', c;
    raise info '%', d;
    raise info '%', e;
    return 3;
end;
$$language plpgsql;
select test_fun_3 (10, 'test_10', '{1,2,3}', '{{1,2,3},{4,5,6}}', '{{"breakfast", "consulting"}, {"meeting", "lunch"}}', 1,1,1,1,1);
--drop function test_fun_3;

create function test_fun_4(a int, b varchar(10), c integer[], d integer[][], e text[][], f int, g int, h int, i int, j int, k int) returns integer
as $$
begin
    raise info '%', a;
    raise info '%', b;
    raise info '%', c;
    raise info '%', d;
    raise info '%', e;
    return 4;
end;
$$language plpgsql;
select test_fun_4 (10, 'test_11', '{1,2,3}', '{{1,2,3},{4,5,6}}', '{{"breakfast", "consulting"}, {"meeting", "lunch"}}', 1,1,1,1,1,1);
--drop function test_fun_4;

create function test_fun_5(a0 varchar, b0 int, c0 int, d0 int, e0 int, f0 int, g0 int, h0 int, i0 int, j0 int,
 a1 int, b1 int, c1 int, d1 int, e1 int, f1 int, g1 int, h1 int, i1 int, j1 int,
a2 int, b2 int, c2 int, d2 int, e2 int, f2 int, g2 int, h2 int, i2 int, j2 int,
a3 int, b3 int, c3 int, d3 int, e3 int, f3 int, g3 int, h3 int, i3 int, j3 int,
a4 int, b4 int, c4 int, d4 int, e4 int, f4 int, g4 int, h4 int, i4 int, j4 int,
a5 int, b5 int, c5 int, d5 int, e5 int, f5 int, g5 int, h5 int, i5 int, j5 int,
a6 int, b6 int, c6 int, d6 int, e6 int, f6 int, g6 int, h6 int, i6 int, j6 int,
a7 int, b7 int, c7 int, d7 int, e7 int, f7 int, g7 int, h7 int, i7 int, j7 int,
a8 int, b8 int, c8 int, d8 int, e8 int, f8 int, g8 int, h8 int, i8 int, j8 int,
a9 int, b9 int, c9 int, d9 int, e9 int, f9 int, g9 int, h9 int, i9 int, j9 int) returns integer
as $$
begin
        raise info '%', a0;
	return 0;
end;
$$language plpgsql;
select test_fun_5('test_100',2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100);
--drop function test_fun_5;

create function test_fun_6(a0 varchar, b0 int, c0 int, d0 int, e0 int, f0 int, g0 int, h0 int, i0 int, j0 int,
 a1 int, b1 int, c1 int, d1 int, e1 int, f1 int, g1 int, h1 int, i1 int, j1 int,
a2 int, b2 int, c2 int, d2 int, e2 int, f2 int, g2 int, h2 int, i2 int, j2 int,
a3 int, b3 int, c3 int, d3 int, e3 int, f3 int, g3 int, h3 int, i3 int, j3 int,
a4 int, b4 int, c4 int, d4 int, e4 int, f4 int, g4 int, h4 int, i4 int, j4 int,
a5 int, b5 int, c5 int, d5 int, e5 int, f5 int, g5 int, h5 int, i5 int, j5 int,
a6 int, b6 int, c6 int, d6 int, e6 int, f6 int, g6 int, h6 int, i6 int, j6 int,
a7 int, b7 int, c7 int, d7 int, e7 int, f7 int, g7 int, h7 int, i7 int, j7 int,
a8 int, b8 int, c8 int, d8 int, e8 int, f8 int, g8 int, h8 int, i8 int, j8 int,
a9 int, b9 int, c9 int, d9 int, e9 int, f9 int, g9 int, h9 int, i9 int, j9 int,
a10 varchar) returns integer
as $$
begin
        raise info '%', a0;
	return 0;
end;
$$language plpgsql;
select test_fun_6('test_101',2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100, 'test_para_101');
--drop function test_fun_6;

--test user defined window function
drop table if exists date_dim;
create table date_dim(a int, d_date date,  d_date_sk int);
CREATE or replace FUNCTION vec_winagg_func_02 (integer) RETURNS  timestamp without time zone
AS 'select $1+d_date from date_dim   order by 1 limit 1 ;'
LANGUAGE SQL
IMMUTABLE WINDOW;

CREATE FUNCTION nth_value_def(val anyelement, n integer = 1) RETURNS anyelement
  LANGUAGE internal WINDOW IMMUTABLE STRICT AS 'nameout';
CREATE FUNCTION nth_value_def(val anyelement, n integer = 1) RETURNS anyelement
  LANGUAGE internal WINDOW IMMUTABLE STRICT AS 'nameout_1';
  
drop schema hw_function_p_1 cascade;
