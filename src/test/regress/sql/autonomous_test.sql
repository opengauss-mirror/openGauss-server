/************************************************************************/
/*                        anonymous block                               */
/* Anonymous block: Only top-level anonymous block autonomous           */
/* transaction declaration is supported, and anonymous block nesting    */
/* is not supported.                                                    */
/************************************************************************/
/* Normal anonymous block printing */
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
	res int;
	res2 text;
BEGIN
	dbe_output.print_line('just use call.');
	res2 := 'aa55';
	res := 55;
END;
/

create table t1(a int ,b text);

DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	dbe_output.print_line('just use call.');
	insert into t1 values(1,'you are so cute!');
END;
/

select * from t1;

/* 
 * Start a transaction, which is an autonomous transaction anonymous block,
 * Transactions are rolled back, and anonymous blocks are not rolled back.
 */
truncate table t1;

START TRANSACTION;
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	dbe_output.print_line('just use call.');
	insert into t1 values(1,'you are so cute,will commit!');
END;
/
insert into t1 values(1,'you will rollback!');
rollback;

select * from t1;


/* 
 * The external anonymous block is a common anonymous block, 
 * and the internal anonymous block is an autonomous transactional anonymous block,
 * Transaction rollback and anonymous block rollback
 */
truncate table t1;
DECLARE 
	--PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	DECLARE 
		PRAGMA AUTONOMOUS_TRANSACTION;
	BEGIN
		dbe_output.print_line('just use call.');
		insert into t1 values(1,'can you rollback!');
	END;
	insert into t1 values(2,'I will rollback!');
	rollback;
END;
/

select * from t1;

drop table if exists t1;

/*
 * Anonymous directly executes autonomy and throws an exception.
 */
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
	res int := 0;
	res2 int := 1;
BEGIN
	dbe_output.print_line('just use call.');
	res2 = res2/res;
END;
/

/*
 * An anonymous block is caught after an exception is thrown during execution.
 */
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
	res int := 0;
	res2 int := 1;
BEGIN
	dbe_output.print_line('just use call.');
	res2 = res2/res;
EXCEPTION
	WHEN division_by_zero THEN
		dbe_output.print_line('autonomous throw exception.');
END;
/


/************************************************************************/
/*                              PROCEDURE                               */
/************************************************************************/
/* text return value */
CREATE OR REPLACE function autonomous_2(proc_name VARCHAR2(10)) return text  AS 
DECLARE 
res2 text;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	res2 := 'aa55';
        return res2;
END;
/
select autonomous_2('');

/* record return value */
CREATE OR REPLACE function autonomous_3(out res int) return text  AS 
DECLARE 
        res2 text;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	res2 := 'aa55';
	res := 55;
        return res2;
END;
/
select autonomous_3();

/*
 * The main transaction invokes the autonomous transaction,
 * The main transaction is rolled back, and the autonomous transaction is not rolled back.
 */
create table t2(a int, b int);
insert into t2 values(1,2);
select * from t2;

CREATE OR REPLACE PROCEDURE autonomous_4(a int, b int)  AS 
DECLARE 
	num3 int := a;
	num4 int := b;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t2 values(num3, num4); 
	dbe_output.print_line('just use call.');
END;
/
CREATE OR REPLACE PROCEDURE autonomous_5(a int, b int)  AS 
DECLARE 
BEGIN
	dbe_output.print_line('just no use call.');
	insert into t2 values(666, 666);
	autonomous_4(a,b);
	rollback;
END;
/
select autonomous_5(11,22);
select * from t2;

/*
 * The main transaction invokes the autonomous transaction,
 * The autonomous transaction is rolled back. The main transaction is not rolled back.
 */
truncate table t2;
select * from t2;

CREATE OR REPLACE PROCEDURE autonomous_6(a int, b int)  AS 
DECLARE 
	num3 int := a;
	num4 int := b;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t2 values(num3, num4); 
	dbe_output.print_line('just use call.');
	rollback;
END;
/
CREATE OR REPLACE PROCEDURE autonomous_7(a int, b int)  AS 
DECLARE 
BEGIN
	dbe_output.print_line('just no use call.');
	insert into t2 values(666, 666);
	autonomous_6(a,b);
END;
/

select autonomous_7(11,22);
select * from t2;

drop table if exists t2;

/*
 * An autonomous transaction exception occurs. After an exception is thrown,
 * The main transaction can receive an exception.
 */
CREATE OR REPLACE PROCEDURE autonomous_8()  AS 
DECLARE 
	a int := 0;
	b int := 1;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	b := b/a;
END;
/
select autonomous_8();

CREATE OR REPLACE PROCEDURE autonomous_9()  AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	autonomous_8();
EXCEPTION
	WHEN division_by_zero THEN
		dbe_output.print_line('autonomous throw exception.');
END;
/

select autonomous_9();


/*
 * Autonomous transaction exception. After the exception is captured,
 * The main transaction no longer catches exceptions.
 */

CREATE OR REPLACE PROCEDURE autonomous_10()  AS 
DECLARE 
	a int := 0;
	b int := 1;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	b := b/a;
EXCEPTION
	WHEN division_by_zero THEN
		dbe_output.print_line('inner autonomous exception.');
END;
/
select autonomous_10();

CREATE OR REPLACE PROCEDURE autonomous_11()  AS 
DECLARE 
BEGIN
	autonomous_10();
EXCEPTION
	WHEN division_by_zero THEN
		dbe_output.print_line('autonomous throw exception.');
END;
/

select autonomous_11();

/*
 * The main transaction is abnormal. After the exception is captured,
 * Invoke the autonomous transaction and roll back the main transaction,
 * Autonomous transactions are not rolled back.
 */
 
create table t3(a int , b int ,c text);
select * from t3;

CREATE OR REPLACE PROCEDURE autonomous_12(a int ,b int ,c text)  AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t3 values(a, b, c);
END;
/

CREATE OR REPLACE PROCEDURE autonomous_13()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	b := b/a;
EXCEPTION
	WHEN division_by_zero THEN
		autonomous_12(a, b, sqlerrm);
		rollback;
END;
/

select autonomous_13();
select * from t3;

CREATE OR REPLACE PROCEDURE autonomous_14()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	autonomous_12(a, b, 'Pre-computing storage');
	insert into t3 values(a, b, 'i will roll back');
	b := b/a;
	autonomous_12(999, 999, 'you can not reach here,hehehe!');
EXCEPTION
	WHEN division_by_zero THEN
		autonomous_12(a, b, sqlerrm);
		rollback;
END;
/

select autonomous_14();
select * from t3;



/*
 * recursion
 */

CREATE OR REPLACE PROCEDURE autonomous_15(a int ,b int ,c text)  AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t3 values(a, b, c);
	dbe_output.print_line('inner autonomous exception.');
	autonomous_15(a, b, c);
END;
/

select autonomous_15(1,2,'1111');
select * from t3;




/*truncate table t3;*/

/************************** commit/rollback statement*****************************************/
truncate table t3;

CREATE OR REPLACE PROCEDURE autonomous_16(a int ,b int ,c text)  AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t3 values(a, b, c);
END;
/

CREATE OR REPLACE PROCEDURE autonomous_17(a int ,b int ,c text)  AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t3 values(a, b, c);
	rollback;
END;
/

/* The main transaction is rolled back, but the autonomous transaction is not rolled back. */
CREATE OR REPLACE PROCEDURE autonomous_18()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	autonomous_16(a, b, 'Pre-computing storage');
	insert into t3 values(a, b, 'i will roll back,you can not catch me');
	rollback;
END;
/

select autonomous_18();
select * from t3;

/* The commit and rollback of the main transaction do not affect the autonomous transaction. */
CREATE OR REPLACE PROCEDURE autonomous_19()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	insert into t3 values(a, b, 'commit after me');
	commit;
	autonomous_16(a, b, 'Pre-computing storage');
	insert into t3 values(a, b, 'i will roll back,you can not catch me');
	rollback;
END;
/

select autonomous_19();
select * from t3;

CREATE OR REPLACE PROCEDURE autonomous_20()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	insert into t3 values(a, b, 'autonomous rollback after me,but i will commit');
	autonomous_17(a, b, 'Pre-computing storage');
	commit;
	insert into t3 values(a, b, 'i will roll back,you can not catch me');
	rollback;
END;
/

select autonomous_20();
select * from t3;

/* Main and Autonomous Transactions with Exceptions */
truncate table t3;

CREATE OR REPLACE PROCEDURE autonomous_21()  AS 
DECLARE 
	a int := 0;
	b int := 1;
BEGIN
	insert into t3 values(a, b, 'i will roll back');
	b := b/a;
EXCEPTION
	WHEN division_by_zero THEN
		autonomous_16(a, b, sqlerrm);
		insert into t3 values(a, b, 'i will roll back,you can not catch me');
		rollback;
END;
/

select autonomous_21();
select * from t3;



drop table if exists t3;

/************************************************************************/ 
/*                              function                                */
/************************************************************************/
create table t4(a int, b int, c text);


/*
 * Common autonomous transaction function
 */
CREATE OR REPLACE function autonomous_31(num1 text) RETURN int AS 
DECLARE 
	num3 int := 220;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	return num3;
END;
/

select autonomous_31('a222');


/*
 * The main transaction is rolled back, but the autonomous transaction is not rolled back.
 */

select * from t4;

CREATE OR REPLACE function autonomous_32(a int ,b int ,c text) RETURN int AS 
DECLARE 
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	insert into t4 values(a, b, c);
	return 1;
END;
/
CREATE OR REPLACE function autonomous_33(num1 int) RETURN int AS 
DECLARE 
	num3 int := 220;
	tmp int;
	PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
	num3 := num3/num1;
	return num3;
EXCEPTION
	WHEN division_by_zero THEN
		select autonomous_32(num3, num1, sqlerrm) into tmp;
		return 0;
END;
/

select autonomous_33(0);

select * from t4;

drop table if exists t4;

create table t5(a int,b text);
CREATE USER jim PASSWORD 'gauss_123';
SET SESSION AUTHORIZATION jim PASSWORD 'gauss_123';
DECLARE
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
dbe_output.print_line('just use call.');
insert into t5 values(1,'aaa');
END;
/

RESET SESSION AUTHORIZATION;

select * from t5;

drop table if exists t5;
DROP USER IF EXISTS jim CASCADE;

drop table if exists test1;
create table test1(c1 date);
INSERT INTO test1 VALUES (date '12-10-2010');

create or replace package datatype_test as
data1 date;
function datatype_test_func() return date;
procedure datatype_test_proc();
end datatype_test;
create or replace package body datatype_test as
function datatype_test_func() return date is
declare
data2 date;
PRAGMA AUTONOMOUS_TRANSACTION;
begin
select c1 into data2 from test1;
data1 = data2;
return(data1);
end;
procedure datatype_test_proc() is
declare
data2 date;
begin
select c1 into data2 from test1;
data1 = data2;
end;
end datatype_test;
/

select datatype_test.datatype_test_func();

create table test_in (id int,a date);

CREATE OR REPLACE FUNCTION autonomous_test_in_f_1(num1  int) RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
EXECUTE 'INSERT INTO test_in VALUES (' || num1::text || ',sysdate)';
EXECUTE 'select pg_sleep(' || num1::text || ')';
RETURN num1;
END;
$$;
declare begin
select autonomous_test_in_f_1(0);
insert into test_in values (2,1);
end;
/

select count(*) from pg_stat_activity where application_name = 'autonomoustransaction';

drop table if exists test_in;

CREATE OR REPLACE FUNCTION autonomous_f_064(num1 int) RETURNS integer     
LANGUAGE plpgsql AS $$
DECLARE
BEGIN
DECLARE PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
insert into test_in values(num1,sysdate);
END;
RETURN num1;
END;
$$;

CREATE OR REPLACE PROCEDURE autonomous_p_086(num1 int,out num2 int)
AS
DECLARE
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
num2:=num1*10;
END;
/

SELECT * FROM autonomous_p_086(10);

/************************************************************************/ 
/*                              package value                           */
/************************************************************************/
-- 1. own package var
create or replace package pck1 as
type r1 is record(a int, b int);
va int;
vb r1;
vc varchar2(20);
procedure p1;
procedure p2;
end pck1;
/

create or replace package body pck1 as
vd int;
ve r1;
vf varchar2(20);
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 'before auto';
vd := 4;
ve := (5,6);
vf := 'before auto';
raise info 'before auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
p2();
raise info 'after auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
va := 11;
vb := (22,33);
vc := 'after auto';
vd := 44;
ve := (55,68);
vf := 'after auto';
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;

-- 2. another package var
create or replace package pck2 as
type r1 is record(a int, b int);
va int;
vb r1;
vc varchar2(20);
end pck2;
/

create or replace package pck1 as
procedure p1;
procedure p2;
end pck1;
/

create or replace package body pck1 as
procedure p1 as
begin
pck2.va := 1;
pck2.vb := (2,3);
pck2.vc := 'before auto';
raise info 'before auto: %, %, %', pck2.va,pck2.vb,pck2.vc;
p2();
raise info 'after auto: %, %, %', pck2.va,pck2.vb,pck2.vc;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %', pck2.va,pck2.vb,pck2.vc;
pck2.va := 11;
pck2.vb := (22,33);
pck2.vc := 'after auto';
end;
end pck1;
/

call pck1.p1();

-- 3. test auto alter package not exist in main session
create or replace package pck2 as
type r1 is record(a int, b int);
va int;
vb r1;
vc varchar2(20);
procedure p1;
end pck2;
/

create or replace package  body pck2 as
procedure p1 as
begin
raise info 'pck2 value: %, %, %', va, vb, vc;
end;
end pck2;
/

create or replace procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %', pck2.va,pck2.vb,pck2.vc;
pck2.va := 11;
pck2.vb := (22,33);
pck2.vc := 'after auto';
end;
/

create or replace procedure p1 as
begin
p2();
end;
/

-- need reload session
call p1();
call pck2.p1();

DROP PROCEDURE p1;
DROP PROCEDURE p2;
DROP PACKAGE pck2;

-- 4. test nested auto
-- (a) p1<->p3
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/


create or replace package pck1 as
va int;
type r1 is record(a int, b int);
vb r1;
procedure p1;
procedure p2;
procedure p3;
end pck1;
/

create or replace package body pck1 as
vc int;
vd r1;
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 4;
vd := (5,6);
pck2.va := 7;
pck2.vb := 'before auto';
raise info 'pck1 value(before auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(before auto): %, %', pck2.va, pck2.vb;
p2();
raise info 'pck1 value(after auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after auto): %, %', pck2.va, pck2.vb;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3();
end;

procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in auto): %, %', pck2.va, pck2.vb;
va := 11;
vb := (22,33);
vc := 44;
vd := (55,66);
pck2.va := 77;
pck2.vb := 'after auto';
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- (b) p2<->p3
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/


create or replace package pck1 as
va int;
type r1 is record(a int, b int);
vb r1;
procedure p1;
procedure p2;
procedure p3;
end pck1;
/

create or replace package body pck1 as
vc int;
vd r1;
procedure p1 as
begin

p2();
raise info 'pck1 value(after auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after auto): %, %', pck2.va, pck2.vb;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
va := 1;
vb := (2,3);
vc := 4;
vd := (5,6);
pck2.va := 7;
pck2.vb := 'before auto';
raise info 'pck1 value(in p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p2): %, %', pck2.va, pck2.vb;
p3();
raise info 'pck1 value(after p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p3): %, %', pck2.va, pck2.vb;
end;

procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p3): %, %', pck2.va, pck2.vb;
va := 11;
vb := (22,33);
vc := 44;
vd := (55,66);
pck2.va := 77;
pck2.vb := 'after auto';
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- 5. test table of and array value
-- (a) array of int;
create or replace package pck1 as
type t1 is varray(10) of int;
type t2 is table of int;
type t3 is table of int index by varchar2(10);
va t1;
vb t2;
vc t3;
procedure p1;
procedure p2;
end pck1;
/

create or replace package body pck1 as
vd t1;
ve t2;
vf t3;
procedure p1 as
begin
va(1) := 1;
va(2) := 2;
vb(1) := 3;
vb(2) := 4;
vc('a') := 5;
vc('aa') := 6;
vd(1) := 1;
vd(2) := 2;
ve(1) := 3;
ve(2) := 4;
vf('a') := 5;
vf('aa') := 6;
raise info 'before auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
p2();
raise info 'after auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
va.delete;
vb(1) := 33;
vb(2) := 44;
vc('a') := 55;
vc('aa') := 66;
vd(1) := 11;
vd(2) := 22;
ve.delete;
vf.delete;
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;

-- (b) array of record;
create or replace package pck1 as
type r1 is record  (a int, b int);
type t1 is varray(10) of r1;
type t2 is table of r1;
type t3 is table of r1 index by varchar2(10);
va t1;
vb t2;
vc t3;
procedure p1;
procedure p2;
end pck1;
/

create or replace package body pck1 as
vd t1;
ve t2;
vf t3;
procedure p1 as
begin
va(1) := (1,1);
va(2) := (2,2);
vb(1) := (3,3);
vb(2) := (4,4);
vc('a') := (5,5);
vc('aa') := (6,6);
vd(1) := (1,1);
vd(2) := (2,2);
ve(1) := (3,3);
ve(2) := (4,4);
vf('a') := (5,5);
vf('aa') := (6,6);
raise info 'before auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
p2();
raise info 'after auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
va.delete;
vb(1).a := 33;
vb(2) := (44,44);
vc('a').a := 55;
vc('aa') := (66,66);
vd(1).a := 11;
vd(2) := (22,22);
ve.delete;
vf.delete;
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;

-- 5. procedure with errors
-- (a) p2 with error
create or replace package pck1 as
type r1 is record(a int, b int);
va int;
vb r1;
vc varchar2(20);
procedure p1;
procedure p2;
end pck1;
/

create or replace package body pck1 as
vd int;
ve r1;
vf varchar2(20);
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 'before auto';
vd := 4;
ve := (5,6);
vf := 'before auto';
raise info 'before auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
p2();
raise info 'after auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in auto: %, %, %, %, %, %', va,vb,vc,vd,ve,vf;
va := 11;
vb := (22,33);
vc := 'after auto';
vd := 44;
ve := (55,68);
vf := 'after auto';
va := 3/0;
end;
end pck1;
/

call pck1.p1();

declare
begin
raise info 'current pck1 value: %, %, %,', pck1.va,pck1.vb,pck1.vc;
end;
/

DROP PACKAGE pck1;

-- (b) p3 with error
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/


create or replace package pck1 as
va int;
type r1 is record(a int, b int);
vb r1;
procedure p1;
procedure p2;
procedure p3;
end pck1;
/

create or replace package body pck1 as
vc int;
vd r1;
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 4;
vd := (5,6);
pck2.va := 7;
pck2.vb := 'before auto';
raise info 'pck1 value(before auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(before auto): %, %', pck2.va, pck2.vb;
p2();
raise info 'pck1 value(after auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after auto): %, %', pck2.va, pck2.vb;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3();
end;

procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in auto): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in auto): %, %', pck2.va, pck2.vb;
va := 11;
vb := (22,33);
vc := 44;
vd := (55,66);
pck2.va := 77;
pck2.vb := 'after auto';
va := 3/0;
end;
end pck1;
/

call pck1.p1();

declare
begin
raise info 'current pck1 value: %, %, ', pck1.va,pck1.vb;
raise info 'current pck2 value: %, %, ', pck2.va,pck2.vb;
end;
/

DROP PACKAGE pck2;
DROP PACKAGE pck1;

-- 6. multi auto procedure
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/


create or replace package pck1 as
va int;
type r1 is record(a int, b int);
vb r1;
procedure p1;
procedure p2;
procedure p3;
end pck1;
/

create or replace package body pck1 as
vc int;
vd r1;
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 4;
vd := (5,6);
pck2.va := 7;
pck2.vb := 'before auto';
raise info 'pck1 value(before p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(before p2): %, %', pck2.va, pck2.vb;
p2();
raise info 'pck1 value(after p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p2): %, %', pck2.va, pck2.vb;
p3();
raise info 'pck1 value(after p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p3): %, %', pck2.va, pck2.vb;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p2): %, %', pck2.va, pck2.vb;
va := 11;
vb := (22,33);
vc := 44;
vd := (55,66);
pck2.va := 77;
pck2.vb := 'after auto';
end;

procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p3): %, %', pck2.va, pck2.vb;
va := 111;
vb := (222,333);
vc := 444;
vd := (555,666);
pck2.va := 777;
pck2.vb := 'after p3';
end;
end pck1;
/

call pck1.p1();

DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- 6. multi nested auto procedure
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/


create or replace package pck1 as
va int;
type r1 is record(a int, b int);
vb r1;
procedure p1;
procedure p2;
procedure p3;
procedure p4;
end pck1;
/

create or replace package body pck1 as
vc int;
vd r1;
procedure p1 as
begin
va := 1;
vb := (2,3);
vc := 4;
vd := (5,6);
pck2.va := 7;
pck2.vb := 'before auto';
raise info 'pck1 value(before p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(before p2): %, %', pck2.va, pck2.vb;
p2();
raise info 'pck1 value(after p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p2): %, %', pck2.va, pck2.vb;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p2): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p2): %, %', pck2.va, pck2.vb;
va := 11;
vb := (22,33);
p3();
raise info 'pck1 value(after p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p3): %, %', pck2.va, pck2.vb;
p4();
raise info 'pck1 value(after p4): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(after p4): %, %', pck2.va, pck2.vb;
end;

procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p3): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p3): %, %', pck2.va, pck2.vb;
va := 111;
vb := (222,333);
vc := 444;
vd := (555,666);
pck2.va := 777;
pck2.vb := 'after p3';
end;

procedure p4 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1 value(in p4): %, %, %, %', va, vb, vc, vd;
raise info 'pck2 value(in p4): %, %', pck2.va, pck2.vb;
va := 1111;
vb := (2222,3333);
vc := 4444;
vd := (5555,6666);
pck2.va := 7777;
pck2.vb := 'after p4';
end;
end pck1;
/

call pck1.p1();


DROP PACKAGE pck1;
DROP PACKAGE pck2;

-- 7. test with out package
create or replace procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'now in p2';
end;
/

create or replace procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'now in p3';
end;
/

create or replace procedure p1 as
begin
raise info 'now in p1';
p2();
p3();
end;
/

call p1();

DROP PROCEDURE p1;
DROP PROCEDURE p2;
DROP PROCEDURE p3;

-- 7. test only p3 call package
create or replace package pck2 as
va int;
vb varchar2(20);
end pck2;
/

create or replace procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
pck2.va := 3;
pck2.vb := 'assign in p3';
end;
/

create or replace procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
p3;
end;
/

create or replace procedure p1 as
begin
raise info 'pck2 value(in p1): %, %', pck2.va, pck2.vb;
p2();
raise info 'pck2 value(after p2): %, %', pck2.va, pck2.vb;
end;
/

call p1();

DROP PROCEDURE p1;
DROP PROCEDURE p2;
DROP PROCEDURE p3;
DROP PACKAGE pck2;

-- 8. multi auto procedure (session will reuse)
create or replace package pck1 as
type r1 is record(a int, b int);
va int;
procedure p1;
procedure p2;
procedure p3;
end pck1;
/

create or replace package body pck1 as
procedure p1 as
begin
va := 1;
raise info 'before p2: %', va;
p2();
raise info 'after p2: %', va;
va := 123;
p3();
raise info 'after p3: %', va;
end;

procedure p2 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in p2: %', va;
va := 11;
end;
procedure p3 as
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'in p3: %', va;
end;
end pck1;
/

call pck1.p1();

drop package pck1;

-- 9. autosesion call another auto procedure
create or replace package pkg1 IS
va int:=1;
function f1(num1 int) return int;
end pkg1;
/

create or replace package body pkg1 as
function f1(num1 int) return int
is
declare
PRAGMA AUTONOMOUS_TRANSACTION;
re_int int;
begin
raise notice 'just in f1, pkg.va:%',va;
va:=va+num1;
raise notice 'in f1, pkg.va:%',va;
re_int = 1;
return re_int;
end;
end pkg1;
/

create or replace function f2(num1 int) return int
is
declare PRAGMA AUTONOMOUS_TRANSACTION;
re_int int;
begin
pkg1.va = 111;
raise notice 'before f1: pkg.va: %',pkg1.va;
re_int = pkg1.f1(num1);
raise notice 'after f1: pkg.va: %', pkg1.va;
return re_int;
end;
/

select f2(10);

drop function f2;
drop package pkg1;

-- 9. autosesion call another nomarl procedure
create or replace package pkg1 IS
va int:=1;
function f1(num1 int) return int;
end pkg1;
/

create or replace package body pkg1 as
function f1(num1 int) return int
is
declare
re_int int;
begin
raise notice 'just in f1, pkg.va:%',va;
va:=va+num1;
raise notice 'in f1, pkg.va:%',va;
re_int = 1;
return re_int;
end;
end pkg1;
/

create or replace function f2(num1 int) return int
is
declare PRAGMA AUTONOMOUS_TRANSACTION;
re_int int;
begin
pkg1.va = 111;
raise notice 'before f1: pkg.va: %',pkg1.va;
re_int = pkg1.f1(num1);
raise notice 'after f1: pkg.va: %', pkg1.va;
return re_int;
end;
/

select f2(10);

drop function f2;
drop package pkg1;

-- auto procedure call normal procedure (auto procedure without package)
create or replace package autonomous_pkg_setup IS
count_public int:=1;
end autonomous_pkg_setup;
/
create or replace package body autonomous_pkg_setup as
  count_private int :=1;
end autonomous_pkg_setup;
/

create or replace procedure out_015(num1 int)
is
declare
va int:=30;
re_int int;
begin
autonomous_pkg_setup.count_public = autonomous_pkg_setup.count_public + va;
raise info 'in out_015,autonomous_pkg_setup.count_public:%', autonomous_pkg_setup.count_public;
end;
/
create or replace procedure app015_1()
is
declare PRAGMA AUTONOMOUS_TRANSACTION;
begin
out_015(1);
end;
/

call app015_1();
call app015_1();

drop procedure app015_1;
drop procedure out_015;
drop package autonomous_pkg_setup;

-- 10. package var same name with function param
create or replace package pck1 IS
va int:=1;
procedure p1(va int,vb int);
end pck1;
/
create or replace package body pck1 as
vb int :=1;
procedure p1(va int,vb int)
is
declare 
PRAGMA AUTONOMOUS_TRANSACTION;
begin
va:=pck1.va+va;
pck1.vb:=pck1.vb+vb;
raise info 'in p1, va : %', va;
raise info 'in p1, vb : %', vb;
raise info 'in p1, pck1.va : %', pck1.va;
raise info 'in p1, pck1.vb : %', pck1.vb;
end;
begin
va := 2;
vb := 2;
end pck1;
/

call pck1.p1(10,20);
call pck1.p1(10,20);

drop package pck1;

-- anoymous block with autonomous
create or replace package pck1 as
type r1 is record(a int, b int);
va int;
vb r1;
vc varchar2(20);
end pck1;
/

declare
begin
pck1.va := 1;
pck1.vb := (2,3);
pck1.vc := 'before auto';
end;
/

-- (a) autonomous anoymous block 
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1.va: %',pck1.va;
raise info 'pck1.vb: %',pck1.vb;
raise info 'pck1.vc: %',pck1.vc;
pck1.va := 11;
pck1.vb := (22,33);
pck1.vc := 'after auto';
end;
/

declare
begin
raise info 'pck1.va: %',pck1.va;
raise info 'pck1.vb: %',pck1.vb;
raise info 'pck1.vc: %',pck1.vc;
pck1.va := 111;
pck1.vb := (222,333);
pck1.vc := 'before after auto';
end;
/

-- (b) autonomous anoymous block with error
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
raise info 'pck1.va: %',pck1.va;
raise info 'pck1.vb: %',pck1.vb;
raise info 'pck1.vc: %',pck1.vc;
pck1.va := 1111;
pck1.vb := (2222,3333);
pck1.vc := 'after after auto';
pck1.va := 3/0;
end;
/

declare
begin
raise info 'pck1.va: %',pck1.va;
raise info 'pck1.vb: %',pck1.vb;
raise info 'pck1.vc: %',pck1.vc;
end;
/