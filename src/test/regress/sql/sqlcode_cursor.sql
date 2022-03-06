create schema hw_sqlcode;
set current_schema = hw_sqlcode;
/* ---------anonymous block------------ */
/* no exception */
DECLARE
    a int;
BEGIN
    RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

/* exception */
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

CREATE or replace procedure func1_1 IS
	--PRAGMA AUTONOMOUS_TRANSACTION;
	a int;
BEGIN
	a := 1/0;
EXCEPTION
    WHEN others THEN
		RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

DECLARE
    a int;
BEGIN
	func1_1();
	RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
/* commit rollback */
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		COMMIT;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		ROLLBACK;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

/* PRAGMA AUTONOMOUS_TRANSACTION; */
CREATE OR REPLACE FUNCTION func5() RETURN void
AS
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;
    a int;
BEGIN
    a := 1/0;
END;
/

DECLARE
    a int;
BEGIN
	func5();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CREATE OR REPLACE FUNCTION func5_1() RETURN void
AS
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;
    a int;
BEGIN
    RAISE NOTICE 'AUTONOMOUS_TRANSACTION SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
DECLARE
    a int;
BEGIN
	a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
		func5_1();
END;
/
CREATE or replace procedure func5_2 IS
	a int;
BEGIN
	a := 1/0;
EXCEPTION
    WHEN others THEN
		RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
DECLARE
    a int;
BEGIN
	func5_2();
	RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

/* CALL function */
CREATE OR REPLACE FUNCTION func7() RETURN void
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
END;
/

DECLARE
    a int;
BEGIN
	func7();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

/* RAISE ERROR */
DECLARE
    a int;
BEGIN
	RAISE sqlstate 'AA666';
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/


/* ---------function------------ */
/* no exception */
CREATE OR REPLACE FUNCTION func1() RETURN void
AS
BEGIN
    RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

CALL func1();

/* exception */
CREATE OR REPLACE FUNCTION func2() RETURN void
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func2();

/* commit rollback */
CREATE OR REPLACE FUNCTION func3() RETURN void
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		COMMIT;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func3();

CREATE OR REPLACE FUNCTION func4() RETURN void
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		ROLLBACK;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func4();

/* PRAGMA AUTONOMOUS_TRANSACTION; */
CREATE OR REPLACE FUNCTION func5() RETURN void
AS
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;
    a int;
BEGIN
    a := 1/0;
END;
/

CREATE OR REPLACE FUNCTION func6() RETURN void
AS
DECLARE
    a int;
BEGIN
	func5();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func6();

/* CALL function */
CREATE OR REPLACE FUNCTION func7() RETURN void
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
END;
/

CREATE OR REPLACE FUNCTION func8() RETURN void
AS
DECLARE
    a int;
BEGIN
	func7();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func8();
/* RAISE ERROR */
CREATE OR REPLACE FUNCTION func9() RETURN void
AS
DECLARE
    a int;
BEGIN
	RAISE sqlstate 'AA666';
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL func9();


/* ---------PROCEDURE------------ */
/* no exception */
CREATE OR REPLACE PROCEDURE proc1()
AS
BEGIN
    RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/

CALL proc1();

/* exception */
CREATE OR REPLACE PROCEDURE proc2()
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc2();

/* commit rollback */
CREATE OR REPLACE PROCEDURE proc3()
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		COMMIT;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc3();

CREATE OR REPLACE PROCEDURE proc4()
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
		ROLLBACK;
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc4();

/* PRAGMA AUTONOMOUS_TRANSACTION; */
CREATE OR REPLACE PROCEDURE proc5()
AS
DECLARE
	PRAGMA AUTONOMOUS_TRANSACTION;
    a int;
BEGIN
    a := 1/0;
END;
/

CREATE OR REPLACE PROCEDURE proc6()
AS
DECLARE
    a int;
BEGIN
	proc5();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc6();

/* CALL function */
CREATE OR REPLACE PROCEDURE proc7()
AS
DECLARE
    a int;
BEGIN
    a := 1/0;
END;
/

CREATE OR REPLACE PROCEDURE proc8()
AS
DECLARE
    a int;
BEGIN
	proc7();
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc8();
/* RAISE ERROR */
CREATE OR REPLACE PROCEDURE proc9()
AS
DECLARE
    a int;
BEGIN
	RAISE sqlstate 'AA666';
EXCEPTION
    WHEN DIVISION_BY_ZERO THEN
        RAISE NOTICE 'SQLSTATE = %, SQLCODE = %, SQLERRM = %',SQLSTATE,SQLCODE,SQLERRM;
END;
/
CALL proc9();

DROP SCHEMA hw_sqlcode CASCADE;

create schema hw_cursor_state;
set current_schema = hw_cursor_state;
set behavior_compat_options='COMPAT_CURSOR';
/* ---------anonymous block------------ */

/*create*/
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
create table tb_test(col1 int);
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/* select */
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
select 1 into v_count;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/* insert */
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
insert into tb_test select 1;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/* update */
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
update tb_test set col1=2;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/* delete */
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
delete from tb_test;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/


/*Same layer*/

CREATE OR REPLACE PROCEDURE proc_test1()
as
v_count int;
BEGIN
v_count := 1;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

CREATE OR REPLACE PROCEDURE proc_test2()
as
v_count int;
BEGIN
v_count := 1;
update tb_test set col1=2;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
proc_test2();
proc_test1();
end
/

/*EXCEPTION*/
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
update tb_test11 set col1=2;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
EXCEPTION
when others then
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
update tb_test11 set col1=2;
EXCEPTION
when others then
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/*COMMIT ROLLBACK*/
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
update tb_test set col1=2;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
COMMIT;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
update tb_test set col1=2;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
ROLLBACK;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/* PRAGMA AUTONOMOUS_TRANSACTION */
CREATE OR REPLACE PROCEDURE proc_test()
as
DECLARE
v_count int;
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
v_count := 1;
update tb_test set col1=2;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/

/*drop*/
CREATE OR REPLACE PROCEDURE proc_test()
as
v_count int;
BEGIN
v_count := 1;
drop table tb_test;
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

DECLARE
v_count int;
BEGIN
v_count := 1;
proc_test();
RAISE NOTICE '%',v_count||','||SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end
/
create table staff(id int, name varchar2(10));
insert into staff values(1, 'xiaoming1');
insert into staff values(2, 'xiaoming2');
insert into staff values(3, 'xiaoming');
insert into staff values(4, 'xiaoming4');

CREATE OR REPLACE FUNCTION fun_cursor1() return void AS
DECLARE
BEGIN
insert into staff values(3, 'xiaoming');
dbe_output.print_line('cursor after insert');
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
SAVEPOINT my_savepoint;
dbe_output.print_line('cursor after savepoint');
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
update staff set name = 'wdc1' where id = 1;
dbe_output.print_line('cursor after update');
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
ROLLBACK TO SAVEPOINT my_savepoint;
dbe_output.print_line('cursor after rollback to savepoint');
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

CREATE OR REPLACE PROCEDURE fun_cursor2() AS
DECLARE
BEGIN
fun_cursor1();
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
IF SQL%FOUND THEN
dbe_output.print_line('cursor effective');
END IF;
delete from staff where id = 3;
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

call fun_cursor2();

CREATE OR REPLACE FUNCTION fun_cursor1() return void AS
DECLARE
BEGIN
insert into staff values(3, 'xiaoming');
update staff set name = 'zcna' where id = 1;
--commit;
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
RAISE division_by_zero;
end;
/

CREATE OR REPLACE PROCEDURE fun_cursor2() AS
DECLARE
BEGIN
fun_cursor1();
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
IF SQL%FOUND THEN
dbe_output.print_line('cursor effective');
END IF;
delete from staff where id = 3;
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
EXCEPTION
WHEN division_by_zero THEN
RAISE NOTICE 'test:% ... %',SQLCODE,SQLSTATE;
RAISE NOTICE '%',SQL%FOUND ||','||SQL%NOTFOUND ||','||SQL%ISOPEN || ',' || SQL%ROWCOUNT;
end;
/

call fun_cursor2();

set behavior_compat_options = '';
DROP SCHEMA hw_cursor_state CASCADE;


