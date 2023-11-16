create database pl_test_cursor_part1 DBCOMPATIBILITY 'pg';
\c pl_test_cursor_part1;

create schema hw_cursor_part1;
set current_schema = hw_cursor_part1;
set behavior_compat_options = 'skip_insert_gs_source';
create table company(name varchar(100), loc varchar(100), no integer);

insert into company values ('macrosoft',    'usa',          001);
insert into company values ('oracle',       'usa',          002);
insert into company values ('backberry',    'canada',       003);
insert into company values ('sumsung',      'south korea',  004);
insert into company values ('tencent',      'china',        005);
insert into company values ('ibm',          'usa',          006);
insert into company values ('nokia',        'finland',      007);
insert into company values ('apple',        'usa',          008);
insert into company values ('sony',         'japan',        009);
insert into company values ('baidu',        'china',        010);
insert into company values ('kingsoft',     'china',        011);

--1 test explicit cursor without args
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        raise notice '%','c1_all%isopen=false: c1_all closed ';
        open c1_all;
        raise notice '%','c1_all opened ';
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c1_all%isopen then
        raise notice '%','c1_all%isopen=true: c1_all opened ';
        close c1_all;
        raise notice '%','c1_all closed ';
    end if;
end;
/
call test_cursor_1();
drop procedure test_cursor_1;

--2 test explicit cursor with args
create or replace procedure test_cursor_2 
as 
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    if not c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=false: c2_no_range closed';
        open c2_no_range(5,10);
        raise notice '%','c2_no_range opened';
    end if;
    loop
        fetch c2_no_range into company_name, company_loc, company_no;
        exit when c2_no_range%notfound;
        raise notice '% : % : %',company_name,company_loc,company_no;
    end loop;
    if c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=true: c2_no_range opened';
        close c2_no_range;
        raise notice '%','c2_no_range closed';
    end if;
end;
/
call test_cursor_2();
drop procedure test_cursor_2;

--3 test explicit cursor attributes
create or replace procedure test_cursor_3
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        raise notice '%','c1_all%isopen=false: c1_all closed';
        open c1_all;
        raise notice '%','c1_all opened';
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        if c1_all%notfound then
            raise notice '%','c1_all%notfound=true: no rows selected';
            exit;
        end if;
        if c1_all%found then
            raise notice 'rows_%: %  %  %',c1_all%rowcount,company_name,company_loc,company_no;
        end if;
    end loop;
    if c1_all%isopen then
        raise notice '%','c1_all%isopen=true: c1_all opened';
        close c1_all;
        raise notice '%','c1_all closed';
    end if;
end;
/
call test_cursor_3();
drop procedure test_cursor_3;

--4 test implicit cursor attributes: (sql%)%found,%notfound,%isopen,%rowcount
create or replace procedure test_cursor_4
as
begin 
    delete from company where loc='china';
    if sql%isopen then --sql%isopen always false, as closed after the execution of sql.
        raise notice '%','sql%isopen=true: implicit opened!';
    end if;
    if not sql%isopen then 
        raise notice '%','sql%isopen=false: implicit closed!';
    end if;
    if sql%found then
        raise notice 'sql%%found=true: % rows',sql%rowcount;
    end if;
    if sql%notfound then
        raise notice '%','sql%found=false: no rows found!';
    end if;
end;
/
call test_cursor_4();
drop procedure test_cursor_4;

--5 test dynamic cursor: (weak type)without return
create or replace procedure test_cursor_5()
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    type ref_cur_type is ref cursor; --declare cursor type
        my_cur ref_cur_type; --declare cursor var
    sqlstr varchar(100);
begin 
    sqlstr := 'select name,loc,no from company where loc=:1 order by 1, 2, 3';
    open my_cur for 'select name,loc,no from company order by 1, 2, 3';
    fetch my_cur into company_name,company_loc,company_no;
    while my_cur%found loop
    raise notice '% : % : %',company_name,company_loc,company_no;
        fetch my_cur into company_name,company_loc,company_no;
    end loop;
    close my_cur;
end;
/
call test_cursor_5();
drop procedure test_cursor_5;

----6 test more than one cursors access	
create or replace procedure test_cursor_6
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;		
    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    open c1_all; 
    open c2_no_range(50,100); --result null 	
    fetch c1_all into company_name, company_loc, company_no;
    fetch c2_no_range into company_name, company_loc, company_no;
    if c1_all%found then
        raise notice '%','c1_all%found=true';
    end if;
    if c1_all%notfound then
        raise notice '%','c1_all%notfound=true';
    end if;
    if c2_no_range%found then
        raise notice '%','c2_no_range%found=true';
    end if;
    if c2_no_range%notfound then
        raise notice '%','c2_no_range%notfound=true';
    end if;
    raise notice 'c1_all%%rowcount=%',c1_all%rowcount;
    raise notice 'c2_no_range%%rowcount=%',c2_no_range%rowcount;
end;
/
call test_cursor_6();
drop procedure test_cursor_6;

create table tbl (id int);
insert into tbl values  (1);
insert into tbl values  (2);
insert into tbl values  (3);
insert into tbl values  (4);

create or replace procedure sp_testsp
as
    temp1       integer;
    temp2       integer;
    sql_str     varchar2(200);
begin
    declare
        type crs_recode_type is ref cursor;
        c1 crs_recode_type;
    begin
        temp1   := 4;
        temp2   := 0;
        sql_str := 'select id from tbl where id < :a and id > :b order by 1';
        open c1 for sql_str using in temp1, in temp2;
        loop
            fetch c1 into temp1;
            raise notice '%',temp1;
            raise notice '%',temp1;
            exit when c1%notfound; 
        end loop;
        close c1;
--test implicit cursor rowcount attribute
        select id into temp1 from tbl where id=2 order by 1;
        raise notice '%',sql%rowcount;
        update tbl set id=100 where id<3;
        raise notice '%',sql%rowcount;
        insert into tbl select * from tbl;
        raise notice '%',sql%rowcount;
        delete from tbl;
        raise notice '%',sql%rowcount;
    end;  
end;
/

call sp_testsp();

create table a3(id int, name varchar2(100));
insert into a3 values(1,'test');
insert into a3 values(2,'test');
create or replace function func_test(a in sys_refcursor,b in varchar2(100))
return varchar2(100) as
v_out varchar2(100);
v_a varchar2(100);
v_b varchar2(100);
begin
    raise notice 'dfdf+%',a%isopen;
    if a%isopen then
        v_out := '0';
		raise notice '%',a%isopen;
        if b = '1' then 
            fetch a into v_a,v_b;
            raise notice '%,%',v_a,v_b;
        end if;
    else
        v_out := 'x'; --not open
    end if;
    return v_out;
end;
/

--test cursor as an in parameter, its option can be changed in a function
create or replace procedure proc_test(i_b in varchar2(100),o_retcode out varchar2(100))
as
v_c sys_refcursor;
v_status varchar2(100);
begin 
    o_retcode := '0';
	v_status := func_test(v_c,i_b);
    open v_c for select id,name from a3 order by id;
    v_status := func_test(v_c,i_b);
	raise notice '%',v_c%isopen;
    raise notice '%',v_status;
    exception when others then
		o_retcode := '-1';
end;
/

call proc_test(1,'hello');

create or replace function func_test(a in sys_refcursor,b in number)
return number 
as
	v_out number;
	v_a number;
	v_b number;
begin
	if a%isopen then
                raise notice '%','open is true';
	end if;
	
    if a%isopen then
        v_out := 0;
                 raise notice '%','a is open';
        if b = 1 then 
            fetch a into v_a;
                        raise notice '%','fetch is execute';
        end if;
    end if;
    return v_out;
end;
/

create or replace procedure proc_test(i_b in number)
as
v_c sys_refcursor;
v_status int;
begin 
    open v_c for select id from a3;
	if (v_c%found) then
                raise notice '%','v_c found is true';
	end if;
	if (v_c%notfound) then
                raise notice '%','v_c not found is true';
	end if;
	if (v_c%rowcount = 0) then
                raise notice '%','v_c row count 0';
	end if;
    v_status := func_test(v_c,i_b);
	
	if (v_c%found) then
                raise notice '%','v_c found is true';
	end if;
	if (v_c%notfound) then
                raise notice '%','v_c not found is true';
	end if;
	if (v_c%rowcount = 1) then
                raise notice '%','v_c row count 1';
	end if;
end;
/

call proc_test(1);

create table sections(section_ID int);
insert into sections values(1);
insert into sections values(1);
insert into sections values(1);
insert into sections values(1);

CREATE OR REPLACE function proc_sys_ref()
return SYS_REFCURSOR
IS 
	C1 SYS_REFCURSOR;
 BEGIN
	OPEN C1 FOR SELECT section_ID FROM sections ORDER BY section_ID; 
	return C1; 
 END; 
 /

 DECLARE 
	 C1 SYS_REFCURSOR; 
	 TEMP NUMBER(4); 
 BEGIN 
	 c1 = proc_sys_ref();
	 if c1%isopen then
                 raise notice '%','ok';
	end if;
	
	 LOOP  
		 FETCH C1 INTO TEMP;   
                 raise notice '%',C1%ROWCOUNT;
		 EXIT WHEN C1%NOTFOUND; 
	 END LOOP;  
 END; 
 /
 
 drop function proc_sys_ref();
 
--test cursor as an out parameter, its option can be changed in a function
CREATE OR REPLACE PROCEDURE proc_sys_ref(O out SYS_REFCURSOR)
IS 
	C1 SYS_REFCURSOR;
 BEGIN
	OPEN C1 FOR SELECT section_ID FROM sections ORDER BY section_ID; 
	O := C1; 
 END; 
 /

 DECLARE 
	 C1 SYS_REFCURSOR; 
	 TEMP NUMBER(4); 
 BEGIN 
	 proc_sys_ref(C1);
	 if c1%isopen then
                 raise notice '%','ok';
	end if;
	
	 LOOP  
		 FETCH C1 INTO TEMP;   
                 raise notice '%',C1%ROWCOUNT;
		 EXIT WHEN C1%NOTFOUND; 
	 END LOOP;  
 END; 
 /
 
drop table TEST_TB;
CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);
DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	if CURS1%isopen then
                raise notice '%','curosr is open';
	end if;
	FOR VARA IN CURS1 LOOP
                raise notice '%',CURS1%ROWCOUNT;
	END LOOP;
	if not CURS1%isopen then
                raise notice '%','curosr is open';
	end if;
END;
/



--test multi cursor option
create or replace procedure Test_cursor_3
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;

	cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;

begin
	
	open c1_all;
	
    if not c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=false: c2_no_range closed';
        raise notice '%','c2_no_range opened';
    end if;	

    if c1_all%isopen then
        raise notice '%','c1_all%isopen=false: c1_all closed';
        raise notice '%','c1_all opened';
    end if;
	
	close c1_all;
	
	open c2_no_range(1,2);
	
    if c1_all%isopen then
        raise notice '%','c1_all%isopen=true: c1_all closed';
        raise notice '%','c1_all opened';
    end if;
	
    if  c2_no_range%isopen then
        raise notice '%','c2_no_range%isopen=true: c2_no_range opened';
    end if;	

	close c2_no_range;
end;
/

call Test_cursor_3();

--test return cursor or record
 drop table TEST_TB;
CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);
DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB order by 1;
 TEMP INTEGER:=0;
BEGIN
    if not CURS1%isopen then
                raise notice '%','curosr is open';
    end if;
    FOR VARA IN CURS1 LOOP
                raise notice '%',CURS1%ROWCOUNT;
    END LOOP;
    if not CURS1%isopen then
                raise notice '%','curosr is open';
    end if;
END;
/

--test return set
create or replace function func_test(a in sys_refcursor,b in number)
return setof number
as
	v_out number;
	v_a number;
	v_b number;
begin
    if a%isopen then
        v_out := 0;
                 raise notice '%','a is open';
        if b = 1 then 
            fetch a into v_a;
                        raise notice '%','fetch is execute';
        end if;
    end if;
end;
/

create or replace procedure proc_test(i_b in varchar2(100),o_retcode out varchar2(100))
as
v_c sys_refcursor;
v_status varchar2(100);
begin 
    o_retcode := '0';
    open v_c for select id,name from a3 limit 10;
    v_status := func_test(v_c,i_b);
	raise notice '%',v_c%isopen;
    raise notice '%',v_status;
end;
/

select proc_test('1');

--test dynamic anonimous
create table staffs(first_name VARCHAR2, phone_number  VARCHAR2, salary        NUMBER(8,2), section_id int);
insert into staffs values('first','12345',1234,30);

DECLARE
    name          VARCHAR2(20);
    phone_number  VARCHAR2(20);
    salary        NUMBER(8,2);
    sqlstr        VARCHAR2(1024);

    TYPE app_ref_cur_type IS REF CURSOR;  
    my_cur app_ref_cur_type;  
    
BEGIN
    sqlstr := 'select first_name,phone_number,salary from staffs
         where section_id = :1';
    OPEN my_cur FOR sqlstr USING '30'; 
    if (my_cur%isopen) then
                raise notice '%','cursor is open';
    end if;

    FETCH my_cur INTO name, phone_number, salary; 
    WHILE my_cur%FOUND LOOP
          raise notice '% # % # %',name,phone_number,salary;
          FETCH my_cur INTO name, phone_number, salary;
    END LOOP;
    CLOSE my_cur; 
END;
/		

CREATE OR REPLACE PROCEDURE proc_sys_ref(O in SYS_REFCURSOR, O2 out SYS_REFCURSOR)
IS 
	C1 SYS_REFCURSOR;
 BEGIN
	OPEN C1 FOR SELECT section_ID FROM sections ORDER BY section_ID; 
	O := C1; 
	O2 := C1;
 END; 
 /

DECLARE
    statement  VARCHAR2(200);
    param2     SYS_REFCURSOR;
	param1     SYS_REFCURSOR;
BEGIN
 
    statement := 'call proc_sys_ref(:col_2,:col_1)';
    EXECUTE IMMEDIATE statement
        USING IN  param2, out param1;
	if (param2%isopen) then
                raise notice '%','function call is ok';
     else
            raise notice '%','function call is wrong';
		
		
	end if;

	if (param1%isopen) then
                raise notice '%','param1 function call is ok';
     else
            raise notice '%','aram1 function call is wrong';
	end if;
END;
/

CREATE OR REPLACE PROCEDURE proc_sys_ref(O out SYS_REFCURSOR, b out SYS_REFCURSOR)
IS 
	C1 SYS_REFCURSOR;
 BEGIN
	OPEN C1 FOR SELECT section_ID FROM sections ORDER BY section_ID; 
	O := C1;
	b := C1;
 END; 
 /

 DECLARE 
	 C1 SYS_REFCURSOR; 
	 C2 SYS_REFCURSOR; 
	 TEMP NUMBER(4); 
 BEGIN 
	 proc_sys_ref(C1, c2);
	 if c1%isopen then
                 raise notice '%','ok';
	end if;
	
	 if c2%FOUND then
                 raise notice '%','ok';
	end if;
	
	 LOOP  
		 FETCH C1 INTO TEMP;   
                 raise notice '%',C1%ROWCOUNT;
		 EXIT WHEN C1%NOTFOUND; 
	 END LOOP;  
 END; 
 /
 
 create table rc_test(a int);
 
  CREATE OR REPLACE PROCEDURE proc_sys_ref2(O out SYS_REFCURSOR, b out SYS_REFCURSOR)
IS 
	C1 SYS_REFCURSOR;
 BEGIN
	OPEN C1 FOR SELECT a FROM rc_test;
	O := C1;
	b := C1;
 END; 
 /

  CREATE OR REPLACE PROCEDURE proc_sys_ref1(O out SYS_REFCURSOR, b out SYS_REFCURSOR)
IS 
	C1 SYS_REFCURSOR;
	TEMP number;
	 C2 SYS_REFCURSOR; 
	 C3 SYS_REFCURSOR; 
 BEGIN
	OPEN C1 FOR SELECT section_ID FROM sections ORDER BY section_ID;
	fetch c1 into temp;
	proc_sys_ref2(c2, c3);
	 if c2%isopen then
                 raise notice '%','ef1 ok';
	end if;
	O := C1;
	b := C1;
 END; 
 /

 
 DECLARE 
	 C1 SYS_REFCURSOR; 
	 C2 SYS_REFCURSOR; 
	 TEMP NUMBER(4); 
 BEGIN 
	 proc_sys_ref1(C1, c2);
	 if c1%isopen then
                 raise notice '%','ok';
	end if;
	
	 if c2%found then
                 raise notice '%','ok';
	end if;
	
	 LOOP  
		 FETCH C1 INTO TEMP;   
                 raise notice '%','ok';
		 EXIT WHEN C1%NOTFOUND; 
	 END LOOP;  
 END; 
 /

 --test inout SYS_REFCURSOR
CREATE OR REPLACE PROCEDURE proc_sys_ref1(O inout SYS_REFCURSOR)
IS
	TEMP number;
 BEGIN
	
	if (O%isopen) then
                raise notice '%','inparameter is open ok';
	else
                raise notice '%','inparameter is not open ok';
	end if;
	fetch O into temp;
	if (O%found) then
                raise notice '%','found is true';
	end if;
	close O;
 END; 
 /

  DECLARE
	C1 SYS_REFCURSOR;
    type ref_cur_type is ref cursor; --declare cursor type
        my_cur ref_cur_type; --declare cursor var
 BEGIN
	 OPEN my_cur FOR SELECT section_ID FROM sections ORDER BY section_ID;
	 c1 = proc_sys_ref1(my_cur);
	 if c1%isopen then
                 raise notice '%','c1 ok';
	end if;
	
	 if my_cur%isopen then
                 raise notice '%',' my_cur ok';
	end if;
 END; 
 /
 
CREATE OR REPLACE PROCEDURE proc_sys_ref1(c1 out SYS_REFCURSOR)
IS
	TEMP number;
	my_cur SYS_REFCURSOR;
 BEGIN
	OPEN my_cur FOR SELECT section_ID FROM sections ORDER BY section_ID;
	if (my_cur%isopen) then
                raise notice '%','cursor is open ok';
	else
                raise notice '%','cursor is not open ok';
	end if;
	fetch my_cur into temp;
	if (my_cur%found) then
                raise notice '%','found is true';
	end if;
	c1 = my_cur;
	close c1;
 END; 
 /

  DECLARE
	C1 SYS_REFCURSOR;
 BEGIN
	 c1 = proc_sys_ref1(c1);
	 if c1%isopen then
                 raise notice '%','c1 ok';
	else
                 raise notice '%','c1 is closed';
	end if;
 END; 
 /

create or replace function pro_base_003(refcursor, refcursor) returns setof refcursor as $$
declare
a int;
b text;
begin
drop table if exists pro_base_001;
create table pro_base_001(a int,b text );
insert into pro_base_001 values(1,'a');
insert into pro_base_001 values(2,'b');
open $2 for select * from pro_base_001 order by 1;
loop
fetch next from $2 into a,b;
EXIT WHEN $2%NOTFOUND;
end loop;
end;
$$ language plpgsql;

select * from  pro_base_003('a','b');

create table tb1(c1 int,c2 int);
insert into tb1 values(1,2);
insert into tb1 values(2,3);

create or replace procedure i_inout(refcur inout refcursor)
AS
DECLARE
v_csr refcursor;
a1 int;
begin 
open v_csr for refcur;
LOOP
fetch v_csr into a1;
exit when v_csr%notfound;
raise notice 'a1:%',a1;
end loop;
close v_csr;
end;
/

call i_inout(' select c1 from tb1;');

DECLARE
c sys_refcursor;
a1 int;
BEGIN 
c:=i_inout(' select c1 from tb1;');
LOOP
fetch c into a1;
exit when c%notfound;
raise notice '%',a1;
end loop;
close c;
end;
/

create table t1_refcursor(a int);
insert into t1_refcursor values (1);
insert into t1_refcursor values (2);
create or replace procedure p3_refcursor (c1 out sys_refcursor) as
va t1_refcursor;
i int;
begin
open c1 for select * from t1_refcursor;
i = 1/0;
exception 
when others then
    raise info '%', 'exception';
end;
/
select * from  p3_refcursor();

create or replace procedure p3 (c4 in int,c2 out int,c3 out int,c1 out sys_refcursor,cc2 out sys_refcursor) as
va t1_refcursor;
i int;
begin
begin
open cc2 for select * from t1_refcursor;
i = 1/0;
exception 
when others then
    raise info '%', 'exception2';
end;
open c1 for select * from t1_refcursor;
c3:=1;
c2:=2;
end;
/

select * from  p3(1);

START TRANSACTION;
CURSOR sc  FOR select * from generate_series(3, 13) i where i <> all (values (1),(2),(4));
MOVE FORWARD 10 IN sc;
FETCH BACKWARD FROM sc;
END;

 -- test return REFCURSOR
CREATE FUNCTION function0() 
RETURNS INT LANGUAGE PLPGSQL 
AS $$ 
DECLARE 
    variable3 REFCURSOR = 1;
BEGIN 
    RETURN variable3; 
END; 
$$;
 -- expect an error
SELECT function0();

DROP FUNCTION function0; 
CREATE FUNCTION function0() 
RETURNS REFCURSOR LANGUAGE PLPGSQL 
AS $$ 
DECLARE 
    variable3 REFCURSOR = 1;
BEGIN 
    RETURN variable3; 
END; 
$$;
 -- expect no error
select function0();

 -- clean up
DROP SCHEMA hw_cursor_part1 CASCADE;
\c regression;
drop database IF EXISTS pl_test_cursor_part1;
