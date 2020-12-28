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

--test explicit cursor without args
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_1();
drop procedure test_cursor_1;

--test explicit cursor with args
create or replace procedure test_cursor_2 
as 
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    if not c2_no_range%isopen then
        open c2_no_range(5,10);
    end if;
    loop
        fetch c2_no_range into company_name, company_loc, company_no;
        exit when c2_no_range%notfound;
    end loop;
    if c2_no_range%isopen then
        close c2_no_range;
        dbms_output.put_line('c2_no_range closed');	
    end if;
end;
/
call test_cursor_2();
drop procedure test_cursor_2;

--test explicit cursor attributes
create or replace procedure test_cursor_3
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        if c1_all%notfound then
            exit;
        end if;
        if c1_all%found then
        end if;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_3();
drop procedure test_cursor_3;

--test implicit cursor attributes: (sql%)%found,%notfound,%isopen,%rowcount
create or replace procedure test_cursor_4
as
begin 
    delete from company where loc='china';
    if sql%isopen then --sql%isopen always false, as closed after the execution of sql.
    end if;
    if not sql%isopen then 
    end if;
    if sql%found then
    end if;
    if sql%notfound then
    end if;
end;
/
call test_cursor_4();
drop procedure test_cursor_4;

--test dynamic cursor: (weak type)without return
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
        fetch my_cur into company_name,company_loc,company_no;
    end loop;
    close my_cur;
end;
/
call test_cursor_5();
drop procedure test_cursor_5;

----test more than one cursors access	
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
    end if;
    if c1_all%notfound then
    end if;
    if c2_no_range%found then
    end if;
    if c2_no_range%notfound then
    end if;
end;
/
call test_cursor_6();
drop procedure test_cursor_6;

drop table company;

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
            exit when c1%notfound; 
        end loop;
        close c1;
--test implicit cursor rowcount attribute
        select id into temp1 from tbl where id=2 order by 1;
        update tbl set id=100 where id<3;
        insert into tbl select * from tbl;
        delete from tbl;
    end;  
end;
/

call sp_testsp();
drop procedure sp_testsp;
drop table tbl;

------------------------------------------------------------------------------
-----test implicit cursor attributes for DML: select,insert,update,delete-----
------------------------------------------------------------------------------
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp
as
    v int:=0;
begin
    --select
    select v1 into v from t1 where v1=1; 
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    

    --insert
    insert into t1 values (4,'abc4');
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    dbms_output.put_line('test update: sql%rowcount=' || sql%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
 
    --delete
    delete from t1 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
end;
/
call sp_testsp();
drop procedure sp_testsp;
drop table t1;
------------------------------------------------------------------------------
-----support A db's cursor in or out params---------------------------------
------------------------------------------------------------------------------
CREATE TABLE TBL(VALUE INT);
INSERT INTO TBL VALUES (1);
INSERT INTO TBL VALUES (2);
INSERT INTO TBL VALUES (3);
INSERT INTO TBL VALUES (4);

CREATE OR REPLACE PROCEDURE TEST_SP
IS
    CURSOR C1(NO IN VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < NO ORDER BY 1;
    CURSOR C2(NO OUT VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < 10 ORDER BY 1;
    V INT;
    RESULT INT;
BEGIN
    OPEN C1(10);
    OPEN C2(RESULT);
    LOOP
    FETCH C1 INTO V; 
        IF C1%FOUND THEN 
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C1;
    
    LOOP
    FETCH C2 INTO V; 
        IF C2%FOUND THEN 
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C2;
END;
/
CALL  TEST_SP();
DROP TABLE TBL;
DROP PROCEDURE TEST_SP;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the implicit cursor use to explicit cursor attributes --
---------------------------------------------------------------------------------
drop table t1;
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select
    select v1 into v from t1 where v1=1;    
    if not cur%isopen then   
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    dbms_output.put_line('test update: cur%rowcount=' || cur%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the explicit cursor use to implicit cursor attributes --
---------------------------------------------------------------------------------
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select    
    select v1 into v from t1 where v1=1;    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then   
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    dbms_output.put_line('test update: sql%rowcount=' || sql%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE TBL(V1 INTEGER);
INSERT INTO TBL VALUES(1);
INSERT INTO TBL VALUES(2);
CREATE OR REPLACE PROCEDURE SP_TEST 
AS
    CURSOR CUR IS
        SELECT * FROM TBL;
BEGIN 
    --EXPLICIT CURSOR ATTRIBUTES INITIAL STATUS
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    OPEN CUR;
    IF CUR%FOUND THEN 
    ELSIF NOT CUR%FOUND THEN 
    ELSE
    END IF;
    
    IF CUR%NOTFOUND THEN 
    ELSIF NOT CUR%NOTFOUND THEN 
    ELSE
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('CUR%ROWCOUNT INITIAL STATUS : ' || NVL(TO_CHAR(CUR%ROWCOUNT),'NULL'));    
    
    CLOSE CUR;
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    
    --IMPLICIT CURSOR ATTRIBUTES INITIAL STATUS 
    IF SQL%ISOPEN THEN 
    ELSIF NOT SQL%ISOPEN THEN 
    ELSE
    END IF;

    IF SQL%FOUND THEN 
    ELSIF NOT SQL%FOUND THEN 
    ELSE
    END IF;
    
    IF SQL%NOTFOUND THEN 
    ELSIF NOT SQL%NOTFOUND THEN 
    ELSE
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('SQL%ROWCOUNT INITIAL STATUS : ' || NVL(TO_CHAR(SQL%ROWCOUNT),'NULL'));  
END;
/
CALL SP_TEST();
DROP TABLE TBL;
DROP PROCEDURE SP_TEST;

-- GAUSSDBV100R001C00-754 
CREATE TABLE TBL_H248LNK_INFO(ULBSGMODULENO INTEGER);
INSERT INTO TBL_H248LNK_INFO VALUES(123);
INSERT INTO TBL_H248LNK_INFO VALUES(456);
INSERT INTO TBL_H248LNK_INFO VALUES(789);
CREATE TABLE TBL (I_MODULENO INTEGER);

CREATE OR REPLACE PROCEDURE TEST_CURSOR 
AS
        TYPE CUR_TYPE IS REF CURSOR;
        CUR CUR_TYPE;
        PSV_SQL VARCHAR2(1000);
        PI_MODULENO INTEGER;
        TBL_STR VARCHAR2(1000) := 'TBL';
BEGIN
        OPEN CUR FOR SELECT DISTINCT ULBSGMODULENO FROM TBL_H248LNK_INFO;
        LOOP
            FETCH CUR INTO PI_MODULENO;
            EXIT WHEN CUR%NOTFOUND;            
            PSV_SQL := 'BEGIN INSERT INTO TBL (I_MODULENO) VALUES('||PI_MODULENO||');END;';
            EXECUTE IMMEDIATE PSV_SQL;

            -- check cursor attris status
            IF CUR%ISOPEN THEN 
            ELSIF NOT CUR%ISOPEN THEN 
            ELSE
            END IF;
            IF CUR%FOUND THEN 
            ELSIF NOT CUR%FOUND THEN 
            ELSE
            END IF;            
            IF CUR%NOTFOUND THEN 
            ELSIF NOT CUR%NOTFOUND THEN 
            ELSE
            END IF;            
            IF SQL%ISOPEN THEN 
            ELSIF NOT SQL%ISOPEN THEN 
            ELSE
            END IF;
            IF SQL%FOUND THEN 
            ELSIF NOT SQL%FOUND THEN 
            ELSE
            END IF;            
            IF SQL%NOTFOUND THEN 
            ELSIF NOT SQL%NOTFOUND THEN 
            ELSE
            END IF;            

        END LOOP;
        
    -- check cursor attris status
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    IF CUR%FOUND THEN 
    ELSIF NOT CUR%FOUND THEN 
    ELSE
    END IF;            
    IF CUR%NOTFOUND THEN 
    ELSIF NOT CUR%NOTFOUND THEN 
    ELSE
    END IF;            
    DBMS_OUTPUT.PUT_LINE('CUR%ROWCOUNT : ' || NVL(TO_CHAR(CUR%ROWCOUNT),'NULL'));                     
    IF SQL%ISOPEN THEN 
    ELSIF NOT SQL%ISOPEN THEN 
    ELSE
    END IF;
    IF SQL%FOUND THEN 
    ELSIF NOT SQL%FOUND THEN 
    ELSE
    END IF;            
    IF SQL%NOTFOUND THEN 
    ELSIF NOT SQL%NOTFOUND THEN 
    ELSE
    END IF;            
END;
/
CALL TEST_CURSOR();
DROP PROCEDURE TEST_CURSOR;
DROP TABLE TBL_H248LNK_INFO;
DROP TABLE TBL;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT,
    USWSBOARDMID SMALLINT,
    UCWSTYPE8100 SMALLINT,
    UCWSTYPE6600 SMALLINT,
    UCLOGINTYPE SMALLINT,
    UCTTSCAPABILITY SMALLINT,
    UCASRCAPABILITY SMALLINT,
    UCRESCAPABILITY CHARACTER VARYING(8)
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184, 472, 0, 1, 0, NULL, NULL, '11011000');

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);
CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
	PI_MODULENO INTEGER;
	PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
	PSV_SQL  VARCHAR2(128);
	V_TEMP INTEGER := 0;
	PI_NN INTEGER := NULL;
BEGIN
	OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG WHERE IWSNO >=0 AND IWSNO <= 0;
	LOOP
		FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
		EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
		IF (FLAG = 0) THEN 
			-- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
			PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
			EXECUTE IMMEDIATE PSV_SQL;
		ELSE
			TEST_TEMP();
		END IF;
	END LOOP;
	-- check cursor attris status
	IF CRS_RPT_EMPTY%ISOPEN THEN 
	ELSIF NOT CRS_RPT_EMPTY%ISOPEN THEN 
	ELSE
	END IF;
	IF CRS_RPT_EMPTY%FOUND THEN 
	ELSIF NOT CRS_RPT_EMPTY%FOUND THEN 
	ELSE
	END IF;            
	IF CRS_RPT_EMPTY%NOTFOUND THEN 
	ELSIF NOT CRS_RPT_EMPTY%NOTFOUND THEN 
	ELSE
	END IF;            
	DBMS_OUTPUT.PUT_LINE('CRS_RPT_EMPTY%ROWCOUNT : ' || NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL'));                     
	IF SQL%ISOPEN THEN 
	ELSIF NOT SQL%ISOPEN THEN 
	ELSE
	END IF;
	IF SQL%FOUND THEN 
	ELSIF NOT SQL%FOUND THEN 
	ELSE
	END IF;            
	IF SQL%NOTFOUND THEN 
	ELSIF NOT SQL%NOTFOUND THEN 
	ELSE
	END IF;            
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);
DROP PROCEDURE TEST_TEMP;
DROP PROCEDURE TEST_CRS_RPT_EMPTYSOR;
DROP TABLE TBL_RCWSCFG;
drop table TBL_TEMP_MODULE_312;

CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR VARA IN CURS1 LOOP
	END LOOP;
END;
/
--2 TEST FOR  DISPLAY CURSOR IN (SELECT ,INSERT ,UPDATE ,DELETE);
DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
FOR VARA IN CURS1 LOOP
	-- FOR SELECT
	SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
	-- FOR INSERT
	INSERT INTO TEST_TB VALUES (125);
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
	--UPDATE
	UPDATE TEST_TB SET ID=ID+1 WHERE ID=124;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;

	--DELETE
	DELETE FROM TEST_TB WHERE ID=125;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TB;
--3 TEST FOR IMPLICIT CURSOR IN (SELECT ,INSERT ,UPDATE ,DELETE)
CREATE TABLE TEST_TB (ID INT);
INSERT INTO TEST_TB VALUES (123);
INSERT INTO TEST_TB VALUES (124);
INSERT INTO TEST_TB VALUES (125);
DECLARE
	TEMP INTEGER = 0;
BEGIN
	-- FOR SELECT
	SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
	-- FOR INSERT
	INSERT INTO TEST_TB VALUES (125);
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
	--UPDATE
	UPDATE TEST_TB SET ID=ID+1 WHERE ID<124;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;

	--DELETE
	DELETE FROM TEST_TB WHERE ID<125;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
END;
/
DROP TABLE IF EXISTS TEST_TB;
--4 TEST FOR IMPLICIT CURSOR;
CREATE TABLE TEST_TB (ID INT);
INSERT INTO TEST_TB VALUES (123);
INSERT INTO TEST_TB VALUES (124);
INSERT INTO TEST_TB VALUES (125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR	 VARA IN CURS1 LOOP
		SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
		IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
		END IF;
		IF SQL%FOUND THEN
		END IF;
		IF NOT SQL%NOTFOUND THEN
		END IF;
		DBMS_OUTPUT.PUT_LINE('TEST SELECT: SQL%ROWCOUNT=' || SQL%ROWCOUNT);	
	END LOOP;
END;
/

DROP TABLE IF EXISTS TEST_TB;
CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR VARA IN CURS1 LOOP
	END LOOP;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
END;
/
DROP TABLE IF EXISTS TEST_TB;

--TEST FOR CURSOR SYS_REFCURSOR IN PROCEDURE AND EMPTY TABLE;
--IF THE RESULT IS 0 ,THAT'S OK,ELSE IS ERROR;
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);

CREATE OR REPLACE PROCEDURE T1(O OUT SYS_REFCURSOR)
IS
C1 SYS_REFCURSOR;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
	O := C1;
END;
/

DECLARE
	C1 SYS_REFCURSOR;
	TEMP INTEGER;
BEGIN
	T1(C1);
	LOOP
 		FETCH C1 INTO TEMP;
		EXIT WHEN C1%NOTFOUND;
	END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;
DROP PROCEDURE T1;
--TEST FOR CURSOR REFCURSOR IN PROCEDURE AND EMPTY TABLE;
--IF THE RESULT IS 0 ,THAT'S OK,ELSE IS ERROR;
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);

CREATE OR REPLACE PROCEDURE T2(O OUT REFCURSOR)
IS
C1 SYS_REFCURSOR;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
	O := C1;
END;
/

DECLARE
   	C1 REFCURSOR;
	TEMP INTEGER;
BEGIN
	T2(C1);
	LOOP
  	FETCH C1 INTO TEMP;
	EXIT WHEN C1%NOTFOUND;
	END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;
DROP PROCEDURE T2;
--TEST CURSOR IN Anonymous block
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);
DECLARE
	C1 REFCURSOR;
	TEMP INTEGER;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
LOOP
	FETCH C1 INTO TEMP;
	EXIT WHEN C1%NOTFOUND;
END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;

DROP PROCEDURE TEST_TEMP;
DROP PROCEDURE TEST_CRS_RPT_EMPTYSOR;
DROP TABLE TBL_RCWSCFG;
drop table TBL_TEMP_MODULE_312;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184);

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);

CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
 PI_MODULENO INTEGER;
 PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
 PSV_SQL  VARCHAR2(128);
 PI_NN INTEGER := NULL;
BEGIN
 OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG;
 LOOP
  FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
  EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
  IF (FLAG = 0) THEN 
   -- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
   PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
   EXECUTE IMMEDIATE PSV_SQL;
  ELSE
   TEST_TEMP();
  END IF;
 END LOOP;
 -- check cursor attris status
 DBMS_OUTPUT.PUT_LINE('CRS_RPT_EMPTY%ROWCOUNT : ' || NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL'));                     
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);
DROP TABLE TBL_TEMP_MODULE_312;

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

--test explicit cursor without args
create or replace procedure test_cursor_1 
as
    company_name    varchar(100);
    company_loc varchar(100);
    company_no  integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        exit when c1_all%notfound;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_1();
drop procedure test_cursor_1;

--test explicit cursor with args
create or replace procedure test_cursor_2 
as 
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c2_no_range(no_1 integer, no_2 integer) is --cursor with args 
        select name, loc, no from company where no >=no_1 and no <= no_2 order by 1, 2, 3;
begin 
    if not c2_no_range%isopen then
        open c2_no_range(5,10);
    end if;
    loop
        fetch c2_no_range into company_name, company_loc, company_no;
        exit when c2_no_range%notfound;
    end loop;
    if c2_no_range%isopen then
        close c2_no_range;
        dbms_output.put_line('c2_no_range closed');	
    end if;
end;
/
call test_cursor_2();
drop procedure test_cursor_2;

--test explicit cursor attributes
create or replace procedure test_cursor_3
as
    company_name    varchar(100);
    company_loc     varchar(100);
    company_no      integer;

    cursor c1_all is --cursor without args 
        select name, loc, no from company order by 1, 2, 3;
begin 
    if not c1_all%isopen then
        open c1_all;
    end if;
    loop
        fetch c1_all into company_name, company_loc, company_no;
        if c1_all%notfound then
            exit;
        end if;
        if c1_all%found then
        end if;
    end loop;
    if c1_all%isopen then
        close c1_all;
    end if;
end;
/
call test_cursor_3();
drop procedure test_cursor_3;

--test implicit cursor attributes: (sql%)%found,%notfound,%isopen,%rowcount
create or replace procedure test_cursor_4
as
begin 
    delete from company where loc='china';
    if sql%isopen then --sql%isopen always false, as closed after the execution of sql.
    end if;
    if not sql%isopen then 
    end if;
    if sql%found then
    end if;
    if sql%notfound then
    end if;
end;
/
call test_cursor_4();
drop procedure test_cursor_4;

--test dynamic cursor: (weak type)without return
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
        fetch my_cur into company_name,company_loc,company_no;
    end loop;
    close my_cur;
end;
/
call test_cursor_5();
drop procedure test_cursor_5;

----test more than one cursors access	
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
    end if;
    if c1_all%notfound then
    end if;
    if c2_no_range%found then
    end if;
    if c2_no_range%notfound then
    end if;
end;
/
call test_cursor_6();
drop procedure test_cursor_6;

drop table company;

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
            exit when c1%notfound; 
        end loop;
        close c1;
--test implicit cursor rowcount attribute
        select id into temp1 from tbl where id=2 order by 1;
        update tbl set id=100 where id<3;
        insert into tbl select * from tbl;
        delete from tbl;
    end;  
end;
/

call sp_testsp();
drop procedure sp_testsp;
drop table tbl;

------------------------------------------------------------------------------
-----test implicit cursor attributes for DML: select,insert,update,delete-----
------------------------------------------------------------------------------
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp
as
    v int:=0;
begin
    --select
    select v1 into v from t1 where v1=1; 
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    

    --insert
    insert into t1 values (4,'abc4');
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    dbms_output.put_line('test update: sql%rowcount=' || sql%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
 
    --delete
    delete from t1 where v1>1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    if not sql%isopen then --sql%isopen always be false    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
end;
/
call sp_testsp();
drop procedure sp_testsp;
drop table t1;
------------------------------------------------------------------------------
-----support A db's cursor in or out params---------------------------------
------------------------------------------------------------------------------
CREATE TABLE TBL(VALUE INT);
INSERT INTO TBL VALUES (1);
INSERT INTO TBL VALUES (2);
INSERT INTO TBL VALUES (3);
INSERT INTO TBL VALUES (4);

CREATE OR REPLACE PROCEDURE TEST_SP
IS
    CURSOR C1(NO IN VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < NO ORDER BY 1;
    CURSOR C2(NO OUT VARCHAR2) IS SELECT * FROM TBL WHERE VALUE < 10 ORDER BY 1;
    V INT;
    RESULT INT;
BEGIN
    OPEN C1(10);
    OPEN C2(RESULT);
    LOOP
    FETCH C1 INTO V; 
        IF C1%FOUND THEN 
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C1;
    
    LOOP
    FETCH C2 INTO V; 
        IF C2%FOUND THEN 
        ELSE 
            EXIT;
        END IF;
    END LOOP;
    CLOSE C2;
END;
/
CALL  TEST_SP();
DROP TABLE TBL;
DROP PROCEDURE TEST_SP;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the implicit cursor use to explicit cursor attributes --
---------------------------------------------------------------------------------
drop table t1;
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select
    select v1 into v from t1 where v1=1;    
    if not cur%isopen then   
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    dbms_output.put_line('test update: cur%rowcount=' || cur%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    if not cur%isopen then    
    end if;
    if cur%found then 
    end if;
    if cur%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;

---------------------------------------------------------------------------------
----- test the mixed use of implicit and explicit cursor attributes -------------
----- test the effect of the explicit cursor use to implicit cursor attributes --
---------------------------------------------------------------------------------
create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');

create or replace procedure sp_testsp_select
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --select    
    select v1 into v from t1 where v1=1;    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then   
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_select();
drop procedure sp_testsp_select;
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_insert
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --insert
    insert into t1 values (4,'abc4');    
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_insert();
drop procedure sp_testsp_insert;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_update
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --update
    update t1 set v1=v1+100 where v1>1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    dbms_output.put_line('test update: sql%rowcount=' || sql%rowcount);  
    
    update t1 set v1=v1+100 where v1<1000;    
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_update();
drop procedure sp_testsp_update;  
drop table t1;

create table t1(v1 int,v2 varchar2(100));
insert into t1 values (1,'abc1');
insert into t1 values (2,'abc2');
insert into t1 values (3,'abc3');
create or replace procedure sp_testsp_delete
as
    v int:=0;
    CURSOR cur IS select v1 from t1; 
begin   
    open cur;
    --delete
    delete from t1 where v1>1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    
    delete from t1 where v1<1000;
    fetch cur into v;
    fetch cur into v;
    if not sql%isopen then    
    end if;
    if sql%found then 
    end if;
    if sql%notfound then 
    end if;    
    close cur;
end;
/
call sp_testsp_delete();
drop procedure sp_testsp_delete;  
drop table t1;
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE TABLE TBL(V1 INTEGER);
INSERT INTO TBL VALUES(1);
INSERT INTO TBL VALUES(2);
CREATE OR REPLACE PROCEDURE SP_TEST 
AS
    CURSOR CUR IS
        SELECT * FROM TBL;
BEGIN 
    --EXPLICIT CURSOR ATTRIBUTES INITIAL STATUS
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    OPEN CUR;
    IF CUR%FOUND THEN 
    ELSIF NOT CUR%FOUND THEN 
    ELSE
    END IF;
    
    IF CUR%NOTFOUND THEN 
    ELSIF NOT CUR%NOTFOUND THEN 
    ELSE
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('CUR%ROWCOUNT INITIAL STATUS : ' || NVL(TO_CHAR(CUR%ROWCOUNT),'NULL'));    
    
    CLOSE CUR;
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    
    --IMPLICIT CURSOR ATTRIBUTES INITIAL STATUS 
    IF SQL%ISOPEN THEN 
    ELSIF NOT SQL%ISOPEN THEN 
    ELSE
    END IF;

    IF SQL%FOUND THEN 
    ELSIF NOT SQL%FOUND THEN 
    ELSE
    END IF;
    
    IF SQL%NOTFOUND THEN 
    ELSIF NOT SQL%NOTFOUND THEN 
    ELSE
    END IF;
    
    DBMS_OUTPUT.PUT_LINE('SQL%ROWCOUNT INITIAL STATUS : ' || NVL(TO_CHAR(SQL%ROWCOUNT),'NULL'));  
END;
/
CALL SP_TEST();
DROP TABLE TBL;
DROP PROCEDURE SP_TEST;

-- GAUSSDBV100R001C00-754 
CREATE TABLE TBL_H248LNK_INFO(ULBSGMODULENO INTEGER);
INSERT INTO TBL_H248LNK_INFO VALUES(123);
INSERT INTO TBL_H248LNK_INFO VALUES(456);
INSERT INTO TBL_H248LNK_INFO VALUES(789);
CREATE TABLE TBL (I_MODULENO INTEGER);

CREATE OR REPLACE PROCEDURE TEST_CURSOR 
AS
        TYPE CUR_TYPE IS REF CURSOR;
        CUR CUR_TYPE;
        PSV_SQL VARCHAR2(1000);
        PI_MODULENO INTEGER;
        TBL_STR VARCHAR2(1000) := 'TBL';
BEGIN
        OPEN CUR FOR SELECT DISTINCT ULBSGMODULENO FROM TBL_H248LNK_INFO;
        LOOP
            FETCH CUR INTO PI_MODULENO;
            EXIT WHEN CUR%NOTFOUND;            
            PSV_SQL := 'BEGIN INSERT INTO TBL (I_MODULENO) VALUES('||PI_MODULENO||');END;';
            EXECUTE IMMEDIATE PSV_SQL;

            -- check cursor attris status
            IF CUR%ISOPEN THEN 
            ELSIF NOT CUR%ISOPEN THEN 
            ELSE
            END IF;
            IF CUR%FOUND THEN 
            ELSIF NOT CUR%FOUND THEN 
            ELSE
            END IF;            
            IF CUR%NOTFOUND THEN 
            ELSIF NOT CUR%NOTFOUND THEN 
            ELSE
            END IF;            
            IF SQL%ISOPEN THEN 
            ELSIF NOT SQL%ISOPEN THEN 
            ELSE
            END IF;
            IF SQL%FOUND THEN 
            ELSIF NOT SQL%FOUND THEN 
            ELSE
            END IF;            
            IF SQL%NOTFOUND THEN 
            ELSIF NOT SQL%NOTFOUND THEN 
            ELSE
            END IF;            

        END LOOP;
        
    -- check cursor attris status
    IF CUR%ISOPEN THEN 
    ELSIF NOT CUR%ISOPEN THEN 
    ELSE
    END IF;
    IF CUR%FOUND THEN 
    ELSIF NOT CUR%FOUND THEN 
    ELSE
    END IF;            
    IF CUR%NOTFOUND THEN 
    ELSIF NOT CUR%NOTFOUND THEN 
    ELSE
    END IF;            
    DBMS_OUTPUT.PUT_LINE('CUR%ROWCOUNT : ' || NVL(TO_CHAR(CUR%ROWCOUNT),'NULL'));                     
    IF SQL%ISOPEN THEN 
    ELSIF NOT SQL%ISOPEN THEN 
    ELSE
    END IF;
    IF SQL%FOUND THEN 
    ELSIF NOT SQL%FOUND THEN 
    ELSE
    END IF;            
    IF SQL%NOTFOUND THEN 
    ELSIF NOT SQL%NOTFOUND THEN 
    ELSE
    END IF;            
END;
/
CALL TEST_CURSOR();
DROP PROCEDURE TEST_CURSOR;
DROP TABLE TBL_H248LNK_INFO;
DROP TABLE TBL;
DROP TABLE TBL_RCWSCFG;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT,
    USWSBOARDMID SMALLINT,
    UCWSTYPE8100 SMALLINT,
    UCWSTYPE6600 SMALLINT,
    UCLOGINTYPE SMALLINT,
    UCTTSCAPABILITY SMALLINT,
    UCASRCAPABILITY SMALLINT,
    UCRESCAPABILITY CHARACTER VARYING(8)
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184, 472, 0, 1, 0, NULL, NULL, '11011000');

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);
CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
	PI_MODULENO INTEGER;
	PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
	PSV_SQL  VARCHAR2(128);
	V_TEMP INTEGER := 0;
	PI_NN INTEGER := NULL;
BEGIN
	OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG WHERE IWSNO >=0 AND IWSNO <= 0;
	LOOP
		FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
		EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
		IF (FLAG = 0) THEN 
			-- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
			PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
			EXECUTE IMMEDIATE PSV_SQL;
		ELSE
			TEST_TEMP();
		END IF;
	END LOOP;
	-- check cursor attris status
	IF CRS_RPT_EMPTY%ISOPEN THEN 
	ELSIF NOT CRS_RPT_EMPTY%ISOPEN THEN 
	ELSE
	END IF;
	IF CRS_RPT_EMPTY%FOUND THEN 
	ELSIF NOT CRS_RPT_EMPTY%FOUND THEN 
	ELSE
	END IF;            
	IF CRS_RPT_EMPTY%NOTFOUND THEN 
	ELSIF NOT CRS_RPT_EMPTY%NOTFOUND THEN 
	ELSE
	END IF;            
	DBMS_OUTPUT.PUT_LINE('CRS_RPT_EMPTY%ROWCOUNT : ' || NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL'));                     
	IF SQL%ISOPEN THEN 
	ELSIF NOT SQL%ISOPEN THEN 
	ELSE
	END IF;
	IF SQL%FOUND THEN 
	ELSIF NOT SQL%FOUND THEN 
	ELSE
	END IF;            
	IF SQL%NOTFOUND THEN 
	ELSIF NOT SQL%NOTFOUND THEN 
	ELSE
	END IF;            
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);
DROP PROCEDURE TEST_TEMP;
DROP PROCEDURE TEST_CRS_RPT_EMPTYSOR;
DROP TABLE TBL_RCWSCFG;
drop table TBL_TEMP_MODULE_312;

CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR VARA IN CURS1 LOOP
	END LOOP;
END;
/
--2 TEST FOR  DISPLAY CURSOR IN (SELECT ,INSERT ,UPDATE ,DELETE);
DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
FOR VARA IN CURS1 LOOP
	-- FOR SELECT
	SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
	-- FOR INSERT
	INSERT INTO TEST_TB VALUES (125);
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
	--UPDATE
	UPDATE TEST_TB SET ID=ID+1 WHERE ID=124;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;

	--DELETE
	DELETE FROM TEST_TB WHERE ID=125;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TB;
--3 TEST FOR IMPLICIT CURSOR IN (SELECT ,INSERT ,UPDATE ,DELETE)
CREATE TABLE TEST_TB (ID INT);
INSERT INTO TEST_TB VALUES (123);
INSERT INTO TEST_TB VALUES (124);
INSERT INTO TEST_TB VALUES (125);
DECLARE
	TEMP INTEGER = 0;
BEGIN
	-- FOR SELECT
	SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
	-- FOR INSERT
	INSERT INTO TEST_TB VALUES (125);
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
	--UPDATE
	UPDATE TEST_TB SET ID=ID+1 WHERE ID<124;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;

	--DELETE
	DELETE FROM TEST_TB WHERE ID<125;
	IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
	END IF;
	IF SQL%FOUND THEN
	END IF;
	IF NOT SQL%NOTFOUND THEN
	END IF;
END;
/
DROP TABLE IF EXISTS TEST_TB;
--4 TEST FOR IMPLICIT CURSOR;
CREATE TABLE TEST_TB (ID INT);
INSERT INTO TEST_TB VALUES (123);
INSERT INTO TEST_TB VALUES (124);
INSERT INTO TEST_TB VALUES (125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR	 VARA IN CURS1 LOOP
		SELECT ID INTO TEMP FROM TEST_TB WHERE ID = 123;
		IF NOT SQL%ISOPEN THEN --SQL%ISOPEN ALWAYS BE FALSE
		END IF;
		IF SQL%FOUND THEN
		END IF;
		IF NOT SQL%NOTFOUND THEN
		END IF;
		DBMS_OUTPUT.PUT_LINE('TEST SELECT: SQL%ROWCOUNT=' || SQL%ROWCOUNT);	
	END LOOP;
END;
/

DROP TABLE IF EXISTS TEST_TB;
CREATE TABLE TEST_TB(ID INTEGER);
INSERT INTO TEST_TB VALUES(123);
INSERT INTO TEST_TB VALUES(124);
INSERT INTO TEST_TB VALUES(125);

DECLARE
    CURSOR CURS1 IS SELECT * FROM TEST_TB;
 TEMP INTEGER:=0;
BEGIN
	FOR VARA IN CURS1 LOOP
	END LOOP;
	IF NOT CURS1%ISOPEN THEN --CURS1%ISOPEN ALWAYS BE FALSE
	END IF;
	IF CURS1%FOUND THEN
	END IF;
	IF CURS1%NOTFOUND THEN
	END IF;
END;
/
DROP TABLE IF EXISTS TEST_TB;

--TEST FOR CURSOR SYS_REFCURSOR IN PROCEDURE AND EMPTY TABLE;
--IF THE RESULT IS 0 ,THAT'S OK,ELSE IS ERROR;
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);

CREATE OR REPLACE PROCEDURE T1(O OUT SYS_REFCURSOR)
IS
C1 SYS_REFCURSOR;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
	O := C1;
END;
/

DECLARE
	C1 SYS_REFCURSOR;
	TEMP INTEGER;
BEGIN
	T1(C1);
	LOOP
 		FETCH C1 INTO TEMP;
		EXIT WHEN C1%NOTFOUND;
	END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;
DROP PROCEDURE T1;
--TEST FOR CURSOR REFCURSOR IN PROCEDURE AND EMPTY TABLE;
--IF THE RESULT IS 0 ,THAT'S OK,ELSE IS ERROR;
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);

CREATE OR REPLACE PROCEDURE T2(O OUT REFCURSOR)
IS
C1 SYS_REFCURSOR;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
	O := C1;
END;
/

DECLARE
   	C1 REFCURSOR;
	TEMP INTEGER;
BEGIN
	T2(C1);
	LOOP
  	FETCH C1 INTO TEMP;
	EXIT WHEN C1%NOTFOUND;
	END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;
DROP PROCEDURE T2;
--TEST CURSOR IN Anonymous block
DROP TABLE IF EXISTS TEST_TBL;
CREATE TABLE TEST_TBL(ID INTEGER);
DECLARE
	C1 REFCURSOR;
	TEMP INTEGER;
BEGIN
	OPEN C1 FOR SELECT ID FROM TEST_TBL ORDER BY ID;
LOOP
	FETCH C1 INTO TEMP;
	EXIT WHEN C1%NOTFOUND;
END LOOP;
END;
/
DROP TABLE IF EXISTS TEST_TBL;

DROP PROCEDURE TEST_TEMP;
DROP PROCEDURE TEST_CRS_RPT_EMPTYSOR;
DROP TABLE TBL_RCWSCFG;
drop table TBL_TEMP_MODULE_312;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184);

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);

CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
 PI_MODULENO INTEGER;
 PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
 PSV_SQL  VARCHAR2(128);
 PI_NN INTEGER := NULL;
BEGIN
 OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG;
 LOOP
  FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
  EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
  IF (FLAG = 0) THEN 
   -- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
   PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
   EXECUTE IMMEDIATE PSV_SQL;
  ELSE
   TEST_TEMP();
  END IF;
 END LOOP;
 -- check cursor attris status
 DBMS_OUTPUT.PUT_LINE('CRS_RPT_EMPTY%ROWCOUNT : ' || NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL'));                     
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);
DROP TABLE TBL_TEMP_MODULE_312;


--test cursor define
create or replace procedure pro_cursor_c0019() as
declare
   cursor cursor1 for create table t1(a int);
BEGIN
END;
/

create table t1(a int);
--test with query
create or replace procedure test_cursor() as
declare
	cursor cursor1 is 
	with recursive StepCTE(a)
	as (select a from t1) select * from StepCTE;
BEGIN
	null;
END;
/

cursor pro_cursor_c0019_1 with hold for select * from t1;

create or replace procedure pro_cursor_c0019() as
declare
	cursor cursor1 for fetch pro_cursor_c0019_1;
BEGIN
	open cursor1;
	close cursor1;
END;
/

select * from pro_cursor_c0019();
close  pro_cursor_c0019_1;
select * from pro_cursor_c0019();
drop table t1;
