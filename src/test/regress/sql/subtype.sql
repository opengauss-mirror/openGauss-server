drop database if exists subtype_db;
create database subtype_db dbcompatibility 'A';

\c subtype_db
-- 数组
DECLARE
    type ty1 is varray(5) of int;
    SUBTYPE ty2 is ty1;
    a1 ty1 := ty1(6,7,8,9,10,11);
    a2 ty2 := ty1(1,2,3);
BEGIN
    raise info 'a = %',a1;
    raise info 'a = %',a2;
END;
/

-- 嵌套表
DECLARE
    type ty1 is table of varchar2;
    SUBTYPE ty2 is ty1;
    a ty2;
BEGIN
    a := ty2('aa','bb','cc');
    raise info 'a = %',a;
END;
/

-- 关系数组
DECLARE
    TYPE ty1 is table of varchar(2) INDEX BY int;
    SUBTYPE ty2 is ty1;
    a ty2;
BEGIN
    a := ty2('aa','bb','cc');
    raise info 'a = %',a;
    a(1) = 'tt';
    raise info 'a = %',a;
END;
/

--TODO 游标
DECLARE
    TYPE ty1 is ref cursor;
    SUBTYPE ty2 is ty1;
BEGIN
    null;
END;
/

DECLARE
    type ty1 is record (a1 int, a2 int);
    SUBTYPE ty2 is ty1;
    a ty2;
BEGIN
    a := (1,1);
    raise info 'a = %',a;
END;
/

create or replace procedure proc1 as
    subtype ty1 is int;
    subtype ty2 is ty1;
    a ty2;
BEGIN
    a := 123;
    raise info 'a = %',a;
END;
/

call proc1();

DECLARE
    subtype ty1 is int;
    a ty1;
BEGIN
    a := 123;
    raise info 'a = %',a;
END;
/

DECLARE
    type ty1_base is record (a int, b int);
    subtype ty1 is ty1_base;
    a ty1;
BEGIN
    a := (1,1);
    raise info 'a = %',a;
END;
/

-------匿名块---------
-- 不同类型，使用subtype
DECLARE
    SUBTYPE ty1 is int;
    SUBTYPE ty2 is varchar;
    SUBTYPE ty3 is number(5,2);
    SUBTYPE ty4 is float(32);
    SUBTYPE ty5 is raw;
    a ty1;
    b ty2;
    c ty3;
    d ty4;
    e ty5;
BEGIN
    a := 6;
    b := 'test';
    c := 1.12123434;
    e := HEXTORAW('FFFF');
    raise info 'a = %',a;
    raise info 'b = %',b;
    raise info 'c = %',c;
    raise info 'd = %',d;
    raise info 'e = %',e;
END;
/

--测试指定精度范围；
DECLARE
    SUBTYPE Balance is NUMBER;
    a Balance(6,2);
    b Balance(8,2);
BEGIN
    a := 1000.1234;
    b := 100000.1234;
    raise info 'a = %',a;
    raise info 'b = %',b;
END;
/

--测试约束
DECLARE
    SUBTYPE Balance is NUMBER;
    a Balance(6,2);
    b Balance(8,2);
BEGIN
    a := 1000.1234;
    b := 123456789.1234;
    raise info 'a = %',a;
    raise info 'b = %',b;
END;
/

--测试报错

--1、约束越界
DECLARE
    SUBTYPE Balance is NUMBER(15,2);
    a Balance(6,2);
    b Balance(8,2);
    c Balance;
BEGIN
    a := 1000.1234;
    b := 123456789.1234;
    c := 123456789.12;
    raise info 'a = %',a;
    raise info 'b = %',b;
    raise info 'c = %',c;
END;
/


--2、错误类型指定约束
DECLARE
    SUBTYPE test is int range 1 .. 2;
    a test;
BEGIN
    a := 1;
    raise info 'a = %', a;
END;
/


--3、指定not null后为null
DECLARE
    SUBTYPE test IS INTEGER NOT NULL;
    a test;
BEGIN
    a := 1;
    raise info 'a = %',a;
END;
/

--4、超出指定范围的精度，报错
--两种指定精度方式：一种是指定subtype，一种是指定具体变量
DECLARE
    SUBTYPE Balance IS NUMBER(8,2);
    checking_account Balance;
    savings_account Balance;
BEGIN
    checking_account := 2000.0;
    savings_account := 10000034.00;
END;
/

DECLARE
    SUBTYPE Balance is number;
    a Balance(6,2);
BEGIN
    a := 10009.1234;
    raise info 'a = %', a;
END;
/

-----函数-------

create or replace procedure proc1 as
    subtype animal is number(6,2);
    subtype animal2 is number(6,2);
    a animal;
begin
    a := 6;
    raise info 'a = %',a;
end;
/

call proc1();
call proc1();

--新会话调用函数
\c
call proc1();
call proc1();

--重建
create or replace procedure proc1 as
    subtype animal is number(6,2);
    subtype animal2 is number(6,2);
    a animal;
begin
    a := 6;
    raise info 'a = %',a;
end;
/

call proc1();
call proc1();

--测试函数中报错
\c
create or replace procedure proc2 as
    subtype animal is int;
    subtype animal2 is int;
    a animal;
begin
    a := 10;
    raise info 'a = %',a;
    return a;
end;
/

call proc2();
call proc2();

create or replace procedure proc2 as
    subtype Balance is number;
    a Balance(6,2);
begin
    a := 10009.1234;
    raise info 'a = %',a;
    return a;
end;
/

call proc2();
call proc2();


-------PKG--------
create or replace package pkg1 as
    subtype animal is number(2,1);
    procedure proc1(a in animal);
end pkg1;
/

declare
    a pkg1.animal;
begin
    a := 10;
    raise info 'a = %', a;
end;
/

declare
    a pkg1.animal;
begin
    a := 6;
    raise info 'a = %', a;
end;
/

-- 类型作为入参时，违反约束
create or replace package body pkg1 as
    procedure proc1(a in animal) is
    begin
        raise info 'pkg.proc1 a = %', a;
    end;
end pkg1;
/

call pkg1.proc1(20);

-- 新会话调用
\c

declare
    a pkg1.animal;
begin
    a := 10;
    raise info 'a = %', a;
end;
/

declare
    a pkg1.animal;
begin
    a := 6;
    raise info 'a = %', a;
end;
/

--测试pkg中报错
create or replace package body pkg1 as
    procedure proc1(a in animal) is
    begin
        a := 10;
        raise info 'pkg1.proc1 a = %', a;
    end;
end pkg1;
/

call pkg1.proc1(1);

create or replace package pkg1 as
    subtype Balance is number;
    a Balance(6,2);
    procedure proc1();
end pkg1;
/

create or replace package body pkg1 as
    procedure proc1() is
    begin
        a := 10009.1234;
        raise info 'a = %',a;
    end;
end pkg1;
/

call pkg1.proc1();

CREATE TABLE sub_employees (
    emp_id numeric primary key,
    emp_name varchar2(50),
    salary numeric,
    hire_date   DATE
);

INSERT INTO sub_employees VALUES (100, 'John', 8000, '01-Jan-10');
INSERT INTO sub_employees VALUES (101, 'Doe', 8000, '15-Feb-10');
INSERT INTO sub_employees VALUES (102, 'Smith', 8000, '01-Mar-10');
INSERT INTO sub_employees VALUES (103, 'Tom', 8000, '01-Apr-10');

declare
    emp sub_employees%ROWTYPE;
    SUBTYPE emp_subtype_base IS sub_employees%ROWTYPE;
    SUBTYPE emp_subtype IS emp_subtype_base;
    emp1 emp_subtype;
BEGIN
    SELECT *
    INTO emp
    FROM sub_employees
    WHERE emp_id = 100;

    emp1.emp_id := emp.emp_id + 100;
    emp1.emp_name := emp.emp_name;
    emp1.salary := emp.salary;
    raise info 'a = %',emp1;
END;
/

create or replace procedure proc3 as
    type ty1 is record (a int, b int);
    a ty1;
BEGIN
    a := (1,1);
    raise info 'a = %',a;
END;
/
call proc3();

create or replace procedure proc4 as
    type ty1 is table of varchar2;
    SUBTYPE ty2 is ty1;
    a ty2;
BEGIN
    a := ty2('aa','bb','cc');
    raise info 'a = %',a;
END;
/
call proc4();

create or replace procedure proc5 as
    type ty1 is varray(5) of int;
    SUBTYPE ty2 is ty1;
    a1 ty1 := ty1(6,7,8,9,10,11);
    a2 ty2 := ty1(1,2,3);
BEGIN
    raise info 'a = %',a1;
    raise info 'a = %',a2;
END;
/
call proc5();

create or replace procedure proc6 as
    emp sub_employees%ROWTYPE;
    SUBTYPE emp_subtype_base IS sub_employees%ROWTYPE;
    SUBTYPE emp_subtype IS emp_subtype_base;
    emp1 emp_subtype;
    BEGIN
    SELECT *
    INTO emp
    FROM sub_employees
    WHERE emp_id = 100;

    emp1.emp_id := emp.emp_id + 100;
    emp1.emp_name := emp.emp_name;
    emp1.salary := emp.salary;
    raise info 'a = %',emp1;
END;
/
call proc6();

create or replace package pkg_type as
    type ty_re is record (a int, b int);
    type ty_va is varray(5) of int;
    type ty_ta is table of varchar2;
    SUBTYPE sty_re is ty_re;
    SUBTYPE sty_va is ty_va;
    SUBTYPE sty_ta is ty_ta;
    SUBTYPE sty_row is sub_employees%ROWTYPE;
    procedure proc_record();
    procedure proc_varry();
    procedure proc_table();
    procedure proc_row();
end pkg_type;
/


create or replace package body pkg_type as
    procedure proc_record() is
        a sty_re;
    BEGIN
        a := (1,1);
        raise info 'a = %',a;
    END;

    procedure proc_table() is
        a sty_ta;
    BEGIN
        a := sty_ta('aa','bb','cc');
        raise info 'a = %',a;
    END;

    procedure proc_varry() is
        a1 sty_va := pkg_type.sty_va(6,7,8,9,10,11);
    BEGIN
        raise info 'a = %',a1;
    END;

    procedure proc_row() Is
        emp sty_row;
        emp1 pkg_type.sty_row;
        BEGIN
        SELECT *
        INTO emp
        FROM sub_employees
        WHERE emp_id = 100;

        emp1.emp_id := emp.emp_id + 100;
        emp1.emp_name := emp.emp_name;
        emp1.salary := emp.salary;
        raise info 'a = %',emp1;
    END;
END pkg_type;
/

call pkg_type.proc_record();
call pkg_type.proc_varry();
call pkg_type.proc_table();
call pkg_type.proc_row();

DECLARE
    a pkg_type.sty_re;
BEGIN
    a := (1,1);
    raise info 'a = %',a;
END;
/

DECLARE
    a1 pkg_type.sty_va := pkg_type.sty_va(6,7,8,9,10,11);
BEGIN
    raise info 'a = %',a1;
END;
/

DECLARE
    a pkg_type.sty_ta;
BEGIN
    a := pkg_type.sty_ta('aa','bb','cc');
    raise info 'a = %',a;
END;
/

DECLARE
    emp sub_employees%ROWTYPE;
    emp1 pkg_type.sty_row;
BEGIN
    SELECT *
    INTO emp
    FROM sub_employees
    WHERE emp_id = 100;

    emp1.emp_id := emp.emp_id + 100;
    emp1.emp_name := emp.emp_name;
    emp1.salary := emp.salary;
    raise info 'a = %',emp1;
END;
/

\c regression
drop database subtype_db;