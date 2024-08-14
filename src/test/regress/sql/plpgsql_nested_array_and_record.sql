-- check compatibility --
show sql_compatibility; -- expect A --

DROP SCHEMA IF EXISTS plpgsql_nested_array_and_record CASCADE;
CREATE SCHEMA plpgsql_nested_array_and_record;
SET current_schema = plpgsql_nested_array_and_record;

-- array of arrays
DECLARE
    TYPE arr1 IS VARRAY(5) OF INTEGER;
    TYPE arr2 IS VARRAY(5) OF arr1;
    nst_arr arr2;
BEGIN
    FOR I IN 1..5 LOOP
        nst_arr(1)(I) := I;
        RAISE NOTICE 'RESULT: %', nst_arr(1)(I);
    END LOOP;
END;
/

-- record of arrays
DECLARE
    TYPE arr1 IS VARRAY(5) OF INTEGER;
    TYPE rec1 IS RECORD(id int, arrarg arr1);
    arr_rec rec1;
BEGIN
    FOR I IN 1..5 LOOP
        arr_rec.arrarg(I):=I;
        RAISE NOTICE 'RESULT: %', arr_rec.arrarg(I);
    END LOOP;    
END;
/

-- array of records
CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE rec1 IS RECORD(id int, name char(10));
    TYPE arr1 IS VARRAY(5) OF rec1;
    rec_arr arr1;
BEGIN
    FOR I IN 1..5 LOOP
        rec_arr(I).id := I;
        RAISE NOTICE 'RESULT: %', rec_arr(I).id;
    END LOOP;
END;
/
CALL test_nested();

-- record of records
CREATE OR REPLACE PROCEDURE test_nested AS
DECLARE
    TYPE rec1 IS RECORD(id int, name char(10));
    TYPE rec2 IS RECORD(id int, recarg rec1);
    recrec rec2;
BEGIN
    recrec.recarg.id := 1;
    recrec.recarg.name := 'RECORD';
    RAISE NOTICE 'ID: %, NAME: %', recrec.recarg.id, recrec.recarg.name;
END;
/
CALL test_nested();

set behavior_compat_options='plpgsql_dependency';

create or replace package pac_PLArray_Case0021 is
  type typ_PLArray_1 is table of varchar(100);
  type typ_PLArray_2 is table of typ_PLArray_1;
  nstarr typ_PLArray_2;

  procedure p_PLArray_1;
  procedure p_PLArray_2(var typ_PLArray_2);
end pac_PLArray_Case0021;
/

create or replace package body pac_PLArray_Case0021 is
procedure p_PLArray_1() is
begin
nstarr(2)(1):='第二行第一列';
perform p_PLArray_2(nstarr);
end;

procedure p_PLArray_2(var typ_PLArray_2) is
begin
    insert into t_PLArray_case0021(col) values(var(2)(1));
end;
end pac_PLArray_Case0021;
/

create or replace package pac_PLArray_Case0021 is
  procedure p_PLArray_1;
  procedure p_PLArray_2(var typ_PLArray_3);
end pac_PLArray_Case0021;
/

create or replace package body pac_PLArray_Case0021 is
procedure p_PLArray_1() is
begin
nstarr(2)(1):='第二行第一列';
perform p_PLArray_2(nstarr);
end;

procedure p_PLArray_2(var typ_PLArray_3) is
begin
    insert into t_PLArray_case0021(col) values(var(2)(1));
end;
end pac_PLArray_Case0021;
/

drop package pac_PLArray_Case0021;

DROP SCHEMA plpgsql_nested_array_and_record CASCADE;
