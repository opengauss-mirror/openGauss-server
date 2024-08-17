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

CREATE OR REPLACE PROCEDURE test_nested_array as
TYPE typ_PLArray_case0001 IS varray(3) OF integer;
TYPE typ_PLArray_case0002 IS varray(3) OF typ_PLArray_case0001;
nstarr typ_PLArray_case0002;
BEGIN
        nstarr(1):=1;
        RAISE NOTICE '二维数组(1)：%', nstarr(1);
END;
/
CALL test_nested_array();

CREATE OR REPLACE PROCEDURE test_nested_array as
TYPE typ_PLArray_case0001 IS varray(3) OF integer;
TYPE typ_PLArray_case0002 IS varray(3) OF typ_PLArray_case0001;
nstarr typ_PLArray_case0002;
arr typ_PLArray_case0001;
BEGIN
        arr(1):=1;
        nstarr(1):=arr;
        RAISE NOTICE '二维数组(1)：%', nstarr(1);
END;
/
CALL test_nested_array();

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

DROP SCHEMA plpgsql_nested_array_and_record CASCADE;
