-- FOR PL/pgSQL ARRAY of RECORD TYPE scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_record;
create schema plpgsql_record;
set search_path=plpgsql_record;



-- initialize table and type--
CREATE TABLE DCT_DATACLR_LOG(TYPE int NOT NULL ENABLE);

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- test TYPE as a table col name
create or replace package p_test1 as
    TYPE IN_CLEANLOG_TYPE IS RECORD(IN_TYPE DCT_DATACLR_LOG.TYPE%TYPE);
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE;
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE as
        va IN_CLEANLOG_TYPE;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;	
end p_test1;
/

select p_test1.f1(ROW(3));

-- test TYPE as a col name of record
create or replace package p_test1 as
    TYPE IN_CLEANLOG_TYPE IS RECORD(TYPE int);
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE;
end p_test1;
/
create or replace package body p_test1 as
    function f1(ss in IN_CLEANLOG_TYPE) return IN_CLEANLOG_TYPE as
        va IN_CLEANLOG_TYPE;
	begin
        va := ss;
        raise info '%',va;
        return va;
	end;	
end p_test1;
/

select p_test1.f1(ROW(3));

--test RECORD col name of exist type and var name
create or replace package p_test2 is
    type array_type is varray(10) of int;
    type tab_type is table of int;
    type r_type is record (a int, b int);
    va array_type;
    vb tab_type;
    vc r_type;
    type IN_CLEANLOG_TYPE is record (array_type int, tab_type int, r_type int, va int, vb int, vc int);
    function f1(ss in IN_CLEANLOG_TYPE) return int;
end p_test2;
/
create or replace package body p_test2 as
    function f1(ss in IN_CLEANLOG_TYPE) return int as
        vaa IN_CLEANLOG_TYPE;
	begin
        vaa := ss;
        raise info '%',vaa;
        return vaa.va;
	end;	
end p_test2;
/

select p_test2.f1((1,2,3,4,5,6));

--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop package p_test2;
drop package p_test1;

-- clean up --
drop schema if exists plpgsql_record cascade;
