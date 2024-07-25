create schema plpgsql_pipelined;
set search_path to plpgsql_pipelined;

CREATE TYPE t_tf_row AS (
    id          NUMBER,
    description VARCHAR2(50)
);
CREATE TYPE t_tf_tab IS TABLE OF t_tf_row;

-- res tuple
CREATE OR REPLACE FUNCTION get_tab_ptf(p_rows in number) returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
declare result t_tf_row;
begin
    for i in 1 .. p_rows loop
        result.id = i;
        result.description = 'Descrption for ' || i;
        pipe row(null);
        pipe row(result);
    end loop;
end;
$BODY$;

select * from get_tab_ptf(2);
select get_tab_ptf(2);
select * from unnest(get_tab_ptf(2));
select * from table(get_tab_ptf(2));
select unnest(get_tab_ptf(2));

-- res value
create type int_arr is table of int;
CREATE OR REPLACE FUNCTION  get_table_of_int(p_rows in number) returns int_arr pipelined  LANGUAGE plpgsql AS
$BODY$
begin
for i in 1 .. p_rows loop
        pipe row(null);
        pipe row(i);
end loop;
return;
end;
$BODY$;

select * from get_table_of_int(2);
select get_table_of_int(2);
select * from unnest(get_table_of_int(2));
select * from table(get_table_of_int(2));
select unnest(get_table_of_int(2));


-- res tauple autom
CREATE OR REPLACE FUNCTION get_tab_ptf_autom(p_rows in number) returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
declare result t_tf_row;
PRAGMA AUTONOMOUS_TRANSACTION;
begin
    for i in 1 .. p_rows loop
        result.id = i;
        result.description = 'Descrption for ' || i;
        pipe row(null);
        pipe row(result);
    end loop;
end;
$BODY$;

select * from get_tab_ptf_autom(2);
select get_tab_ptf_autom(2);
select * from unnest(get_tab_ptf_autom(2));
select * from table(get_tab_ptf_autom(2));
select unnest(get_tab_ptf_autom(2));

-- res value
CREATE OR REPLACE FUNCTION  get_table_of_int_autom(p_rows in number) returns _int4 pipelined  LANGUAGE plpgsql AS
$BODY$
declare
PRAGMA AUTONOMOUS_TRANSACTION;
begin
for i in 1 .. p_rows loop
        pipe row(null);
        pipe row(i);
end loop;
return;
end;
$BODY$;

select * from get_table_of_int_autom(2);
select get_table_of_int_autom(2);
select * from unnest(get_table_of_int_autom(2));
select * from table(get_table_of_int_autom(2));
select unnest(get_table_of_int_autom(2));


/* RewriteNAutomResultIfPipelinedFunc estate->tuple_store == NULL breanch */
CREATE OR REPLACE FUNCTION do_nothing_func(p_rows in number) returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
begin
end;
$BODY$;
select  do_nothing_func(1);

-- rec test
CREATE OR REPLACE PACKAGE pkg_pi AUTHID DEFINER AS TYPE rec as record(r1 int, r2 int);
TYPE recset_arr IS table OF rec;
FUNCTION fuc(n INTEGER) RETURN recset_arr PIPELINED;
END pkg_pi;
/
CREATE OR REPLACE PACKAGE BODY pkg_pi AS
 FUNCTION fuc(n INTEGER) RETURN recset_arr PIPELINED IS
BEGIN
declare
    r rec;
BEGIN
FOR i IN 1..n LOOP
		  r.r1 =i;
          r.r2=i+1;
pipe row(r);
END LOOP;
END;
END;
END pkg_pi;
/
SELECT * from pkg_pi.fuc(6);


CREATE TABLE stocktable
(
    ticker      VARCHAR2(20),
    trade_date  DATE,
    open_price  NUMBER,
    close_price NUMBER
);
INSERT INTO stocktable select 'STK' || indx, SYSDATE, indx, indx + 15 from generate_series(1,100) as indx;

-- ROWTYPE(PLPGSQL_DTYPE_CURSORROW)/record(PLPGSQL_DTYPE_REC) test
CREATE OR REPLACE FUNCTION stockpivot_pl () RETURN _stocktable PIPELINED
   IS
    CURSOR c IS SELECT * FROM stocktable;
    in_rec c%ROWTYPE;
    in_record record;
   BEGIN
    open c;
LOOP
FETCH c INTO in_rec;
FETCH c INTO in_record;
EXIT WHEN c%NOTFOUND;
PIPE ROW (in_rec);
PIPE ROW (in_record);
END LOOP;
END;
/
select count(*) from stockpivot_pl();

-- lower workmem
set work_mem = '64kB';
create or replace type type_0022 as (c1 integer, c2 tinyint);
create or replace type tb_type_0022 as table of type_0022;
create or replace function func_pipelined_022(count in number)
    returns tb_type_0022 pipelined language plpgsql as
$BODY$
declare result type_0022;
begin
for i in 1 .. count loop
result.c1 = 123;
result.c2 = 32;
pipe row(result);
pipe row(null);
end loop;
return;
end;
$BODY$;
select count(*) from func_pipelined_022(10000);
select func_pipelined_022(10);
select * from func_pipelined_022(0);
select func_pipelined_022(0);
reset work_mem;

-- nest function call for ereport
create table test(id int);
CREATE OR REPLACE FUNCTION insert_test() returns VOID LANGUAGE plpgsql AS
$BODY$
    DECLARE PRAGMA AUTONOMOUS_TRANSACTION;
begin
    insert into test select * from get_table_of_int(1);
end;
$BODY$;

CREATE OR REPLACE FUNCTION get_tab_ptf_failed() returns t_tf_tab pipelined LANGUAGE plpgsql AS
$BODY$
declare result t_tf_row;
begin
    perform insert_test();
    insert into test values(5);
end;
$BODY$;

select get_tab_ptf_failed();

CREATE OR REPLACE PACKAGE pkg0016 AS
TYPE array_type_0016_1 AS varray(10) OF char(10);
FUNCTION func_pipelined_0016(count NUMBER) RETURN array_type_0016_1 pipelined;
END pkg0016;
/

CREATE OR REPLACE PACKAGE BODY pkg0016 AS
FUNCTION func_pipelined_0016(count NUMBER) RETURN array_type_0016_1 pipelined IS
declare result array_type_0016_1;
BEGIN
FOR i IN 1..count LOOP
result := '{1}';
pipe row(result);
pipe row(1);
pipe row(date'2022-01-01');
pipe row(123456.1);
pipe row(-123456.1);
END LOOP;
RETURN;
END;
END pkg0016;
/
select pkg0016.func_pipelined_0016(2);

CREATE OR REPLACE PACKAGE BODY pkg0016 AS
FUNCTION func_pipelined_0016(count NUMBER) RETURN array_type_0016_1 pipelined IS
declare result array_type_0016_1;
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
FOR i IN 1..count LOOP
result := '{1}';
pipe row(result);
pipe row(1);
pipe row(date'2022-01-01');
pipe row(123456.1);
pipe row(-123456.1);
END LOOP;
RETURN;
END;
END pkg0016;
/

select pkg0016.func_pipelined_0016(2);
select * from pkg0016.func_pipelined_0016(2);
reset search_path;
drop schema plpgsql_pipelined cascade;
