create schema parallel_enable_function;
set search_path=parallel_enable_function;

create table employees (employee_id number(6), department_id NUMBER, first_name varchar2(30), last_name varchar2(30), email varchar2(30), phone_number varchar2(30));

BEGIN  
   FOR i IN 1..100 LOOP  
      INSERT INTO employees VALUES (i, 60, 'abc', 'def', '123', '123');  
   END LOOP;  
   COMMIT;
END;  
/

CREATE TYPE my_outrec_typ AS (
    employee_id numeric(6,0),
    department_id numeric,
    first_name character varying(30),
    last_name character varying(30),
    email character varying(30),
    phone_number character varying(30)
);


-- create srf function with parallel_enable
CREATE OR REPLACE FUNCTION hash_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END hash_srf;
/

CREATE OR REPLACE FUNCTION any_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END any_srf;
/

-- create function with multi-partkey
CREATE OR REPLACE FUNCTION multi_partkey_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id, department_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END multi_partkey_srf;
/

-- create pipelined function
create type table_my_outrec_typ is table of my_outrec_typ;

CREATE OR REPLACE FUNCTION pipelined_table_f (p SYS_REFCURSOR) RETURN table_my_outrec_typ pipelined parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
      FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
      EXIT WHEN p%NOTFOUND;
	  pipe row(out_rec);
    END LOOP;
END pipelined_table_f;
/

CREATE OR REPLACE FUNCTION pipelined_array_f (p SYS_REFCURSOR) RETURN _employees PIPELINED parallel_enable (partition p by any) 
   IS
    in_rec my_outrec_typ;
   BEGIN
LOOP
    FETCH p INTO in_rec;
    EXIT WHEN p%NOTFOUND;
    PIPE ROW (in_rec);
    END LOOP;
END pipelined_array_f;
/

-- without partition by
CREATE OR REPLACE FUNCTION no_partition_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END no_partition_srf;
/

-- call function
set query_dop = 1002;
explain (costs off) select * from hash_srf(cursor (select * from employees)) limit 10;
select * from hash_srf(cursor (select * from employees)) limit 10;

explain (costs off) select * from any_srf(cursor (select * from employees)) limit 10;
select * from any_srf(cursor (select * from employees)) limit 10;

explain (costs off) select * from pipelined_table_f(cursor (select * from employees)) limit 10;
select * from pipelined_table_f(cursor (select * from employees)) limit 10;

explain (costs off) select * from multi_partkey_srf(cursor (select * from employees)) limit 10;
select * from multi_partkey_srf(cursor (select * from employees)) limit 10;

explain (costs off) select * from pipelined_array_f(cursor (select * from employees)) limit 10;
select * from pipelined_array_f(cursor (select * from employees)) limit 10;

explain (costs off) select * from no_partition_srf(cursor (select * from employees)) limit 10;
select * from no_partition_srf(cursor (select * from employees)) limit 10;

-- test count(*)
explain (costs off) select count(*) from hash_srf(cursor (select * from employees));
select count(*) from hash_srf(cursor (select * from employees));

-- test multi cursor args
CREATE OR REPLACE FUNCTION multi_cursor_srf (p1 SYS_REFCURSOR, p2 SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p1 by hash(employee_id)) IS
    out_rec_1 my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
    out_rec_2 my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p1 INTO out_rec_1.employee_id, out_rec_1.department_id, out_rec_1.first_name, out_rec_1.last_name, out_rec_1.email, out_rec_1.phone_number;  -- input row
        EXIT WHEN p1%NOTFOUND;
        FETCH p2 INTO out_rec_2.employee_id, out_rec_2.department_id, out_rec_2.first_name, out_rec_2.last_name, out_rec_2.email, out_rec_2.phone_number;  -- input row
        EXIT WHEN p2%NOTFOUND;
        return next out_rec_1;
    END LOOP;
    RETURN;
END multi_cursor_srf;
/

explain (costs off) select * from multi_cursor_srf(cursor (select * from employees), cursor (select * from employees)) limit 10;
select * from multi_cursor_srf(cursor (select * from employees), cursor (select * from employees)) limit 10;

explain (costs off) select count(*) from multi_cursor_srf(cursor (select * from employees), cursor (select * from employees));
select count(*) from multi_cursor_srf(cursor (select * from employees), cursor (select * from employees));

-- query dop reset after error
explain (costs off) select count(*) from multi_cursor_srf(cursor (select * from multi_cursor_srf(cursor (select * from employees))), cursor (select * from employees));
explain (costs off) select * from employees;

-- test top plan of cursor expr is not stream
explain (costs off) select count(*) from hash_srf(cursor (select * from employees limit 10)), employees;
select count(*) from hash_srf(cursor (select * from employees limit 10)), employees;

explain (costs off) select count(*) from hash_srf(cursor (select * from employees a ,employees b)), employees limit 10;
select count(*) from hash_srf(cursor (select * from employees a ,employees b)), employees limit 10;

-- test initplan not smp
explain (costs off) select 1, (select count(*) from hash_srf(cursor (select * from employees))) a from employees;

-- test plan hint
set query_dop = 1;
explain (costs off) select count(*) from hash_srf(cursor (select /*+ set(query_dop 1002) */ * from employees)); -- not smp
select count(*) from hash_srf(cursor (select /*+ set(query_dop 1002) */ * from employees));

explain (costs off) select /*+ set(query_dop 1002) */ count(*) from hash_srf(cursor (select * from employees)); -- not smp
select /*+ set(query_dop 1002) */ count(*) from hash_srf(cursor (select * from employees));

explain (costs off) select /*+ set(query_dop 1002) */ count(*) from hash_srf(cursor (select /*+ set(query_dop 1002) */ * from employees)); -- smp
select /*+ set(query_dop 1002) */ count(*) from hash_srf(cursor (select /*+ set(query_dop 1002) */ * from employees));
set query_dop = 1002;
-- nested function call
explain (costs off) select * from hash_srf(cursor (select * from hash_srf(cursor (select * from employees)))) limit 10;
select * from hash_srf(cursor (select * from hash_srf(cursor (select * from employees)))) limit 10;

-- functionscan join
explain (costs off) select * from hash_srf(cursor (select * from employees)) a, hash_srf(cursor (select * from employees)) b limit 10;
select * from hash_srf(cursor (select * from employees)) a, hash_srf(cursor (select * from employees)) b limit 10;

-- targetlist
explain (costs off) select hash_srf(cursor (select * from employees)) limit 10;
select hash_srf(cursor (select * from employees)) limit 10;

explain (costs off) select hash_srf(cursor (select * from employees)) from employees limit 10;
select hash_srf(cursor (select * from employees)) from employees limit 10;

-- subquery cannot smp
explain (costs off) select 1, (select count(*) from hash_srf(cursor (select * from employees)));
select 1, (select count(*) from hash_srf(cursor (select * from employees)));

-- test create or replace
CREATE OR REPLACE FUNCTION any_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END any_srf;
/

select parallel_cursor_seq, parallel_cursor_strategy, parallel_cursor_partkey from pg_proc_ext where proc_oid = 'any_srf'::regproc;

CREATE OR REPLACE FUNCTION any_srf (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END any_srf;
/

select parallel_cursor_seq, parallel_cursor_strategy, parallel_cursor_partkey from pg_proc_ext where proc_oid = 'any_srf'::regproc;

-- set provolatile. stable/volatile with parallel_enable would throw error
CREATE OR REPLACE FUNCTION stable_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ stable parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END stable_f;
/

CREATE OR REPLACE FUNCTION volatile_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ volatile parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
        LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END volatile_f;
/

CREATE OR REPLACE FUNCTION immutable_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ immutable parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END immutable_f;
/

-- Alter Function set volatile/stable would clear parallel_cursor info
alter function immutable_f(p SYS_REFCURSOR) volatile;
select parallel_cursor_seq, parallel_cursor_strategy, parallel_cursor_partkey from pg_proc_ext where proc_oid = 'immutable_f'::regproc;

alter function immutable_f(p SYS_REFCURSOR) stable;
alter function immutable_f(p SYS_REFCURSOR) immutable;

-- throw error when the operation of parallel cursor is not FETCH CURSOR
CREATE OR REPLACE FUNCTION invalid_opr_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH absolute 5 from p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END invalid_opr_f;
/

CREATE OR REPLACE FUNCTION invalid_opr_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH backward from p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END invalid_opr_f;
/

CREATE OR REPLACE FUNCTION invalid_opr_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH prior from p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END invalid_opr_f;
/

-- test specified non refcursor type
CREATE OR REPLACE FUNCTION invalid_type_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition a by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END invalid_type_f;
/

CREATE OR REPLACE FUNCTION invalid_type_f (p SYS_REFCURSOR, a int) RETURN setof my_outrec_typ parallel_enable (partition a by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH from p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END invalid_type_f;
/

-- create non-SRF/pipelined function
CREATE OR REPLACE FUNCTION return_int_f (p SYS_REFCURSOR) RETURN int parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
    res int := 0;
BEGIN
    LOOP
        FETCH from p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        res := res + 1;
    END LOOP;
    RETURN res;
END return_int_f;
/

explain (costs off) select * from return_int_f(cursor (select * from employees));
select * from return_int_f(cursor (select * from employees));

-- declare cursor
begin;
declare xc no scroll cursor for select * from employees;
explain select * from hash_srf('xc');
end;

-- test bulk collect
CREATE OR REPLACE FUNCTION bulk_collect_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by hash(employee_id)) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
    emp_tab table_my_outrec_typ;
BEGIN
    LOOP
        FETCH p bulk collect INTO emp_tab limit 5;  -- input row
        EXIT WHEN p%NOTFOUND;
        out_rec := emp_tab(emp_tab.first);
        return next out_rec;
    END LOOP;
    RETURN;
END bulk_collect_f;
/

explain (costs off) select count(*) from bulk_collect_f(cursor (select * from employees));
select count(*) from bulk_collect_f(cursor (select * from employees));

-- create package
create or replace package my_pkg as
    FUNCTION pkg_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by any);
end my_pkg;
/

create or replace package body my_pkg as
    FUNCTION pkg_f (p SYS_REFCURSOR) RETURN setof my_outrec_typ parallel_enable (partition p by any) IS
    out_rec my_outrec_typ := my_outrec_typ(NULL, NULL, NULL, NULL, NULL, NULL);
BEGIN
    LOOP
        FETCH p INTO out_rec.employee_id, out_rec.department_id, out_rec.first_name, out_rec.last_name, out_rec.email, out_rec.phone_number;  -- input row
        EXIT WHEN p%NOTFOUND;
        return next out_rec;
    END LOOP;
    RETURN;
END pkg_f;
end my_pkg;
/

explain (costs off) select count(*) from my_pkg.pkg_f(cursor (select * from employees));
select count(*) from my_pkg.pkg_f(cursor (select * from employees));

drop schema parallel_enable_function cascade;
