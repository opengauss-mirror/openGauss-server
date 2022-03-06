-- setups
drop schema if exists pl_debugger cascade;
create schema pl_debugger;
set search_path = pl_debugger;
create table test(a int, b varchar(40), c timestamp);
insert into test values (2, 'Giving to the Needy', '2020-02-02');
insert into test values (3, 'Prayer', '2021-12-02');
insert into test values (5, 'Fasting', '2030-03-02');
insert into test values (7, 'Treasures in Heaven', '2040-04-02');

CREATE OR REPLACE FUNCTION test_debug(x int) RETURNS SETOF test AS
$BODY$
DECLARE
    sql_stmt VARCHAR2(500);
    r test%rowtype;
    rec record;
    b_tmp text;
    cnt int;
    a_tmp int;
    cur refcursor;
    n_tmp NUMERIC(24,6);
    t_tmp tsquery;
    CURSOR cur_arg(criterion INTEGER) IS
        SELECT * FROM test WHERE a < criterion;
BEGIN
    cnt := 0;
    FOR r IN SELECT * FROM test
    WHERE a > x
    LOOP
        RETURN NEXT r;
    END LOOP;

    FOR rec in SELECT * FROM test
    WHERE a < x
    LOOP
        RETURN NEXT rec;
    END LOOP;

    FORALL index_1 IN 0..1
        INSERT INTO test VALUES (index_1, 'Happy Children''s Day!', '2021-6-1');

    SELECT b FROM test where a = 7 INTO b_tmp;
    sql_stmt := 'select a from test where b = :1;';
    OPEN cur FOR sql_stmt USING b_tmp;
    IF cur%isopen then LOOP
        FETCH cur INTO a_tmp;
        EXIT WHEN cur%notfound;
        END LOOP;
    END IF;
    CLOSE cur;
    WHILE cnt < 3 LOOP
        cnt := cnt + 1;
    END LOOP;

    RAISE INFO 'cnt is %', cnt;

    RETURN;

END
$BODY$
LANGUAGE plpgsql;

create table show_code_table(lineno int, code text, canBreak bool);

do $$
declare
    funcoid oid;
begin
    select oid from pg_proc into funcoid where proname = 'test_debug';
    INSERT INTO show_code_table SELECT * FROM dbe_pldebugger.info_code(funcoid);
end;
$$;

select * from show_code_table;

create table debug_info(nodename text, port smallint);

create function turn_on_debugger(funcname text)
    returns int
as $$
declare
    funcoid bigint;
    cnt int;
begin
    select count(*) from pg_proc into cnt where proname = funcname;
    if cnt != 1 then
        raise exception 'There are multiple/no function with name %', funcname;
    end if;

    select oid from pg_proc into funcoid where proname = funcname;
    insert into debug_info select * from dbe_pldebugger.turn_on(funcoid);
    return 0;
end
$$ language plpgsql;

-- next without attach
select * from dbe_pldebugger.next();

-- info locals without attach
select * from dbe_pldebugger.info_locals();

-- attach fail (target not turned on)
select * from dbe_pldebugger.attach('sgnode', 1);

-- abort without attach
select * from dbe_pldebugger.abort();

-- turn off without turn on
select * from dbe_pldebugger.turn_off(1);

-- turn on dropped function
CREATE OR REPLACE FUNCTION pld_simple(a int)
 RETURNS boolean
AS $$
declare
begin
    insert into test values(a);
    return true;
end
$$ language plpgsql;

do $$
declare
    funcoid bigint;
begin
    select oid from pg_proc into funcoid where proname = 'pld_simple';
    drop function pld_simple;
    insert into debug_info select * from dbe_pldebugger.turn_on(funcoid);
end;
$$;

truncate debug_info;

-- turn on language sql function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'sql' limit 1;
-- turn on language java function (no such thing)
-- turn on language internal function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'internal' limit 1;
-- turn on language c function
select dbe_pldebugger.turn_on(f.oid) from pg_proc f, pg_language l where f.prolang = l.oid and l.lanname = 'c' limit 1;
-- turn on correctly
truncate debug_info;

select * from turn_on_debugger('test_debug');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

-- start debug - 1st run
select * from test_debug(4);

-- start debug - 2nd run - to be aborted
select * from test_debug(4);

-- commit/rollback in procedure
create table tb1(a int);
create or replace procedure test_debug2 as
begin
    insert into tb1 values (1000);
    commit;
    insert into tb1 values (2000);
    rollback;
end;
/

truncate debug_info;

select * from turn_on_debugger('test_debug2');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug';

-- start debug
select * from test_debug2();


-- test for implicit variables
CREATE OR REPLACE function test_debug3(a in integer) return integer
AS
declare
b int;
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
        ELSE
            b := 999;
    END CASE;
    raise info 'pi_return : %',pi_return ;
    return b;
    EXCEPTION WHEN others THEN
        b := 101;
    return b;
END;
/

truncate debug_info;

select * from turn_on_debugger('test_debug3');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug2';

-- start debug
select * from test_debug3(1);



-- test for step into
CREATE OR REPLACE FUNCTION test_debug4(a in integer) return integer
AS
declare
b int;
BEGIN
    CASE a
        WHEN 1 THEN
            b := 111;
            call test_debug(a);
        ELSE
            b := 999;
    END CASE;
    return b;
    raise info 'pi_return : %',pi_return ;
    EXCEPTION WHEN others THEN
        b := 101;
    return b;
END;
/

truncate debug_info;

select * from turn_on_debugger('test_debug4');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select * from test_debug4(1);

-- test with client error in exception
select * from test_debug4(1);

-- test with breakpoint
select * from test_debug4(1);

-- test with finish without encountered breakpoint
select * from test_debug4(1);

-- test with finish with encountered breakpoint and inner error
insert into test values(generate_series(1,10)); -- this will make test_debug() raise more-than-one-row exception
select * from test_debug4(1);

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug3';

select dbe_pldebugger.turn_off(oid) from pg_proc where proname = 'test_debug4';

create or replace function test_debug_recursive (ct int, pr int)
returns table (counter int, product int)
language plpgsql
as $$
begin
    return query select ct, pr;
    if ct < 5 then
        return query select * from test_debug_recursive(ct+ 1, pr * (ct+ 1));
    end if;
end $$;

truncate debug_info;

select * from turn_on_debugger('test_debug_recursive');

select * from test_debug_recursive (1, 1);

--test empty procedure
CREATE OR REPLACE PROCEDURE test_empty(i int,j out int)
AS
DECLARE
begin

end;
/

truncate debug_info;

select * from turn_on_debugger('test_empty');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

call test_empty(1, '');

-- test set_var
CREATE OR REPLACE PROCEDURE test_setvar(x int) AS
DECLARE
    vint int8;
    vnum NUMERIC(24,6);
    vfloat float;
    vtext text;
    vvarchar VARCHAR2(500);
    vrow test%rowtype;
    vrec record;
    vrefcursor refcursor;
    vconst constant smallint;
    vpoint point;
BEGIN
    RAISE INFO E'vint:%\nvnum:%\nvfloat:%\nvtext:%\nvvarchar:%\nvrow:%\nvrefcursor:%',
        vint, vnum, vfloat, vtext, vvarchar, vrow, vrefcursor;
    COMMIT;
    SELECT * FROM test ORDER BY 1 LIMIT 1 INTO vrow; -- do set var here
    RAISE INFO E'vint:%\nvnum:%\nvfloat:%\nvtext:%\nvvarchar:%\nvrow:%\nvrefcursor:%',
        vint, vnum, vfloat, vtext, vvarchar, vrow, vrefcursor;
    ROLLBACK;
    RAISE INFO E'vint:%\nvnum:%\nvfloat:%\nvtext:%\nvvarchar:%\nvrow:%\nvrefcursor:%',
        vint, vnum, vfloat, vtext, vvarchar, vrow, vrefcursor;
END
/

truncate debug_info;

select * from turn_on_debugger('test_setvar');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

call test_setvar(0);

-- test package

create or replace PACKAGE z_pk2
AS
 a int := 10;
 type t1 is record(c1 varchar2, c2 int);
 type t2 is table of t1;
 type t3 is varray(10) of int; 
END z_pk2;
/

create or replace PACKAGE z_pk
AS
  function pro1(p1 int,p2 int ,p3 VARCHAR2(5)) return int;
  PROCEDURE pro2(p1 int,p2 out int,p3 inout varchar(20));
  b int := 2;
  type t1 is record(c1 varchar2, c2 int);
  type t2 is table of t1;
  type t3 is varray(10) of int; 
END z_pk;
/

create or replace PACKAGE BODY z_pk
AS
  
  function pro1(p1 int,p2 int ,p3 VARCHAR2(5)) return int
  as
  aa t1;
  bb z_pk2.t1;
  cc z_pk2.t2;
  dd z_pk2.t3;
  p4 int;
  BEGIN
      select 'aa',2 into aa;
      select 'bb',2 into bb;
      cc(1) = ('aa',1);
      dd(1) = 10; 
      p4 := 0;
      if p3 = '+' then 
          p4 := p1 + p2 + z_pk2.a;
       end if;
       
       if p3 = '-' then 
          p4 := p1 - p2;
       end if;
       
       if p3 = '*' then 
          p4 := p1 * p2;
       end if;
       
       if p3 = '/' then 
          p4 := p1 / p2;
       end if;    
  return p4;
  END;

  PROCEDURE pro2(p1 int,p2 out int,p3 inout varchar(20))
  AS
  BEGIN
      p2 := p1;
      p3 := p1 ||'___a';
      --select dsuser.test_func_p1(1,5);
  END;
  
END z_pk; 

/

truncate debug_info;

select * from turn_on_debugger('pro1');

select * from dbe_pldebugger.local_debug_server_info();

select * from debug_info;

select z_pk.pro1(1,2,'+');

drop schema pl_debugger cascade;
