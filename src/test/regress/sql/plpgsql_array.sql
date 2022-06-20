-- FOR PL/pgSQL VARRAY scenarios --

-- check compatibility --
show sql_compatibility; -- expect A --

-- create new schema --
drop schema if exists plpgsql_array;
create schema plpgsql_array;
set current_schema = plpgsql_array;

-- initialize tables --
create table customers (
    id number(10) not null,
    c_name varchar2(50) not null,
    c_age number(8) not null,
    c_address varchar2(50),
    salary float(2) not null,
    constraint customers_pk primary key (id)
);

insert into customers (id, c_name, c_age, c_address, salary) values (1, 'Vera' ,32, 'Paris', 22999.00);
insert into customers (id, c_name, c_age, c_address, salary) values (2, 'Zera' ,25, 'London', 5999.00);
insert into customers (id, c_name, c_age, c_address, salary) values (3, 'Alice' ,22, 'Bangkok', 9800.98);
insert into customers (id, c_name, c_age, c_address, salary) values (4, 'Jim' ,26, 'Dubai', 18700.00);
insert into customers (id, c_name, c_age, c_address, salary) values (5, 'Kevin' ,28, 'Singapore', 18999.00);
insert into customers (id, c_name, c_age, c_address, salary) values (6, 'Gauss' ,42, 'Beijing', 32999.00);

create table tmp(a int, b varchar(100));

-- initialize functions, types etc. --
create type mytype as (
    id integer,
    biome varchar2(100)
);

create type mytype2 as (
    id integer,
    locale myType
);

-- it turns any input to (9, (1, 'space'))
create or replace function myfunc(habitat in mytype2)
return mytype2
is
    ret mytype2;
begin
    ret := (9, (1, 'space'));
    return ret;
end;
/

-- type and function shares the same name --
-- Oh~oh, what's gonna happened?? --
create type functype as (
    id integer,
    locale myType
);

create or replace function functype(habitat in mytype2)
return mytype2
is
    ret mytype2;
begin
    ret := (-1, (1, 'unknown realm'));
    return ret;
end;
/

-- test function datatype priority --
create or replace function name_list(inint in integer)
return integer
is
    ret integer;
begin
    ret := 1;
    return ret;
end;
/

----------------------------------------------------
------------------ START OF TESTS ------------------
----------------------------------------------------

-- general declare + assign + access --
-- support varray with parentheses in SQL --
DECLARE
    CURSOR c_customers is
    SELECT c_name FROM customers order by id;
    type c_list is varray (6) of customers.c_name%type;
    name_list c_list := c_list();
    counter integer := 0;
BEGIN 
    FOR n IN c_customers LOOP
        counter := counter + 1; -- 6 iterations --
        name_list.extend;
        name_list(counter) := n.c_name;
    END LOOP;

    insert into tmp values (NULL, name_list(1));
    insert into tmp values (NULL, name_list(3));
    name_list(2) := name_list(3);
    insert into tmp values (NULL, name_list[2]); -- same as last one --
END;
/

select * from tmp order by 1, 2;
truncate tmp;

-- ERROR: mix of parens and brackets are not allowed --
declare
    cursor c_customers is (select c_name from customers order by id);
    type c_list is varray(6) of customers.c_name%type;
    name_list c_list := c_list();
    counter integer := 0;
begin 
    for n in c_customers loop
        counter := counter + 1; -- 6 iterations --
        name_list.extend;
        name_list(counter) := n.c_name;
    end loop;

    insert into tmp values (null, name_list(1]);
end;
/

-- parentheses support in SQL 2 --
-- array of record --
declare
    cursor c_customers is (select * from customers order by id);
    type c_list is varray(6) of customers;
    customer_list c_list := c_list();
    counter integer := 0;
    name varchar2(50) := '';
begin
    for n in c_customers loop
        counter := counter + 1;
        customer_list.extend;
        customer_list(counter) := (n.id, n.c_name, n.c_age, n.c_address, n.salary); -- insert record --
        name := customer_list(counter).c_name;
        if customer_list(counter).c_age <= 30 then
            dbe_output.print_line('Individual who is below 30: ' || customer_list(counter).c_name);
        else
            dbe_output.print_line('Individual who is above 30: ' || name);
        end if;
        insert into tmp values (customer_list(counter).c_age, customer_list(counter).salary); -- parentheses --
    end loop;
end;
/

select * from tmp order by 1, 2;
truncate tmp;

-- batch initialization, batch insert varray--
declare
    type students is varray(6) of varchar2(10);
    type grades is varray(6) of integer;
    marks grades := grades('98', 97, 74 + 4, (87), 92, 100); -- batch initialize --
    names students default students('none'); -- default --
    total integer;
begin
    names := students();  -- should append NULL then do the coerce --
    names := students('Vera ', 'Zera ', 'Alice', 'Jim  ', 'Kevin', to_char('G') || 'auss'); -- batch insert --
    total := names.count;
    dbe_output.print_line('Total '|| total || ' Students'); 
    for i in 1 .. total loop
        dbe_output.print_line('Student: ' || names(i) || '  Marks: ' || marks(i));
    end loop;
end;
/

-- block above will be rewritten into this form (close to this form, but with parens and coerces)--
declare
    type students is varray(6) of varchar2(10);
    type grades is varray(6) of integer;
    marks grades := array['98', 97, 74 + 4, (87), 92, 100]; -- batch initialize --
    names students default array['none']; -- default --
    total integer;
begin
    names := array[NULL];
    names := array['Vera ', 'Zera ', 'Alice', 'Jim  ', 'Kevin', to_char('G') || 'auss']; -- batch insert --
    total := names.count;
    dbe_output.print_line('Total '|| total || ' Students'); 
    for i in 1 .. total loop
        dbe_output.print_line('Student: ' || names(i) || '  Marks: ' || marks(i));
    end loop;
end;
/

-- test of PL/SQL data type instantiation --
-- If we specified our type (use PL/SQL like instantiation), all varray members .. --
--   should be able to cast to the correct data type. --
declare
    type students is varray(5) of varchar2(10);
    names students;
begin
    names := students(1, 'Zera ', 'Alice', 'Jim  ', 'Kevin'); -- should be able read all values correctly --
    for i in 1 .. 5 loop
        dbe_output.print_line('Student: ' || names(i));
    end loop;
end;
/

-- However, if we use the PL/pgSQL style instantiation, it is not guaranteed --
-- error out for this one --
declare
    type students is varray(5) of varchar2(10);
    names students;
begin
    -- we can only make assumptions base on the first element, which, not always a good answer --
    names := array[1, 'Zera ', 'Alice', 'Jim  ', 'Kevin'];
    for i in 1 .. 5 loop
        dbe_output.print_line('Student: ' || names(i));
    end loop;
end;
/


-- test of uneven brackets --
-- error out --
declare
    type students is varray(5) of varchar2(10);
    names students;
begin
    names := students(1, 'Zera ', 'Alice', 'Jim  ', 'Kevin'); -- should be able read all values correctly --
    for i in 1 .. 5 loop
        dbe_output.print_line('Student: ' || names(i]);
    end loop;
end;
/

-- Using composite type defined outside of precedure block --
declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        mytype2(1, (1, 'ground')),
        mytype2(1, (2, 'air'))
    );
begin
    aa.extend(10);
    aa(2) := (2, (3, 'water')); -- overwrite record (1, (2, 'air')) --
    dbe_output.print_line('locale id is: ' || aa(1).id);
    dbe_output.print_line('biome 1.3 is: ' || aa(2).locale.biome); -- ... water (not air) --
end;
/

-- Note: array can handle proper type-in-type declaration --
declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        mytype2(1, mytype(1, 'ground')),
        mytype2(1, mytype(2, 'air'))
    );
begin
    aa.extend(10);
    aa(2) := (2, (3, 'water')); -- overwrite record (1, (2, 'air')) --
    dbe_output.print_line('locale id is: ' || aa(1).id);
    dbe_output.print_line('biome 1.3 is: ' || aa(2).locale.biome); -- ... water (not air) --
end;
/

declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        mytype2(1, mytype(1, 'ground')),
        mytype2(1, mytype(2, 'air'))
    );
begin
    aa.extend(10);
    aa(2) := mytype2(2, mytype(3, 'water'));
    dbe_output.print_line('locale id is: ' || aa(1).id);
    dbe_output.print_line('biome 1.3 is: ' || aa(2).locale.biome); -- ... water (not air) --
end;
/

-- working with functions --
-- should be the same, except the result, make sure functions are correctly identified --
declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        myfunc((1, mytype(1, 'ground'))), -- for records, we need an extra parens to work --
        myfunc((1, mytype(2, 'air')))
    );
begin
    aa.extend(10);
    dbe_output.print_line('locale id is: ' || aa(1).id);
    dbe_output.print_line('biome 9.1 is: ' || aa(2).locale.biome); -- ... space! --
end;
/

-- This is what going to happened with functions and types shares teh same name --
-- (Don't try this at home) --
declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        functype(1, mytype(1, 'ground')), -- we are prioritizing types here --
        functype(1, mytype(2, 'air'))
    );
begin
    aa.extend(10);
    dbe_output.print_line('locale id is: ' || aa(1).id);
    dbe_output.print_line('biome 1.2 is: ' || aa(2).locale.biome); -- air --
end;
/

drop type functype; -- abandon type functype --

declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        functype((1, mytype(1, 'ground'))), -- here we have to use function functype --
        functype((1, mytype(2, 'air')))
    );
begin
    aa.extend(10);
    dbe_output.print_line('locale ?? is: ' || aa(1).id);
    dbe_output.print_line('biome ??? is: ' || aa(2).locale.biome); -- weird places --
end;
/

drop function functype; -- oops! --

declare
    type finaltype is varray(10) of mytype2;
    aa finaltype := finaltype(
        functype((1, mytype(1, 'ground'))), -- not sure --
        functype((1, mytype(2, 'air')))
    );
begin
    aa.extend(10);
    dbe_output.print_line('This message worth 300 tons of gold (once printed).');
end;
/

-- Multi-dimension arrays --
declare
    type arrayfirst is varray(10) of int;
    arr arrayfirst := arrayfirst(1, 2, 3);
    mat int[][] := ARRAY[arr, arr]; -- PLpgSQL style --
begin
    dbe_output.print_line('The magic number is: ' || mat(1)(2)); -- should be 2 --
end;
/

-- assignments && statements test --
declare
    type arrayfirst is varray(10) of int;
    arr arrayfirst := arrayfirst(1, 2, 3);
    mat int[][] := ARRAY[arr, ARRAY[4, 5 ,6]]; -- PLpgSQL style --
begin
    dbe_output.print_line('The magic number is: ' || mat[2](1)); -- should be 4 --
    mat[1](3) = mat(2)[3];
    dbe_output.print_line('The magic number is: ' || mat[1](3)); -- should be 6 --

    insert into tmp(a) values (mat[1](2)), (mat(1)[2]), (mat(1)(2)), (mat[1][2]);
end;
/

select * from tmp order by 1, 2;
truncate tmp;

-- error out! --
declare
    type arrayfirst is varray(10) of int;
    arr arrayfirst := arrayfirst(1, 2, 3);
    type arraySecond is varray(10) of arrayfirst; -- Nested types are not supported, yet --
    mat arraySecond := arraySecond(arr, arr);
begin
    dbe_output.print_line('The magic number is: ' || mat(1)(2)); -- should be 2 --
end;
/

-- Should be empty --
create or replace procedure pro1() as  
declare
    type students is varray(5) of varchar2(10);
    names students := students();
begin 
	raise NOTICE '%', names;
    raise NOTICE '%', names.count;
end;
/

call pro1();

-- constant! --
declare
    type ta is table of varchar(100);
    tb constant ta := ta('10','11');
begin
    tb(1) := 12;
    dbe_output.print_line(tb[1]);
end;
/

declare
    type ta is table of varchar(100);
    tb constant ta := ta('10','11');
begin
    tb := ta('12','13');
    dbe_output.print_line(tb[1]);
end;
/

-- nested array --
create or replace package pckg_test as
    type rec1 is record(col1 varchar2);
    type t_arr is table of rec1;
    type rec2 is record(col1 t_arr, col2 t_arr);
    type t_arr1 is table of rec2;
procedure proc_test();
end pckg_test;
/

create or replace package body pckg_test as
procedure proc_test() as
v_arr t_arr1;
v_rec rec1;
begin
    v_arr(1).col1 := array[ROW('hello')];
    v_arr(1).col2 := array[ROW('world')];
    v_rec := v_arr(1).col2[1];                      -- normal bracket --
    raise notice '%', v_arr(1).col2(1);             -- parentheses --
    insert into tmp(b) values (v_arr(1).col2(1));   -- sql --
end;
end pckg_test;
/
call pckg_test.proc_test();
select * from tmp order by 1, 2;


CREATE OR REPLACE FUNCTION myarray_sort (ANYARRAY)
RETURNS ANYARRAY LANGUAGE SQL AS $$
SELECT ARRAY(
    SELECT $1[s.i] AS "foo"
    FROM
        generate_series(array_lower($1,1), array_upper($1,1)) AS s(i) 
    ORDER BY foo
);
$$; 

select myarray_sort(array[9,8,7,1,2,35]);

create table testtbl (plg_id varchar2);

declare
 type array_vchar is VARRAY(20) of varchar2;
 plg_id array_vchar := array_vchar();
  ans int;
 begin
 select count(1) into ans from testtbl where plg_id = plg_id(1);
end;

drop table testtbl;
select array_cat(
    array_cat(
        array_extendnull(
            array_cat('[2147483645:2147483647]={1,2,3}'::int[], array[4]),
            1
        ),
        array_extendnull(
            array_cat('[2147483645:2147483647]={10,20,30}'::int[], array[40]),
            1
        )
    ),
    array[0,0,0,0,0,0,0,0,0]
) as result;

DECLARE
id_arr int[];
BEGIN
id_arr[2047483630] = 1;
id_arr[2147483647] = 1;
id_arr[2047483641] = 1;
raise notice '%,%',id_arr[2147483644],id_arr[2147483645];
END;
/

drop table if exists cve_2021_32027_tb;
create table cve_2021_32027_tb (i int, p point);
insert into cve_2021_32027_tb(p) values('(1,1)');
update cve_2021_32027_tb set p[2147483647] = 0 returning *;
update cve_2021_32027_tb set p[268435456] = 0 returning *;
update cve_2021_32027_tb set p[536870912] = 0 returning *;

--------------------------------------------------
------------------ END OF TESTS ------------------
--------------------------------------------------
drop table if exists cve_2021_32027_tb;
drop package if exists pckg_test;
drop procedure if exists pro1;
drop function if exists functype;
drop function if exists myfunc;
drop function if exists myarray_sort;
drop table if exists tmp;
drop table if exists customers;
drop type if exists functype;
drop type if exists mytype2;
drop type if exists mytype;

-- clean up --
drop schema if exists plpgsql_array cascade;
