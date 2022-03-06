-- FOR VARRAY INTERFACE --

-- check compatibility --
show sql_compatibility;  -- expect A --

-- create new schema --
drop schema if exists plpgsql_array_interface;
create schema plpgsql_array_interface;
set current_schema = plpgsql_array_interface;

-- test array interface count --
create or replace procedure array_interface_p1()
as
declare
    colors varchar[] := array['red','orange',null,'','green','blue','indigo','violet'];
    colors1 int[] := array[1,2,null,3,'',4,null,5,6,null,null,7,8];
    colors2 varchar[] := array['red','orange','null','green','blue',null,'indigo','violet'];
begin
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
    raise NOTICE '%', colors2;
    raise NOTICE '%', colors2.count;
    raise NOTICE '%', colors1;
    raise NOTICE '%', colors1.count;
end;
/

call array_interface_p1();

-- test array interface count --
create or replace procedure array_interface_p2()
as
declare
    colors varchar[] := array['red','orange','green','blue','indigo','violet'];
    colors1 int[] := array[1,2,3,4,5,6,7,8];
begin
    raise NOTICE '%', colors;
    colors[1] := null;
    colors[4] := null;
    colors[6] := '';
    raise NOTICE '%', colors;
    raise NOTICE '%', colors1;
    colors1[1] := null;
    colors1[4] := null;
    colors1[6] := '';
    raise NOTICE '%', colors1;
end;
/

call array_interface_p2();

-- test array interface exists --
create or replace procedure array_interface_p3()
as
declare
    colors varchar[] := array[null,'red','orange',null,'green','','blue',null,'indigo','violet',null];
    ind int := 1;
    colors1 varchar[] := array['null','red','orange',null,'green','blue',null,'indigo','violet',null];
begin
    raise NOTICE '--------------------colors--------------------------';
    raise NOTICE '%', colors;
    for ind in 1..colors.last
    loop
        raise NOTICE '%', colors[ind];
        raise NOTICE '%', colors.exists(ind);
        if colors.exists(ind) then
            raise NOTICE '    exists';
        else 
            raise NOTICE '    not exists';
        end if;
        raise NOTICE '----------------';
    end loop;

    raise NOTICE '--------------------colors1--------------------------';
    raise NOTICE '%', colors1;
    for ind in 1 .. colors1.last
    loop
        raise NOTICE '%', colors1[ind];
        raise NOTICE '%', colors1.exists(ind);
        if colors1.exists(ind) then
            raise NOTICE '    exists';
        else 
            raise NOTICE '    not exists';
        end if;
        raise NOTICE '----------------';
    end loop;
end;
/

call array_interface_p3();

-- test array interface exists --
create or replace procedure array_interface_p4()
as
declare
    colors int[] := array[1,2,'',3,4,null,5,6,7,8];
    ind int := 1;
    colors1 int[] := array[null,1,2,3,4,null,5,6,'',7,8,null];
begin
    raise NOTICE '--------------------colors--------------------------';
    raise NOTICE '%', colors;
    for ind in 1 .. colors.last
    loop
        raise NOTICE '%', colors[ind];
        raise NOTICE '%', colors.exists(ind);
        if colors.exists(ind) then
            raise NOTICE '    exists';
        else 
            raise NOTICE '    not exists';
        end if;
        raise NOTICE '----------------';
    end loop;

    raise NOTICE '--------------------colors1--------------------------';
    raise NOTICE '%', colors1;
    for ind in 1 .. colors1.last
    loop
        raise NOTICE '%', colors1[ind];
        raise NOTICE '%', colors1.exists(ind);
        if colors1.exists(ind) then
            raise NOTICE '    exists';
        else 
            raise NOTICE '    not exists';
        end if;
        raise NOTICE '----------------';
    end loop;
end;
/

call array_interface_p4();

-- test array interface first and last --
create or replace procedure array_interface_p5()
as
declare
    colors1 varchar[] := array['red','orange',null,'green','','blue'];
    colors2 varchar[] := array['red','orange',null,'green','blue',null];
    colors3 varchar[] := array[null,'red','orange',null,'green','blue'];
    colors4 int[] := array[null,1,2,3,4,null,5,6,7,8,null,''];
begin
    raise NOTICE '---------colors1---------';
    raise NOTICE '%', colors1;
    raise NOTICE 'colors1 first number: %', colors1.first;
    raise NOTICE 'colors1 first: %', colors1[colors1.first];
    raise NOTICE 'colors1 last number: %', colors1.last;
    raise NOTICE 'colors1 last: %', colors1[colors1.last];

    raise NOTICE '---------colors2---------';
    raise NOTICE '%', colors2;
    raise NOTICE 'colors2 first number: %', colors2.first;
    raise NOTICE 'colors2 first: %', colors2[colors2.first];
    raise NOTICE 'colors2 last number: %', colors2.last;
    raise NOTICE 'colors2 last: %', colors2[colors2.last];

    raise NOTICE '---------colors3---------';
    raise NOTICE '%', colors3;
    raise NOTICE 'colors3 first number: %', colors3.first;
    raise NOTICE 'colors3 first: %', colors3[colors3.first];
    raise NOTICE 'colors3 last number: %', colors3.last;
    raise NOTICE 'colors3 last: %', colors3[colors3.last];

    raise NOTICE '---------colors4---------';
    raise NOTICE '%', colors4;
    raise NOTICE 'colors4 first number: %', colors4.first;
    raise NOTICE 'colors4 first: %', colors4[colors4.first];
    raise NOTICE 'colors4 last number: %', colors4.last;
    raise NOTICE 'colors4 last: %', colors4[colors4.last];
end;
/

call array_interface_p5();

-- next&prior
create or replace procedure array_interface_p6()
as
declare
    colors1 varchar[] := array['red','orange',null,'green','blue','','indigo','violet'];
    colors2 int[]:=array[1,2,3,null,4,5,6,'',7,8];
    colors3 int[]:=array[null,1,2,3,null,4,5,'',6,7,8,null];
    ind int := 1;
begin
    raise NOTICE '--------------------colors1---------------------';
    raise NOTICE '%', colors1;
    for ind in  1 .. colors1.last
    loop
        raise NOTICE 'current is: %', colors1[ind];
        raise NOTICE 'next number is: %', colors1.next(ind);
        raise NOTICE 'next is: %', colors1[colors1.next(ind)];
        raise NOTICE 'prior number is: %', colors1.prior(ind);
        raise NOTICE 'prior is: %', colors1[colors1.prior(ind)];
        raise NOTICE '-------';
    end loop;

    raise NOTICE '--------------------colors2---------------------';
    raise NOTICE '%', colors2;
    for ind in  1 .. colors2.last
    loop
        raise NOTICE 'current is: %', colors2[ind];
        raise NOTICE 'next number is: %', colors2.next(ind);
        raise NOTICE 'next is: %', colors2[colors2.next(ind)];
        raise NOTICE 'prior number is: %', colors2.prior(ind);
        raise NOTICE 'prior is: %', colors2[colors2.prior(ind)];
        raise NOTICE '-------';
    end loop;
    raise NOTICE '--------------------colors3---------------------';
    raise NOTICE '%', colors3;
    for ind in  1 .. colors3.last
    loop
        raise NOTICE 'current is: %', colors3[ind];
        raise NOTICE 'next number is: %', colors3.next(ind);
        raise NOTICE 'next is: %', colors3[colors3.next(ind)];
        raise NOTICE 'prior number is: %', colors3.prior(ind);
        raise NOTICE 'prior is: %', colors3[colors3.prior(ind)];
        raise NOTICE '-------';
    end loop;
end;
/

call array_interface_p6();

-- test empty array exists interface return
create or replace procedure array_interface_p7()
as
declare
    colors1 varchar[] := array[]::varchar[];
    colors2 integer[]:= array[]::integer[];
    vi varchar2(32);
begin
    raise NOTICE 'colors1 is %', colors1;
    raise NOTICE 'colors2 is %', colors2;
    vi := 111;
    raise NOTICE 'colors1[%] exists return %', vi, colors1.exists(vi);
    vi := '1';
    raise NOTICE 'colors1["%"] exists return %', vi, colors1.exists(vi);
    vi := 123432;
    raise NOTICE 'colors2[%] exists return %', vi, colors2.exists(vi);
    vi := '43243442';
    raise NOTICE 'colors2["%"] exists return %', vi, colors2.exists(vi);
end;
/

call array_interface_p7();

-- test array exists interface string input parameter
create or replace procedure array_interface_p8()
as
declare
    colors1 varchar2[]  := array['11', '12', '13'];
    line varchar[]:=array['--------------------------------'];
    chk boolean := false;
begin
    raise NOTICE'%', colors1;
    chk := colors.exists(2);
    raise NOTICE'check exists return %', chk;
end;
/

--call array_interface_p8();

-- test array exists interface A.B input parameter
create or replace procedure array_interface_p9()
as
declare
    v_a  varchar2[] := array[]::varchar2[];
begin
    raise NOTICE 'v_a is %', v_a;
    for rec in (select generate_series(1,10) x) loop
        if v_a.exists(rec.x) then
            raise NOTICE 'v_a[%] is exist', rec.x;
        else
            raise NOTICE 'v_a[%] is not exist', rec.x;
        end if;
    end loop;
    v_a.extend(10);
    for i in 1 .. 10 loop
        v_a(i) := i;
    end loop;
    raise NOTICE 'v_a is %', v_a;
    for rec in (select generate_series(1,10) x) loop
        if v_a.exists(rec.x) then
            raise NOTICE 'v_a[%] is exist', rec.x;
        else
            raise NOTICE 'v_a[%] is not exist', rec.x;
        end if;
    end loop;
end;
/

call array_interface_p9();

create or replace procedure array_interface_p10() as
declare
    colors varchar2[];
begin
    colors := array['red','orange','yellow','green','blue','indigo','violet','c8','c9','c10','c11','c12','c13','c14','c15'];
    if colors.exists(1+1) then
        raise NOTICE 'array exist, element is %', colors[1+1];
    else
        raise NOTICE 'array not exist';
    end if;
	if colors.exists('1' || '2') then
        raise NOTICE 'array exist, element is %', colors['1'||'2'];
    else
        raise NOTICE 'array not exist';
    end if;
end;
/

call array_interface_p10();

create or replace procedure array_interface_p11() as
declare
    colors varchar2[];
begin
    colors := array['red','orange','yellow','green','blue','indigo','violet','c8','c9','c10','c11','c12','c13','c14','c15'];
    if colors.exists(1+1) then
        raise NOTICE 'array exist, element is %', colors[1+1];
    else
        raise NOTICE 'array not exist';
    end if;
	if colors.exists('1'||'2') then
        raise NOTICE 'array exist, element is %', colors['1'||'2'];
    else
        raise NOTICE 'array not exist';
    end if;
end;
/

call array_interface_p11();

create or replace procedure array_interface_p12() as
declare
    colors varchar2[];
begin
    colors := array['red','orange','yellow','green','blue','indigo','violet'];
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
    raise NOTICE '%', colors.count();
    raise NOTICE '%', colors.first;
    raise NOTICE '%', colors.first();
    raise NOTICE '%', colors.last;
    raise NOTICE '%', colors.last();
    for i in colors.first .. colors.last loop
        raise NOTICE '%', colors[i];
    end loop;
    for i in 1 .. colors.count loop
        raise NOTICE '%', colors[i];
    end loop;
    for i in colors.first() .. colors.last() loop
        raise NOTICE '%', colors[i];
    end loop;
    for i in 1 .. colors.count() loop
        raise NOTICE '%', colors[i];
    end loop;
    colors.extend;
    raise NOTICE '%', colors;
    colors.extend();
    raise NOTICE '%', colors;
    colors.extend(2);
    raise NOTICE '%', colors;
    colors.delete;
    raise NOTICE '%', colors;
    colors.extend();
    raise NOTICE '%', colors;
    colors.delete();
    raise NOTICE '%', colors;
end;
/

call array_interface_p12();


-- clean up --
drop schema if exists plpgsql_array_interface cascade;
