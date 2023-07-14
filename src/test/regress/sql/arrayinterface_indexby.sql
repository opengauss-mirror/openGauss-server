-- FOR VARRAY INTERFACE --

-- check compatibility --
show sql_compatibility;  -- expect A --

-- create new schema --
drop schema if exists plpgsql_array_interface_indexby;
create schema plpgsql_array_interface_indexby;
set current_schema = plpgsql_array_interface_indexby;

-- must be call in procedure --
select array_indexby_delete(array[1, 2]);


-- test array interface count --
create or replace procedure array_interface_p1() as
declare
    type ta is table of varchar(32) index by varchar;
    colors ta;  -- array['red', 'orange', null, '', 'green', 'blue', 'indigo', 'violet']
begin
    colors('a1234567') := 'red';
	colors('a12345678') := 'orange';
    colors('a2345671') := null;
    colors('a3456712') := '';
    colors('a4567123') := 'green';
    colors('a5671234') := 'blue';
    colors('a6712345') := 'indigo';
    colors('a7123456') := 'violet';
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
    colors[1] := null;
    colors[4] := null;
    colors[6] := '';
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
end;
/

call array_interface_p1();

create or replace procedure array_interface_p1() as
declare
    type ta is table of varchar(32) index by integer;
    colors ta;  -- array['red', 'orange', null, '', 'green', 'blue', 'indigo', 'violet']
begin
    colors(5) := 'red';
    colors(-1) := 'orange';
    colors(2) := null;
    colors(8) := '';
    colors(-6) := 'green';
    colors(10) := 'blue';
    colors(-3) := 'indigo';
    colors(3) := 'violet';
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
    colors[1] := null;
    colors[4] := null;
    colors[6] := '';
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
end;
/

call array_interface_p1();

-- test array interface exists --
create or replace procedure array_interface_p2() as
declare
    type ta is table of varchar(32) index by varchar;
    colors ta;  -- array[null,'red','orange',null,'green','','blue',null,'indigo','violet',null]
    ind varchar2(32);
begin
    colors('1') := null;
    colors('2') := 'red';
    colors('3') := 'orange';
    colors('4') := null;
    colors('5') := 'green';
    colors('6') := '';
    colors('7') := 'blue';
    colors('8') := null;
    colors('9') := 'indigo';
    colors('10') := 'violet';
    colors('11') := null;
    raise NOTICE '--------------------colors--------------------------';
    raise NOTICE '%', colors;
	ind := colors.first;
	raise NOTICE '%', ind;
    while colors.exists(ind) loop
        raise NOTICE '%:%', ind, colors(ind);
        ind := colors.next(ind);
    end loop;
end;
/

call array_interface_p2();

-- test array interface exists --
create or replace procedure array_interface_p2() as
declare
    type ta is table of varchar(32) index by integer;
    colors ta;  -- array[null,'red','orange',null,'green','','blue',null,'indigo','violet',null]
    ind varchar2(32);
begin
    colors(5) := null;
    colors(-3) := 'orange';
    colors(-5) := null;
    colors(-1) := 'green';
    colors(0) := '';
    colors(3) := 'indigo';
    colors(1) := 'blue';
    colors(-2) := null;
    colors(2) := null;
    colors(-4) := 'red';
    colors(4) := 'violet';
    
    raise NOTICE '--------------------colors--------------------------';
    raise NOTICE '%', colors;
    ind := colors.first;
    raise NOTICE '%', ind;
    while colors.exists(ind) loop
        raise NOTICE '%:%', ind, colors(ind);
        ind := colors.next(ind);
    end loop;
end;
/

call array_interface_p2();

-- test array interface exists --
create or replace procedure array_interface_p3() as
declare
    type ta is table of integer index by varchar;
    colors ta; -- array[1,2,'',3,4,null,5,6,7,8,9]
    ind varchar2(32);
begin
    colors('a') := 1;
    colors('ab') := 2;
    colors('ba') := '';
    colors('bab') := 3;
    colors('bb') := 4;
    colors('bc') := null;
    colors('ca') := 5;
    colors('cb') := 6;
    colors('cab') := 7;
    colors('cba') := 8;
    colors('cbb') := 9;
    raise NOTICE '--------------------colors--------------------------';
    raise NOTICE '%', colors;
	ind := colors.first;
	raise NOTICE '%', ind;
    while colors.exists(ind) loop
        raise NOTICE '%:%', ind, colors[ind];
        raise NOTICE '%', colors.exists(ind);
		ind := colors.next(ind);
    end loop;
end;
/

call array_interface_p3();

-- test array interface first and last --
create or replace procedure array_interface_p4() as
declare
    type ta is table of varchar(32) index by varchar;
    type tb is table of integer index by varchar;
    colors1 ta; -- array['red','orange',null,'green','','blue']
    colors2 ta; -- array['red','orange',null,'green','blue',null]
    colors3 ta; -- array[null,'red','orange',null,'green','blue']
    colors4 tb; -- array[null,1,2,3,4,null,5,6,7,8,null,'']
begin
    colors1('123') := 'red';
    colors1('132') := 'orange';
    colors1('213') := null;
    colors1('231') := 'green';
    colors1('312') := '';
    colors1('321') := 'blue';
    raise NOTICE '---------colors1---------';
    raise NOTICE '%', colors1;
    raise NOTICE 'colors1 first number: %', colors1.first;
    raise NOTICE 'colors1 first: %', colors1[colors1.first];
    raise NOTICE 'colors1 last number: %', colors1.last;
    raise NOTICE 'colors1 last: %', colors1[colors1.last];

    colors2('abc') := 'red';
    colors2('acb') := 'orange';
    colors2('bac') := null;
    colors2('bca') := 'green';
    colors2('cab') := 'blue';
    colors2('cba') := null;
    raise NOTICE '---------colors2---------';
    raise NOTICE '%', colors2;
    raise NOTICE 'colors2 first number: %', colors2.first;
    raise NOTICE 'colors2 first: %', colors2[colors2.first];
    raise NOTICE 'colors2 last number: %', colors2.last;
    raise NOTICE 'colors2 last: %', colors2[colors2.last];

    colors3('a1') := null;
    colors3('a2') := 'red';
    colors3('b1') := 'orange';
    colors3('ba') := null;
    colors3('b2') := 'green';
    colors3('a0') := 'blue';
    raise NOTICE '---------colors3---------';
    raise NOTICE '%', colors3;
    raise NOTICE 'colors3 first number: %', colors3.first;
    raise NOTICE 'colors3 first: %', colors3[colors3.first];
    raise NOTICE 'colors3 last number: %', colors3.last;
    raise NOTICE 'colors3 last: %', colors3[colors3.last];

    colors4('a312') := null;
    colors4('a123') := 1;
    colors4('b1') := 2;
    colors4('ba') := 3;
    colors4('b0') := 4;
    colors4('a0') := null;
    colors4('b1') := 5;
    colors4('bc') := 6;
    colors4('bb') := 7;
    colors4('c1') := 8;
    colors4('ca') := null;
    colors4('cb') := '';
    raise NOTICE '---------colors4---------';
    raise NOTICE '%', colors4;
    raise NOTICE 'colors4 first number: %', colors4.first;
    raise NOTICE 'colors4 first: %', colors4[colors4.first];
    raise NOTICE 'colors4 last number: %', colors4.last;
    raise NOTICE 'colors4 last: %', colors4[colors4.last];
end;
/

call array_interface_p4();

-- test array interface first and last --
create or replace procedure array_interface_p4() as
declare
    type ta is table of varchar(32) index by integer;
    colors1 ta; -- array['red','orange',null,'green','','blue']
begin
    colors1(132) := 'orange';
    colors1(321) := 'blue';
    colors1(213) := null;
    colors1(123) := 'red';
    colors1(231) := 'green';
    colors1(312) := '';
    
    raise NOTICE '---------colors1---------';
    raise NOTICE '%', colors1;
    raise NOTICE 'colors1 first number: %', colors1.first;
    raise NOTICE 'colors1 first: %', colors1[colors1.first];
    raise NOTICE 'colors1 last number: %', colors1.last;
    raise NOTICE 'colors1 last: %', colors1[colors1.last];
end;
/

call array_interface_p4();

-- next&prior
create or replace procedure array_interface_p5() as
declare
    type ta is table of varchar(32) index by varchar;
    type tb is table of integer index by varchar;
    colors1 ta; -- array['red','orange',null,'green','blue','','indigo','violet']
    colors2 tb; -- array[1,2,3,null,4,5,6,'',7,8]
	ind varchar2(32);
	tmp varchar2(32);
begin
    colors1('1') := 'red';
    colors1('2') := 'orange';
    colors1('3') := null;
    colors1('4') := 'green';
    colors1('5') := 'blue';
    colors1('6') := '';
    colors1('7') := 'indigo';
    colors1('8') := 'violet';
    raise NOTICE '--------------------colors1---------------------';
    raise NOTICE '%', colors1;
	ind := colors1.first;
    while colors1.exists(ind) loop
        raise NOTICE 'current is: %', colors1[ind];
        raise NOTICE 'next index is: %', colors1.next(ind);
		tmp := colors1.next(ind);
		if tmp is null then
        raise NOTICE 'next element is: %', tmp;
		else
		raise NOTICE 'next element is: %', colors1[tmp];
		end if;
        raise NOTICE 'prior index is: %', colors1.prior(ind);
		tmp := colors1.prior(ind);
		if tmp is null then
        raise NOTICE 'prior element is: %', tmp;
		else
		raise NOTICE 'prior element is: %', colors1[tmp];
		end if;
        raise NOTICE '-------slash-------';
		ind := colors1.next(ind);
    end loop;

    colors1('a') := 1;
    colors1('b') := 2;
    colors1('c') := 3;
    colors1('d') := null;
    colors1('e') := 4;
    colors1('f') := 5;
    colors1('g') := 6;
    colors1('h') := '';
    colors1('i') := 7;
    colors1('j') := 8;
    raise NOTICE '--------------------colors2---------------------';
    raise NOTICE '%', colors2;
    ind := colors1.first;
    while colors1.exists(ind) loop
        raise NOTICE 'current is: %', colors2[ind];
        raise NOTICE 'next index is: %', colors2.next(ind);
        raise NOTICE 'next element is: %', colors2[colors2.next(ind)];
        raise NOTICE 'prior index is: %', colors2.prior(ind);
        raise NOTICE 'prior element is: %', colors2[colors2.prior(ind)];
        raise NOTICE '-----------';
		ind := colors1.next(ind);
    end loop;
end;
/

call array_interface_p5();

create or replace procedure array_interface_p5() as
declare
    type ta is table of varchar(32) index by integer;
    colors1 ta; -- array['red','orange',null,'green','blue','','indigo','violet']
    ind varchar2(32);
    tmp varchar2(32);
begin
    colors1(-15) := 'red';
    colors1(-8) := 'orange';
    colors1(-1) := null;
    colors1(0) := 'green';
    colors1(10) := 'blue';
    colors1(24) := '';
    colors1(45) := 'indigo';
    colors1(50) := 'violet';
    raise NOTICE '--------------------colors1---------------------';
    raise NOTICE '%', colors1;
    ind := colors1.first;
    while colors1.exists(ind) loop
        raise NOTICE 'current is: %', colors1[ind];
        raise NOTICE 'next index is: %', colors1.next(ind);
        tmp := colors1.next(ind);
        if tmp is null then
        raise NOTICE 'next element is: %', tmp;
        else
        raise NOTICE 'next element is: %', colors1[tmp];
        end if;
        raise NOTICE 'prior index is: %', colors1.prior(ind);
        tmp := colors1.prior(ind);
        if tmp is null then
        raise NOTICE 'prior element is: %', tmp;
        else
        raise NOTICE 'prior element is: %', colors1[tmp];
        end if;
        raise NOTICE '-------slash-------';
        ind := colors1.next(ind);
    end loop;
end;
/

call array_interface_p5();

-- test empty array exists interface return
create or replace procedure array_interface_p6() as
declare
    type ta is table of varchar(32) index by varchar;
    type tb is table of integer index by varchar;
    colors1 ta := array[]::varchar[];
    colors2 tb := array[]::integer[];
    vi varchar2(32);
begin
    raise NOTICE 'colors1 is %', colors1;
    raise NOTICE 'colors1 length is %', colors1.count;
    raise NOTICE 'colors1 first is %', colors1.first;
    raise NOTICE 'colors1 last is %', colors1.last;
    raise NOTICE 'colors2 is %', colors2;
    raise NOTICE 'colors2 length is %', colors2.count;
    raise NOTICE 'colors2 first is %', colors2.first;
    raise NOTICE 'colors2 last is %', colors2.last;
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

call array_interface_p6();

-- test empty array exists interface return
create or replace procedure array_interface_p6() as
declare
    type ta is table of varchar(32) index by integer;
    type tb is table of integer index by integer;
    colors1 ta := array[]::varchar[];
    colors2 tb := array[]::integer[];
    vi varchar2(32);
begin
    raise NOTICE 'colors1 is %', colors1;
    raise NOTICE 'colors1 length is %', colors1.count;
    raise NOTICE 'colors1 first is %', colors1.first;
    raise NOTICE 'colors1 last is %', colors1.last;
    raise NOTICE 'colors2 is %', colors2;
    raise NOTICE 'colors2 length is %', colors2.count;
    raise NOTICE 'colors2 first is %', colors2.first;
    raise NOTICE 'colors2 last is %', colors2.last;
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

call array_interface_p6();

-- test array exists interface A.B input parameter
create or replace procedure array_interface_p7() as
declare
    type ta is table of varchar(32) index by varchar;
    v_a  ta := array[]::varchar2[];
begin
    raise NOTICE 'v_a is %', v_a;
    for rec in (select generate_series(1,10) x) loop
        if v_a.exists(rec.x) then
            raise NOTICE 'v_a[%] is exist', rec.x;
        else
            raise NOTICE 'v_a[%] is not exist', rec.x;
        end if;
    end loop;
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

call array_interface_p7();

-- test array exists interface A.B input parameter
create or replace procedure array_interface_p7() as
declare
    type ta is table of varchar(32) index by integer;
    v_a  ta := array[]::varchar2[];
begin
    raise NOTICE 'v_a is %', v_a;
    for rec in (select generate_series(1,10) x) loop
        if v_a.exists(rec.x) then
            raise NOTICE 'v_a[%] is exist', rec.x;
        else
            raise NOTICE 'v_a[%] is not exist', rec.x;
        end if;
    end loop;
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

call array_interface_p7();

create or replace procedure array_interface_p8() as
declare
    type ta is table of varchar(32) index by varchar;
    colors ta;
begin
    -- colors := array['red','orange','yellow','green','blue','indigo','violet','c8','c9','c10','c11','c12','c13','c14','c15'];
    colors('0') := 'red';
    colors('1') := 'orange';
    colors('2') := 'yellow';
    colors('3') := 'green';
    colors('4') := 'blue';
    colors('5') := 'indigo';
    colors('6') := 'violet';
    colors('7') := 'c8';
    colors('8') := 'c9';
    colors('9') := 'c10';
    colors('10') := 'c11';
    colors('11') := 'c12';
    colors('12') := 'c13';
    colors('13') := 'c14';
    colors('14') := 'c15';
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

call array_interface_p8();

create or replace procedure array_interface_p8() as
declare
    type ta is table of varchar(32) index by integer;
    colors ta;
begin
    -- colors := array['red','orange','yellow','green','blue','indigo','violet','c8','c9','c10','c11','c12','c13','c14','c15'];
    colors(0) := 'red';
    colors(1) := 'orange';
    colors(2) := 'yellow';
    colors(3) := 'green';
    colors(4) := 'blue';
    colors(5) := 'indigo';
    colors(6) := 'violet';
    colors(7) := 'c8';
    colors(8) := 'c9';
    colors(9) := 'c10';
    colors(10) := 'c11';
    colors(11) := 'c12';
    colors(12) := 'c13';
    colors(13) := 'c14';
    colors(14) := 'c15';
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

call array_interface_p8();

create or replace procedure array_interface_p9() as
declare
    type ta is table of varchar(32) index by varchar;
    colors ta;
begin
    -- colors := array['red','orange','yellow','green','blue','indigo','violet'];
    colors('1') := 'red';
    colors('2') := 'orange';
    colors('3') := 'yellow';
    colors('4') := 'green';
    colors('5') := 'blue';
    colors('6') := 'indigo';
    colors('7') := 'violet';
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
    colors.delete('7');
    raise NOTICE '%', colors;
    colors.delete('1');
    raise NOTICE '%', colors;
	colors.delete('13424');
    raise NOTICE '%', colors;
    colors.delete();
    raise NOTICE '%', colors;
	raise NOTICE '%', colors.count;
	raise NOTICE '%', colors.first;
	raise NOTICE '%', colors.last;
	raise NOTICE '%', colors.next('1');
	raise NOTICE '%', colors.prior('1');
	raise NOTICE '%', colors;
	colors.delete('1');
    raise NOTICE '%', colors;
end;
/

call array_interface_p9();

create or replace procedure array_interface_p9() as
declare
    type ta is table of varchar(32) index by integer;
    colors ta;
begin
    -- colors := array['red','orange','yellow','green','blue','indigo','violet'];
    colors(1) := 'red';
    colors(2) := 'orange';
    colors(3) := 'yellow';
    colors(4) := 'green';
    colors(5) := 'blue';
    colors(6) := 'indigo';
    colors(7) := 'violet';
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
    colors.delete(7);
    raise NOTICE '%', colors;
    colors.delete(1);
    raise NOTICE '%', colors;
    colors.delete(13424);
    raise NOTICE '%', colors;
    colors.delete();
    raise NOTICE '%', colors;
    raise NOTICE '%', colors.count;
    raise NOTICE '%', colors.first;
    raise NOTICE '%', colors.last;
    raise NOTICE '%', colors.next(1);
    raise NOTICE '%', colors.prior(1);
    raise NOTICE '%', colors;
    colors.delete(1);
    raise NOTICE '%', colors;
end;
/

call array_interface_p9();

declare
type ta is table of varchar2(10) index by varchar2;
va ta;
var varchar(10);
begin
va('a1') = 'a';
va('a2') = 'b';
va('a3') = 'c';
va('aaa') = 'd';
var = 'a';
raise notice '%' , va.exists('a'||'2');
raise notice '%' , va.exists('a'||'4');
if va.exists('a'|| var ||'a') then
raise NOTICE 'aaa exists';
else
raise NOTICE 'not exists';
end if;
raise notice '%' , va.next('a'||'2');
raise notice '%' , va.prior('a'||'2');
raise notice '%' , va(va.prior('a'||'2'));
raise notice '%' , va(va.first());
raise notice '%' , va(va.last());
end;
/

declare
type ta is table of varchar2(10) index by integer;
va ta;
var varchar(10);
begin
va(11) = 'a';
va(12) = 'b';
va(13) = 'c';
va('111') = 'd';
var = '1';
raise notice '%' , va.exists('1'||'2');
raise notice '%' , va.exists('1'||'4');
if va.exists('1'|| var ||'1') then
raise NOTICE '111 exists';
else
raise NOTICE 'not exists';
end if;
raise notice '%' , va.next('1'||'2');
raise notice '%' , va.prior('1'||'2');
raise notice '%' , va(va.prior('1'||'2'));
raise notice '%' , va(va.first());
raise notice '%' , va(va.last());
end;
/

declare 
   type t_arr is table of number index by varchar2(20);
   v_arr t_arr;
begin
   if v_arr.exists('1'||'12')=false then
      raise info 'not exists';
   end if;
end;
/

declare 
   type t_arr is table of number index by integer;
   v_arr t_arr;
begin
   if v_arr.exists(12)=false then
      raise info 'not exists';
   end if;
end;
/

declare 
   type t_arr is varray(10) of number;
   v_arr t_arr;
begin
   if v_arr.exists(12)=false then
      raise info 'not exists';
   end if;
end;
/

create or replace procedure indexbychar1()
as
type ta is table of varchar2(10) index by varchar2;
va ta;
var varchar(10);
begin
va('a1') = 'a';
va('a2') = 'b';
va('a3') = 'c';
va('aaa') = 'd';
var = 'a';
raise notice '%' , va.exists('a'||'2');
raise notice '%' , va.exists('a'||'4');
if va.exists('a'|| var ||'a') then
raise NOTICE 'aaa exists';
else
raise NOTICE 'not exists';
end if;
raise notice '%' , va.next('a'||'2');
raise notice '%' , va.prior('a'||'2');
raise notice '%' , va(va.prior('a'||'2'));
raise notice '%' , va(va.first());
raise notice '%' , va(va.last());
raise notice '%' , va.count;
va.delete();
raise notice '%' , va.count;
raise notice '%' , va(va.last());
exception when others then
raise notice 'delete all the array items';
for i in 1..10 loop
va(i) := 'va'||(i::varchar(2));
END LOOP;
raise notice '%', va.COUNT;
raise notice '%', va(1);
raise notice '%', va(10);
raise notice 'first%', va.FIRST;
raise notice 'last%', va.LAST;
raise notice '%', va;
raise notice '%', va(va.LAST);
raise notice '%', va(va.NEXT(va.FIRST));
raise notice '%', va(va.PRIOR(va.LAST));
end;
/

call indexbychar1();

create table pkgtbl085 (c1 int,c2 number,c3 varchar2(30),c4 clob,c5 text,c6 blob);
insert into pkgtbl085 values(1,1,'var1','clob1','text1','bb1');
insert into pkgtbl085 values(2,2,'var2','clob2','text2','bb2');
insert into pkgtbl085 values(3,3,'var3','clob3','text3','bb3');
--I2.table of index by varchar2(20)
create or replace package pkg085
as
type ty1 is table of varchar2(20) index by varchar2(20);
type ty2 is record (c1 number,c2 pkgtbl085%rowtype);
procedure p1();
end pkg085;
/

create or replace package body pkg085
as
procedure p1()
is
va ty2;
vb ty1;
numcount int;
begin
for i in 1..3 loop
select c3 into va.c2.c3 from pkgtbl085 where c1=i;
if va.c2.c3 is not null then
vb(va.c2.c3)=va.c2.c3;
end if;
end loop;
raise info 'vb is %',vb;
raise info 'vb.count is %',vb.count;
raise info 'vb(va.c2,c3) is %',vb(va.c2.c3);
raise info 'va.c2.c3 is %',va.c2.c3;
raise info 'vb.prior(va.c2.c3) is %',vb.prior(va.c2.c3);
raise info 'vb.prior(var3) is %',vb.prior('var3');
va.c2.c3='var1';
raise info 'vb.next(va.c2.c3) is %',vb.next(va.c2.c3);
raise info 'vb.exists(va.c2.c3) is %',vb.exists(va.c2.c3);
if vb.exists(va.c2.c3) then
raise notice 'true';
end if;
end;
end pkg085;
/

call pkg085.p1();
drop package pkg085;
drop table pkgtbl085;

create or replace procedure array_interface_p10() as
declare
    type ta is table of varchar(32) index by varchar;
    c1 ta;
    c2 ta;
begin
    if c1.first = c2.first then
	null;
	end if;
end;
/

create or replace procedure array_interface_p10() as
declare
    type ta is table of varchar(32) index by varchar;
    c1 ta;
    c2 ta;
begin
    raise info '%', c1.next(c2.first);
end;
/

create or replace procedure tableof_delete_1()
is
type ty4 is table of integer index by varchar;
pv4 ty4;
begin
pv4('1'):=2;
pv4('-1'):=-1;
pv4.delete('-1');
raise info '%', pv4('1');
raise info '%', pv4('-1');
raise info '%', pv4;
end;
/

call tableof_delete_1();

create or replace procedure tableof_delete_2()
is
type ty4 is table of integer index by integer;
pv4 ty4;
begin
pv4(1):=2;
pv4(-1):=-1;
pv4.delete(-1);
raise info '%', pv4(1);
raise info '%', pv4(-1);
raise info '%', pv4;
end;
/

call tableof_delete_2();

create or replace procedure tableof_delete_3()
is
type ty4 is varray(10) of integer;
pv4 ty4;
begin
pv4(1):=2;
pv4(-1):=-1;
pv4(-2):=-2;
raise info '%', pv4;
pv4.delete(-1);
raise info '%', pv4(1);
raise info '%', pv4(-1);
raise info '%', pv4;
end;
/

call tableof_delete_3();

create or replace procedure tableof_delete_4()
is
type ty4 is varray(10) of integer;
pv4 ty4;
begin
pv4(4):=2;
pv4(3):=-1;
pv4(2):=-2;
raise info '%', pv4;
pv4.delete(3);
raise info '%', pv4(4);
raise info '%', pv4;
end;
/

call tableof_delete_4();

create or replace procedure tableof_delete_5()
is
type ty4 is varray(10) of integer;
pv4 ty4;
a integer;
begin
a = 1;
pv4(1):=2;
pv4(-1):=-1;
pv4(-2):=-2;
raise info '%', pv4;
pv4.delete(-a);
raise info '%', pv4(1);
raise info '%', pv4(-1);
raise info '%', pv4;
end;
/

call tableof_delete_5();

-- clean up --
drop schema if exists plpgsql_array_interface_indexby cascade;
