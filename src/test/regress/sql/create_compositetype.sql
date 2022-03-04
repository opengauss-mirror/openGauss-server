CREATE TYPE compfoo AS (f1 int, f2 text);

/*Verify type name*/
create type "01jfiejriouieraejijiosjoerqkjou含有中文字符feoajfoeuitoooeiajfofeoiwurouyy" AS (c1 int, c2 int);
drop type  "01jfiejriouieraejijiosjoerqkjou含有中文字符feoajfoeuitoooeiajfo";
create type pg_class as (a int);
drop type public.pg_class;

create table t_group (a int, b compfoo) WITH (orientation=row);
insert into t_group values (1,(1,'Simon1'));
insert into t_group values (2,(2,'Simon2'));
insert into t_group values (3,(3,'Simon3'));
insert into t_group select * from t_group where b = cast((1,'syr') as compfoo);
select * from t_group order by 1;
update t_group set b = (3,'syr3') where b = cast((2,'syr2') as compfoo);
delete from t_group where b = cast((1,'syr1') as compfoo);
create view v1 as select * from t_group;
CREATE TYPE compfoo_v1 AS (f1 int, f2 v1);

/*enum type support sort,comparison operators and aggregate functions*/
SELECT * FROM t_group WHERE (b).f1 > 1 order by 1;
SELECT * FROM t_group WHERE (b).f1 > 1 ORDER BY (b).f1;
SELECT count(*) FROM t_group WHERE (b).f1 > 1;
SELECT * FROM t_group WHERE b = cast((1,'Simon1') as compfoo);

/*Verify the compositetype as the function input parameter or return value*/
CREATE FUNCTION compositetype_func1(compfoo) RETURNS compfoo
AS 'select b from t_group where b = $1;'
LANGUAGE SQL;
select compositetype_func1(cast((1,'syr1') as compfoo));
CREATE FUNCTION compositetype_func2() RETURNS compfoo
AS 'select b from t_group where a = 3;'
LANGUAGE SQL;
select compositetype_func2();
create table t_func1 (c int, b compfoo);
create function compositetype_func3(c int, v compfoo) returns void as $$
insert into t_func1 values (c, v);
$$ language sql;
create or replace function compositetype_func4(c int, v compfoo) returns void as $$
select compositetype_func3(c, v);
$$ language sql;
select compositetype_func3(1,(5, 'Simon5'));
select compositetype_func4(2,(6, 'Simon6'));
select * from t_func1;

drop function if exists compositetype_func1;
drop function if exists compositetype_func2;
drop function if exists compositetype_func3;
drop function if exists compositetype_func4;
drop table if exists t_func1;

/*Verify supported table types:row table, column table(not supported), temp table, replication table, partition table, foreign table*/
create table t_group_rep (a int, b compfoo) WITH (orientation=row) ;
insert into t_group_rep values (1,(1,'syr1'));
insert into t_group_rep values (2,(2,'syr2'));
insert into t_group_rep select * from t_group_rep where b = cast((1,'syr') as compfoo);
select * from t_group_rep;
update t_group_rep set b = (3,'syr3') where b = cast((2,'syr2') as compfoo);
delete from t_group_rep where b = cast((1,'syr1') as compfoo);
create temp table t_type1(a int, b compfoo);
insert into t_type1 values (1, 'sad');
select * from t_type1;
create table t_type2(a compfoo, b compfoo, c date) partition by range(c)(partition p1 values less than('2020-01-01'),partition p2 values less than(maxvalue));
insert into t_type2 values (1, 'happy', '2019-05-07');
select * from t_type2;
create table t_type3(a int, b compfoo)with(orientation=column);
create table t_type4 (a compfoo);
/*
create foreign table t_type5(a int, b compfoo)
SERVER gsmpp_server
OPTIONS(location 'gsfs://000.000.000.000:8098',
format 'text',
delimiter '|',
encoding 'utf8'
)write only;
insert into t_type5 select * from t_type1;
create foreign table t_type6(a int, b compfoo)
SERVER gsmpp_server
OPTIONS(location 'gsfs://000.000.000.000:8098/t_type5.*',  
format 'text',
delimiter '|',
encoding 'utf8'
)read only;
select * from t_type6;
*/

drop table if exists t_group_rep;
drop table if exists t_type1;
drop table if exists t_type2;
drop table if exists t_type3;
drop table if exists t_type4;
/*
drop foreign table if exists t_type5;
drop foreign table if exists t_type6;
*/

/*Verify the data type supported by the composite type*/
--string type
create type comp1 as(c1 char(10), c2 varchar(100), c3 text, c4 clob);
create table t_type1(a serial, b comp1);
insert into t_type1(b) values(('a','abc','×ÜÎñºì¼õ·Ê°¡','jeoifjÔÚ#$@!#$½¨ê±½ð¶î·Ça121'));
select * from t_type1;
drop table t_type1;
drop type comp1;

--date/time type
create type comp1 as(c1 date, c2 timestamp with time zone, c3 INTERVAL DAY(3) TO SECOND (4));
create table t_type1(a serial, b comp1) ;
insert into t_type1(b) values(('2019-03-04','2019-03-04 17:50:23 +8', interval '2' day));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--geometric type
create type comp1 as(c1 bool, c2 point, c3 lseg, c4 box, c5 path, c6 polygon, c7 circle);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values((1,'(1,2)','((1,2),(3,4))','((1,2),(3,4))','((1,2))','((1,2))','<(5,1),3>'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--website address type
create type comp1 as(c1 cidr, c2 inet, c3 macaddr);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values(('192.168.0.0/24','127.0.0.1','08-00-2b-01-02-03'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--bit string type
create type comp1 as(c1 bytea, c2 bit(3));
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values(('abc','1001'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--text search type
create type comp1 as(c1 tsvector,c2 tsquery);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values((null,'adfad'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--json type
create type comp1 as(c1 json, c2 json);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values(('{"a": "aaa in bbb", "b": 123, "c": 456, "d": true, "f": false, "g": null}'::json, '{"js": [123, "123", null, {"key": "value"}]}'::json));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--others
create type comp1 as(c1 oid,c2 cid, c3 xid, c4 tid, c5 uuid);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values((1,'','',null,'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--array type
create type comp1 as(c1 int[],c2 char[],c3 date[]);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values(('{1,2,3}','{a,bc,c}','{}'));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

--range type
create type comp1 as(c1 int4range, c2 tstzrange);
create temp table t_type1(a int default 1, b comp1);
insert into t_type1(b) values((int4range(1, 1, '[]'),'[2010-01-01 01:00:00 -05, 2010-01-01 02:00:00 -08)'::tstzrange));
select * from t_type1;
drop type comp1 cascade;
drop table t_type1;

/*type nesting*/
create type typ1 as (x1 float8, y1 float8);
create type typ2 as (x2 typ1, y2 typ1);
create table t_group_nesting1 (a int, b typ2);
insert into t_group_nesting1 (a, b.x2.x1, b.x2.y1, b.y2.x1, b.y2.y1) values(44,55,66,77,88);
select * from t_group_nesting1;
create type typ3 as (x3 typ2, y3 typ2, z3 typ2);
create table t_group_nesting2 (a int, b typ3);
insert into t_group_nesting2 (a, b.x3.x2.x1, b.y3.x2.y1, b.x3.y2.x1, b.y3.y2.y1, b.z3.x2.x1, b.z3.y2.y1) values(44,55,66,77,88,99,33);
select * from t_group_nesting2;

create type typ4 as (a int, b bit(3));

create table tab_typ4 (a int, b typ4);
insert into tab_typ4(b) values ((1,'1001'));
insert into tab_typ4(a,b) values (1,(1, '1001'));

create table tab_typ4_2 (a int default 1,b typ4);
insert into tab_typ4_2(b) values ((1,'1001'));

create type typ5 as(a int, b int);
create table t1_typ5(a int, b typ5);
create table t2_typ5(a int, b typ5);
insert into t1_typ5 values(1,(1,1));
insert into t2_typ5 select * from t1_typ5;
select (b).a from t1_typ5;
select * from t1_typ5 t1 join t2_typ5 t2 on (t1.b).a=(t1.b).a;

drop table IF EXISTS t_group_nesting1;
drop table IF EXISTS t_group_nesting2;
drop type IF EXISTS typ1 cascade;
drop type IF EXISTS typ2 cascade;
drop type IF EXISTS typ3 cascade;
drop type typ4 cascade;
drop table tab_typ4;
drop table tab_typ4_2;
drop type typ5 cascade;
drop table IF EXISTS t1_typ5;
drop table IF EXISTS t2_typ5;

CREATE TYPE compfoo_nesting AS (c1 compfoo, c2 compfoo);
create table fullname (first text, last text);

--verify basic function
create table quadtable(f1 int, q compfoo_nesting);
insert into quadtable values (1, ((3,'Simon'),(4,'Bob')));
insert into quadtable values (2, ((5,'Clare'),(6,'ok')));

select * from quadtable order by 1,2;
select f1, (q).c1, (qq.q).c1.f1 from quadtable qq order by 1,2;
select * from quadtable order by 1,2;

create table people (fn fullname, bd date);
insert into people values ('(Joe,Blow)', '1984-01-10');

select * from people;

alter table fullname add column suffix text default null;
select * from people;

update people set fn.suffix = 'Jr';
select * from people;

create table pp (f1 text);
insert into pp values (repeat('abcdefghijkl', 100000));
insert into people select ('Jim', f1, null)::fullname, current_date from pp;

select (fn).first, substr((fn).last, 1, 20), length((fn).last) from people order by 1,2,3;

create type cantcompare as (p point, r float8);
create table cc (c1 int default 1,f1 cantcompare);
insert into cc(f1) values('("(1,2)",3)');
insert into cc(f1) values('("(4,5)",6)');
select * from cc order by 1,2;

drop table if exists quadtable;
drop table if exists people;
drop table if exists pp;
drop type cantcompare cascade;
drop table if exists cc;

/*table as the data type*/
create table compos (f1 int, f2 text);
create function fcompos1(v compos) returns void as $$
insert into compos values (v);
$$ language sql;
create function fcompos1(v compos) returns void as $$
insert into compos values (v.*);
$$ language sql;
create function fcompos2(v compos) returns void as $$
select fcompos1(v);
$$ language sql;
create function fcompos3(v compos) returns void as $$
select fcompos1(fcompos3.v.*);
$$ language sql;
select fcompos1(row(1,'one'));
select fcompos2(row(2,'two'));
select fcompos3(row(3,'three'));
select * from compos order by 1;

insert into fullname values ('Joe', 'Blow');

select f.last from fullname f;
select last(f) from fullname f;

create function longname(fullname) returns text language sql
as $$select $1.first || ' ' || $1.last$$;

select f.longname from fullname f;
select longname(f) from fullname f;

alter table fullname add column longname text;

select f.longname from fullname f;
select longname(f) from fullname f;

drop function if exists fcompos1;
drop function if exists fcompos2;
drop function if exists fcompos3;
drop function if exists longname;
drop table if exists compos;
drop type compfoo_nesting cascade;
drop table if exists fullname;

--row表达式
--
-- IS [NOT] NULL should not recurse into nested composites (bug #14235)
--
explain (verbose, costs off)
select r, r is null as isnull, r is not null as isnotnull
from (values (1,row(1,2)), (1,row(null,null)), (1,null),
             (null,row(1,2)), (null,row(null,null)), (null,null) ) r(a,b);

select r, r is null as isnull, r is not null as isnotnull
from (values (1,row(1,2)), (1,row(null,null)), (1,null),
             (null,row(1,2)), (null,row(null,null)), (null,null) ) r(a,b);

explain (verbose, costs off)
with r(a,b) as
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

with r(a,b) as
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

explain (verbose, costs off)
with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

--return composite type
CREATE TABLE compositetable(a text, b text);
INSERT INTO compositetable(a, b) VALUES('fa', 'fb');

-- composite type columns can't directly be accessed (error)
SELECT d.a FROM (SELECT compositetable AS d FROM compositetable) s;
-- but can be accessed with brackets
SELECT (d).a, (d).b FROM (SELECT compositetable AS d FROM compositetable) s;

-- existing column in a NULL composite yield NULL
SELECT (NULL::compositetable).a;

DROP TABLE compositetable;

--create function with composite type
create type avg_state as (total bigint, count bigint);

create or replace function avg_transfn(state avg_state, n int) returns avg_state as
$$
declare new_state avg_state;
begin
        raise notice 'avg_transfn called with %', n;
        if state is null then
        if n is not null then
            new_state.total := n;
            new_state.count := 1;
            return new_state;
        end if;
        return null;
   elsif n is not null then
        state.total := state.total + n;
        state.count := state.count + 1;
        return state;
   end if;
   return null;
end
$$ language plpgsql;

create or replace function avg_finalfn(state avg_state) returns int4 as
$$
begin
    if state is null then
        return NULL;
    else
        return state.total / state.count;
    end if;
end
$$ language plpgsql;

select avg_transfn((1,null),null);
select avg_transfn((1,null),1);
select avg_transfn((null,1),1);
select avg_transfn((null,null),1);
select avg_transfn((null,null),null);
select avg_transfn((10,2),45);

select avg_finalfn(null);
select avg_finalfn((1,null));
select avg_finalfn((null,1));
select avg_finalfn((100,1));

drop type avg_state cascade;
drop function if exists avg_transfn;
drop function if exists avg_finalfn;

--create type with schema
create schema alter1;
create type alter1.ctype as (f1 int, f2 text);

create function alter1.same(alter1.ctype, alter1.ctype) returns boolean language sql
as 'select $1.f1 is not distinct from $2.f1 and $1.f2 is not distinct from $2.f2';

select alter1.same((1,'a'),('2','a'));
select alter1.same((1,'a'),('1','b'));
select alter1.same((1,'a'),('1','a'));
select alter1.same((1,''),('1',''));
select alter1.same((1,null),('1',''));

drop schema alter1 cascade;

/*array type*/
create type type_array as (
id int,
name varchar(50),
score decimal(5,2),
create_time timestamp
);

create table array_tab(a serial, b type_array[])
partition by range (a)
(partition p1 values less than(100),partition p2 values less than(maxvalue));

create table array_tb2(a serial, b type_array[]);

insert into array_tab(b) values('{}');
insert into array_tab(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
analyze array_tab;

insert into array_tb2(b) values('');
insert into array_tb2(b) values(array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)]);
select * from array_tb2 where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;
update array_tab set b=array[cast((1,'test',12,'2018-01-01') as type_array),cast((2,'test2',212,'2018-02-01') as type_array)] where b='{}';

create index i_array on array_tab(b) local;
select * from array_tab where b>array[cast((0,'test',12,'') as type_array),cast((1,'test2',212,'') as type_array)]
order by 1,2;

alter type type_array add attribute attr bool;
SELECT b, LISTAGG(a, ',') WITHIN GROUP(ORDER BY b DESC)
FROM array_tab group by 1;

drop type type_array cascade;
drop table if exists array_tab;
drop table if exists array_tb2;

create type type_array as (
id int,
name varchar(50),
score decimal(15,2)
);
create table array_tab(a serial);
alter table array_tab add column b type_array[] default array[cast((1,'test',12) as type_array),cast((2,'test2',212) as type_array)];
insert into array_tab (a) values (1);
insert into array_tab (a) values (2);
select * from array_tab;

select * from array_tab where b>array[cast((0,'test',12) as type_array),cast((1,'test2',212) as type_array)]
order by 1,2;

alter table array_tab drop column b;
alter table array_tab add column b type_array[];

select * from array_tab where b>array[cast((0,'test',12) as type_array),cast((1,'test2',212) as type_array)]
order by 1,2;

drop type type_array cascade;
drop table if exists array_tab;

create table t_group_array (a compfoo[], b int, c int[]);
insert into t_group_array values(array[cast((1,'syr') as compfoo),cast((2,'sss') as compfoo)],1,array[1,2]);
insert into t_group_array (a[5].f1) values(32);
update t_group_array set a[5].f2='sss'; 
select a[5] from t_group_array order by 1;
select a[5].f1 from t_group_array order by 1;

drop table if exists t_group_array;

/*alter type*/
alter type compfoo add attribute f3 int;
select * from t_group order by 1;
insert into t_group values (1,(1,'syr1',1));
select * from t_group order by 1;

create schema schema1;
alter type compfoo set schema schema1;
create table t1_schema_test1 (
sk int,
a compfoo,
b compfoo
)
WITH (orientation=row);
set current_schema = schema1;
create table t1_schema_test2 (
sk int,
a compfoo,
b compfoo
)
WITH (orientation=row);
alter type compfoo set schema public;
set current_schema = public;

alter type compfoo rename to compfoo_1;
alter type compfoo_1 rename to compfoo;

drop type compfoo cascade;
drop table if exists t_group;
drop schema schema1 cascade;

drop type if exists avg_state cascade;
create type avg_state as (total bigint, count bigint, first int, last char, extend text);
create or replace function avg_transfn1(state avg_state, n int) returns avg_state as
$$
declare new_state avg_state;
begin
 raise notice 'avg_transfn called with %', n;
        if n is not null then
			new_state.total := 0;
			new_state.count := 1;
			new_state.first :=2;
			new_state.last := 'a';
			new_state.extend :='abc';
            return new_state;
        end if;
        return null;
end
$$ language plpgsql;
select avg_transfn1((1,1,2,'a','efe'),1);

CREATE OR REPLACE FUNCTION TEST_FUNC_VARRAY() RETURNS avg_state AS $$
DECLARE
        TYPE array_integer is varray(1024) of integer;
        arrint array_integer :=array_integer();
		new_state avg_state;
BEGIN
		arrint.extend(1024);
		FOR I IN 1..1024 LOOP
			arrint(I):=I;
		END LOOP;
		--display value with raise info
		raise info'%', arrint(arrint.first);
		raise info'%', arrint(arrint.last);
		raise info'%', arrint(11);
		raise info'%', arrint.count;
		new_state.total := 0;
		new_state.count := 1;
		new_state.first :=2;
		new_state.last := 'a';
		new_state.extend :='abc';
        return new_state;
END;
$$ LANGUAGE plpgsql;
select test_func_VARRAY();

--NULLTEST FOR AFORMAT
set behavior_compat_options='aformat_null_test';

explain (verbose, costs off)
select r, r is null as isnull, r is not null as isnotnull
from (values (1,row(1,2)), (1,row(null,null)), (1,null),
             (null,row(1,2)), (null,row(null,null)), (null,null) ) r(a,b);

select r, r is null as isnull, r is not null as isnotnull
from (values (1,row(1,2)), (1,row(null,null)), (1,null),
             (null,row(1,2)), (null,row(null,null)), (null,null) ) r(a,b);

explain (verbose, costs off)
with r(a,b) as
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

with r(a,b) as
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

explain (verbose, costs off)
with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

with r(a,b) as materialized
  (values (1,row(1,2)), (1,row(null,null)), (1,null),
          (null,row(1,2)), (null,row(null,null)), (null,null) )
select r, r is null as isnull, r is not null as isnotnull from r;

declare
type ta as record (a int, b int);
va ta;
begin
va.b = 2;
if va is not null then
raise info '1111';
else
raise info '2222';
end if;
end;
/

select r, r is null as isnull, r is not null as isnotnull
from (values (1,row(1,2)), (1,row(null,null)), (1,null),
             (null,row(1,2)), (null,row(null,null)), (null,null) ) r(a,b);

reset behavior_compat_options;

drop function avg_transfn1();
drop function test_func_VARRAY();
drop type avg_state cascade;


