--This file is a supplement to jsonb.sql.
CREATE SCHEMA jsonb_test;
SET CURRENT_SCHEMA TO jsonb_test;

-- data format
select ''::jsonb;
select null::jsonb;
select 'null'::jsonb;
select 'NULL'::jsonb;
select '-1.5'::jsonb;
select '+1.5'::jsonb;
select '-1.5e5'::jsonb;
select '-1.5e5.6'::jsonb;
select '-1.5e-5'::jsonb;
select '-1.5e+5'::jsonb;
select '25.000000'::jsonb;
select 'true'::jsonb, 'false'::jsonb;
select 'True'::jsonb, 'False'::jsonb;
select 'TRUE'::jsonb, 'FALSE'::jsonb;


-- jsonb Advanced Features
select '{   }'::jsonb;
select '{"aa" :     1, "d":4, "aa"   :2, "b":3}'::jsonb;
select '[   ]'::jsonb;
select '[1,   5, 8, {"aa" :     1, "d":4, "aa"   :2, "b":3}]'::jsonb;


-- jsonb normal functions and operator
select '{"a": {"b":{"c": "foo"}}}'::jsonb #> array[]::text[];


-- jsonb hash funcs
select jsonb_hash('');
select jsonb_hash('1');
select jsonb_hash('null');
select jsonb_hash('true');
select jsonb_hash('"xxx"');
select jsonb_hash('[1,2,3]');
select jsonb_hash('{"xxx":123, "das":[789,{}]}');


-- gin/contain/exist
-- there is only one func we can call directly，and operators have been well tested in jsonb.sql
-- The size of the returned value of the character string depends on the operating system.
select btint4cmp(gin_compare_jsonb('',''),0);
select btint4cmp(gin_compare_jsonb('1','1'),0);
select btint4cmp(gin_compare_jsonb('[1]','[2]'),0);
select btint4cmp(gin_compare_jsonb('1','2'),0);


-- jsonb compare
-- cmp eq ne gt ge lt le （also = <> > >= < <=）
-- The size of the returned value of the character string depends on the operating system.
create table cmpjsonb (a jsonb, b jsonb);
insert into cmpjsonb values
('null',  'null'), ('null', '"oooo"'), ('null', '-3.5e8'), ('null', 'false'), ('null', '[]'), ('null', '[1,2]'), ('null', '{}'), ('null', '{"":""}'),
('""','""'), ('"aa"', '"aa"'), ('"a"', '"b"'), ('"a"', '"cc"'), ('"a"', '-58.6e-2'), ('""', 'false'), ('"true"', 'true'), ('""', '[]'), ('"s"', '["s"]'), ('"x"', '{}'), ('"x"', '{"x":1}'), 
('0',  '-0'),('100', '1e2'), ('-3.5e8', '8e3'), ('9','-9'), ('1', 'false'), ('-1', 'true'), ('0', '[]'), ('1', '[1]'), ('0', '{}'), ('1', '{"1":1}'), 
('false', 'false'), ('false', 'true'), ('false', '[]'), ('false', '[true]'), ('true', '{}'), ('true', '{"false":true}'),
('[]','[]'),('[1,2]','[1]'), ('[1,2,3]','[true, false, true]'), ('[1,2]', '[1,3]'), ('[]','{}'),('[[]]', '[{}]'),('[[false]]', '[1]'), ('[[{"":""}]]','[{}]'),
('{}','{}'),('{"a":4}','{"a":5}'),('{"aa":4}','{"a":5}'),('{"aa":4}','{"a":5, "aa":4}'),('{"f":[1,5,8], "r":{"":30, "o":"ooo"}}', '{"f":[1,5], "r":{"":30, "o":"ooo"}}'),
(null,null),(null,'888');

select a, b,
  btint4cmp(jsonb_cmp(a, b),0) as cmp,
  jsonb_eq(a, b) as eq,
  jsonb_ne(a, b) as ne,
  jsonb_lt(a, b) as lt,
  jsonb_le(a, b) as le,
  jsonb_gt(a, b) as gt,
  jsonb_ge(a, b) as ge
from cmpjsonb order by 1,2,3,4,5,6,7,8,9;

select a, b,
  a =  b as eq,
  a <> b as ne,
  a <  b as lt,
  a <= b as le,
  a >  b as gt,
  a >= b as ge
from cmpjsonb order by 1,2,3,4,5,6,7,8;


-- partition/temp/unlog row/column table
-- index  uindex
create table jsonbt (a jsonb, b int);
create temp table tmpjsonbt (a jsonb);
create unlogged table ulgjsonbt (a jsonb);
create table partjsonbt1 (a jsonb) partition by range(a) (partition p1 values less than('1'),partition p2 values less than('{}'));
create table partjsonbt2 (a jsonb) partition by hash(a) (partition p1,partition p2);
create table csjsonbt(a jsonb) with (orientation=column);
create unique index uijsonbt on jsonbt(a);
create index ijsonbt on jsonbt(a);

insert into jsonbt values('null',1), ('""',1), ('0',1), ('false',1), ('[]',1), ('{}',1);
insert into tmpjsonbt values('null'), ('""'), ('0');
insert into ulgjsonbt values('false'), ('[]'), ('{}');

update tmpjsonbt set a = '"a"' where jsonb_typeof(a) = 'string';
delete from ulgjsonbt where jsonb_typeof(a) = 'boolean';
insert into jsonbt values('false',0) on duplicate key update b = 5;

select * from jsonbt;
select * from tmpjsonbt;
select * from ulgjsonbt;


-- pk fk join
create table jsonbpk (pk jsonb primary key);
create table jsonbfk (a int, fk jsonb references jsonbpk);

insert into jsonbpk values(null);
insert into jsonbpk values('null'), ('""'), ('0'), ('false'), ('[]'), ('{}');
insert into jsonbpk values('"aaa"'), ('-2.5e2'), ('true'), ('[1,5,"ff",{}]'), ('{"":""}'), ('{"xx":55, "dd":[1,2]}');
insert into jsonbpk values('null');
insert into jsonbpk values('[1,5,"ff",{}]');
insert into jsonbfk values(1,'null'), (2,'""'), (3,'0'), (4,'false'), (5,'[]'), (6,'{}');
insert into jsonbfk values(7,'"aaa"'), (8,'-2.5e2'), (9,'true'), (1,'[1,5,"ff",{}]'), (2,'{"":""}'), (3,'{"xx":55, "dd":[1,2]}');
insert into jsonbfk values(10,'"aaa"'), (11,'-2.5e2'), (12,'true'), (13,'[1,5,"ff",{}]'), (14,'{"":""}'), (15,'{"xx":55, "dd":[1,2]}');
insert into jsonbfk values(1,'[100]'), (2,'"err"'), (3,'100'), (4,'[1,3,2]'), (5,'{"err":"err"}');

select a, pk, fk from jsonbfk join jsonbpk on fk = pk order by 1,2;
explain select a, pk, fk from jsonbfk join jsonbpk on fk = pk order by 1,2;


-- scan indexscan bypass
set opfusion_debug_mode = log;
analyze jsonbpk;
explain(costs off) select * from jsonbpk;
select * from jsonbpk where pk >= '[]';
select * from jsonbpk where pk >= 'false';
explain(costs off) select * from jsonbpk where pk >= 'false';

insert into jsonbpk select generate_series(1,10000)::text::jsonb;
analyze jsonbpk;
explain(costs off) select * from jsonbpk where pk < '0';

set opfusion_debug_mode = off;


-- normal agg function
create table jsonbaggtest (a int, b jsonb);
insert into jsonbaggtest select * from jsonbfk;
select count(*) from jsonbaggtest where b > '{}';
select count(a), max(a), b from jsonbaggtest group by b having max(a) > 5 order by 1,2,3;
select max(b), a from jsonbaggtest group by a having max(b) > '{}' order by 1,2;
select min(b) from jsonbaggtest;
select sum(b) from jsonbaggtest;
select median(b) from jsonbaggtest;
select avg(b) from jsonbaggtest;


-- view/matview/incmatview
create table jsonbvt(a jsonb);
insert into jsonbvt values ('null'), ('""'), ('0'), ('false'), ('[]'), ('{}');
create view jsonbpkv as select * from jsonbvt;
create materialized view jsonbpkmv as select * from jsonbvt;
create incremental materialized view jsonbpkimv as select * from jsonbvt;

insert into jsonbvt values('[null, "", 0, false, [], {}]');

select * from jsonbpkv;
select * from jsonbpkmv;
select * from jsonbpkimv;

refresh materialized view jsonbpkmv;
refresh incremental materialized view jsonbpkimv;
select * from jsonbpkmv;
select * from jsonbpkimv;


-- transaction and rollback and err
create table jsonbtr (a jsonb);
begin;
select * from jsonbtr;
insert into jsonbtr values('{}'), ('[]');
select * from jsonbtr;
end;

begin;
insert into jsonbtr values('{"":""}'), ('[1,2,3]');
select * from jsonbtr;
rollback;
select * from jsonbtr;

begin;
insert into jsonbtr values('{:}'), ('[,]');
select * from jsonbtr;
end;
select * from jsonbtr;


-- clean up
DROP SCHEMA jsonb_test CASCADE;
