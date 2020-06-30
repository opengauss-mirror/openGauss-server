--
-- HW_LIGHT_CN
--
set enable_light_proxy to on;
show client_encoding;
select * from getdatabaseencoding();

create table hw_light_cn_t1 (id1 int, id2 int, num int);
insert into hw_light_cn_t1 values (1,11,11), (2,21,21), (3,31,31), (4,41,41), (5,51,51);


-- Testset 1 one query in one transaction
-- single table
explain select * from hw_light_cn_t1 where id1 = 3;
explain select * from hw_light_cn_t1 where id1 = 3 for update;
explain insert into hw_light_cn_t1 values (6,6,6);
explain update hw_light_cn_t1 set num = 0 where id1 = 1;
explain delete from hw_light_cn_t1 where id1 = 2;
explain select * from hw_light_cn_t1 order by id1;

select * from hw_light_cn_t1 where id1 = 3;
select * from hw_light_cn_t1 where id1 = 3 for update;
insert into hw_light_cn_t1 values (6,6,6);
update hw_light_cn_t1 set num = 0 where id1 = 1;
delete from hw_light_cn_t1 where id1 = 2;
select * from hw_light_cn_t1 order by id1;


-- Testset 2 multiple queries in one transaction
start transaction;
select * from hw_light_cn_t1 where id1 = 3;
select * from hw_light_cn_t1 where id1 = 3 for update;
insert into hw_light_cn_t1 values (6,6,6);
update hw_light_cn_t1 set num = 1 where id1 = 1;
delete from hw_light_cn_t1 where id1 = 3;
select * from hw_light_cn_t1 order by id1;
end;
select * from hw_light_cn_t1 order by id1;

start transaction;
select * from hw_light_cn_t1 where id1 = 4;
select * from hw_light_cn_t1 where id1 = 4 for update;
insert into hw_light_cn_t1 values (6,6,6);
update hw_light_cn_t1 set num = 2 where id1 = 1;
delete from hw_light_cn_t1 where id1 = 4;
select * from hw_light_cn_t1 order by id1;
abort;
select * from hw_light_cn_t1 order by id1;


-- Testset 3 queries that do not support light_cn
-- enable_light_proxy
set enable_light_proxy = off;
explain select * from hw_light_cn_t1 where id1 = 3;
select * from hw_light_cn_t1 where id1 = 3;
set enable_light_proxy = on;

explain select * from hw_light_cn_t1 where id1 = 3;
select * from hw_light_cn_t1 where id1 = 3;

-- more than one shard
explain select * from hw_light_cn_t1;

-- has agg
explain select count(*) from hw_light_cn_t1 where id1 = 1;
explain select sum(num) from hw_light_cn_t1 where id1 = 1;
explain select avg(num) from hw_light_cn_t1 where id1 = 1;
explain select min(num) from hw_light_cn_t1 where id1 = 1;
explain select max(num) from hw_light_cn_t1 where id1 = 1;

select count(*) from hw_light_cn_t1 where id1 = 1;
select sum(num) from hw_light_cn_t1 where id1 = 1;
select avg(num) from hw_light_cn_t1 where id1 = 1;
select min(num) from hw_light_cn_t1 where id1 = 1;
select max(num) from hw_light_cn_t1 where id1 = 1;

-- declare cursor
start transaction;
explain cursor c1 for select * from hw_light_cn_t1 where id1 = 1;
cursor c1 for select * from hw_light_cn_t1 where id1 = 1;
fetch all c1;
end;

-- subquery
explain select * from (select avg(num) from hw_light_cn_t1 where id1 = 1) t1; 
select * from (select avg(num) from hw_light_cn_t1 where id1 = 1) t1; 

-- update sharding key
update hw_light_cn_t1 set id1 = 1 where id1 = 2;

-- permission denied
create user tuser identified by "Test@Mpp";
grant select on hw_light_cn_t1 to tuser;
SET SESSION AUTHORIZATION tuser PASSWORD 'Test@Mpp';
SELECT session_user, current_user;
select * from hw_light_cn_t1 where id1 = 1;
insert into hw_light_cn_t1 values (6,6,6);
update hw_light_cn_t1 set num = 2 where id1 = 1;
delete from hw_light_cn_t1 where id1 = 2;
select * from hw_light_cn_t1 order by id1;
\c
drop user tuser cascade;

-- encoding not same
set client_encoding='GBK'; 
create table testtd(
c_nchar nchar(10),
c_character1 character(10),
c_character2 character varying(10),
c_varchar2 varchar2(10)
);
explain insert into testtd values('a','a','a','a');
insert into testtd values('a','a','a','a');
drop table testtd;
reset client_encoding;

-- pbe by prepare/execute
prepare a as select * from hw_light_cn_t1 where id1 = 1;
explain execute a;
execute a;
deallocate a;
prepare a as select * from hw_light_cn_t1 where id1 = $1;
explain execute a(1);
execute a(1);
deallocate a;

-- explain verbose/analyze/performance
explain verbose select * from hw_light_cn_t1 where id1 = 1;
explain analyze select * from hw_light_cn_t1 where id1 = 1;
explain analyze verbose select * from hw_light_cn_t1 where id1 = 1;
explain performance select * from hw_light_cn_t1 where id1 = 1;

-- function
CREATE OR REPLACE FUNCTION test_lp()
RETURNS bool
AS $$
DECLARE
	tmp record;
	result bool;
	BEGIN
		execute('explain select * from hw_light_cn_t1 where id1 = 1');
		execute('select * from hw_light_cn_t1 where id1 = 1');
		return true;
	END; $$
LANGUAGE 'plpgsql' NOT FENCED;
select test_lp();
explain select * from hw_light_cn_t1 where id1 = 1;


-- Bug fixing
truncate hw_light_cn_t1;
start transaction;
insert into hw_light_cn_t1 values (7,7,7);
select cmax, cmin, id1 from hw_light_cn_t1 order by 3;
insert into hw_light_cn_t1 values (8,8,8);
select cmax, cmin, id1 from hw_light_cn_t1 order by 3;
commit;
select cmax, cmin, id1 from hw_light_cn_t1 order by 3;
truncate hw_light_cn_t1;
start transaction;
insert into hw_light_cn_t1 values (7,7,7);
select cmax, cmin, id1 from hw_light_cn_t1 where id1 = 7;
insert into hw_light_cn_t1 values (8,8,8);
select cmax, cmin, id1 from hw_light_cn_t1 where id1 = 8;
commit;
select cmax, cmin, id1 from hw_light_cn_t1 order by 3;

-- N message
create table ts_test(id int, c1 text);
insert into ts_test values(1, rpad('bcg', 2500, 'AbCdef'));
select to_tsvector(c1) from ts_test where id = 1;
drop table ts_test;

-- one distribute key
create table tnull (id int, num int);
explain verbose insert into tnull values (null,1);
insert into tnull values (null,1);
explain verbose select * from tnull where id is null;
select * from tnull where id is null;
explain verbose select * from tnull where id = null;
select * from tnull where id = null;
-- multiple distribute key
create table tnull2 (id1 int, id2 int, num int);
explain verbose insert into tnull2 values (null,null,1);
explain verbose insert into tnull2 values (null,1,1);
explain verbose insert into tnull2 values (null,20,1);
insert into tnull2 values (null,null,1);
insert into tnull2 values (null,1,1);
insert into tnull2 values (null,20,1);
explain verbose select * from tnull2 where id1 is null and id2 is null;
select * from tnull2 where id1 is null and id2 is null;
explain verbose select * from tnull2 where id1 is null and id2 = 1;
select * from tnull2 where id1 is null and id2 = 1;
explain verbose select * from tnull2 where id1 is null and id2 = 20;
select * from tnull2 where id1 is null and id2 = 20;
-- join
create table tnull1 (id1 int, num int);
insert into tnull1 values (null,1);
explain verbose select id from tnull, tnull1 where id is null and id1 is null;
select id from tnull, tnull1 where id is null and id1 is null;
explain verbose select id from tnull, tnull2 where id is null and id1 is null and id2 is null;
select id from tnull, tnull2 where id is null and id1 is null and id2 is null;
explain verbose select id from tnull, tnull2 where id is null and id1 is null and id2 =1;
select id from tnull, tnull2 where id is null and id1 is null and id2 =1;
explain verbose select id from tnull, tnull2 where id is null and id1 is null and id2 =20;
select id from tnull, tnull2 where id is null and id1 is null and id2 =20;

-- BufferConnection
create table t_BufferConnection (id int, num int);
insert into t_BufferConnection values (1,1),(2,2),(3,3),(4,4);
begin;
declare c1 cursor for select * from t_BufferConnection order by id;
fetch next from c1;
insert into t_BufferConnection values (5,5);
fetch next from c1;
fetch next from c1;
fetch next from c1;
fetch next from c1;
end;
select * from t_BufferConnection order by id;
drop table t_BufferConnection;


-- clean up
drop table hw_light_cn_t1;