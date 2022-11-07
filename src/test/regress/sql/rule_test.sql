--
-- RULES TEST
--

--
-- Tables and rules for the view test
--
create schema schema_rule_test;
set search_path = schema_rule_test;
create table rule_test1_table (a int4, b int4);
create view tv1 as select * from rule_test1_table;
create rule tv1_ins as on insert to tv1 do instead
	insert into rule_test1_table values (new.a, new.b);
create rule tv1_upd as on update to tv1 do instead
	update rule_test1_table set a = new.a, b = new.b
	where a = old.a;
create rule tv1_del as on delete to tv1 do instead
	delete from rule_test1_table where a = old.a;

-- insert values
insert into tv1 values (1, 11);
insert into tv1 values (2, 12);
select * from tv1;

-- update values
update tv1 set a = 10 where b = 11;
update tv1 set a = 12 , b = 22 where b = 12;
select * from tv1;

-- delete values
delete from tv1 where a = 10;
select * from tv1;

drop rule if exists tv1_ins on tv1;
drop rule if exists tv1_upd on tv1;
drop rule if exists tv1_del on tv1;
drop view if exists tv1;
drop table if exists rule_test1_table;


--
-- Tables and rules for the constraint update/delete/insert test
--
create table ttsystem (sysname text, sysdesc text);
create table ttadmin (pname text, sysname text);
create table ttperon (pname text, pdesc text);
create table ttinterface (sysname text, ifname text);

create rule usys_ins as on insert to ttsystem do also (
	insert into ttinterface values (new.sysname,'');
	insert into ttadmin values ('',new.sysname);
	);

create rule usys_del as on delete to ttsystem do also (
	delete from ttinterface where sysname = old.sysname;
	delete from ttadmin where sysname = old.sysname;
	);

create rule usys_upd as on update to ttsystem do also (
	update ttinterface set sysname = new.sysname
		where sysname = old.sysname;
	update ttadmin set sysname = new.sysname
		where sysname = old.sysname
	);

create rule upers_ins as on insert to ttperon do also (
	insert into ttadmin values (new.pname,'');
	);

create rule upers_del as on delete to ttperon do also
	delete from ttadmin where pname = old.pname;

create rule upers_upd as on update to ttperon do also
	update ttadmin set pname = new.pname where pname = old.pname;
	
-- test 1
insert into ttsystem values ('winxi', 'Linux Jan Wieck');
insert into ttsystem values ('notjw', 'Qu Yan');
insert into ttsystem values ('yuyan', 'Fileserver');

insert into ttinterface values ('winxi', 'dola');
insert into ttinterface values ('winxi', 'eth1');
insert into ttinterface values ('notjw', 'dola');
insert into ttinterface values ('yuyan', 'dola');

insert into ttperon values ('jw', 'Jan Wieck');
insert into ttperon values ('bm', 'Bruce Momjian');

insert into ttadmin values ('jw', 'winxi');
insert into ttadmin values ('jw', 'notjw');
insert into ttadmin values ('bm', 'yuyan');

select * from ttsystem;
select * from ttinterface;
select * from ttperon;
select * from ttadmin;

-- test 2
update ttsystem set sysname = 'pluto' where sysname = 'yuyan';
select * from ttinterface;
select * from ttadmin;

update ttperon set pname = 'jwieck' where pdesc = 'Jan Wieck';
select * from ttadmin order by pname, sysname;

delete from ttsystem where sysname = 'winxi';
select * from ttinterface;
select * from ttadmin;

delete from ttperon where pname = 'bm';
select * from ttadmin;

drop rule if exists usys_upd on ttsystem;
drop rule if exists usys_del on ttsystem;
drop rule if exists usys_ins on ttsystem;
drop rule if exists upers_upd on ttperon;
drop rule if exists upers_del on ttperon;
drop rule if exists upers_ins on ttperon;
drop table if exists ttsystem;
drop table if exists ttinterface;
drop table if exists ttperon;
drop table if exists ttadmin;

--
-- Tables and rules for the logging test
--
create table temp (ename char(20), salary money);
create table templog (ename char(20), action char(10), newsal money, oldsal money);

create rule temp_ins as on insert to temp do
	insert into templog values (new.ename,	'hired', new.salary, '0.00');

create rule temp_upd as on update to temp where new.salary != old.salary do
	insert into templog values (new.ename,	'honored', new.salary, old.salary);

create rule temp_del as on delete to temp do
	insert into templog values (old.ename, 'fired', '0.00', old.salary);

insert into temp values ('tyu', '45.00');
insert into temp values ('asd', '90.00');
select * from templog;

update temp set salary = salary * 2 where ename = 'tyu';
select * from templog;

delete from temp where ename = 'tyu';
select * from templog;

select * from temp;

drop rule if exists temp_ins on temp;
drop rule if exists temp_upd on temp;
drop rule if exists temp_del on temp;
drop table if exists temp;
drop table if exists templog;

--
-- Rules for condition
-- rule test
--
create table test4 (a int4, b text);
create table test5 (a int4, b text);
create table test6 (a int4, b text);

create rule test4_ins1 as on insert to test4
		where new.a >= 10 and new.a < 20 do instead
	insert into test5 values (new.a, new.b);

create rule test4_ins2 as on insert to test4
		where new.a >= 20 and new.a < 30 do
	insert into test6 values (new.a, new.b);


-- test	
insert into test4 values (5, 'huijioa');
insert into test4 values (15, 'afhuvbn');
insert into test4 values (25, 'qwerty');
insert into test4 values (35, 'zxcvbn');

select * from test4;
select * from test5;
select * from test6;

drop rule if exists test4_ins1 on test4;
drop rule if exists test4_ins2 on test4;
drop table if exists test4;
drop table if exists test5;
drop table if exists test6;

--
-- Tables and rules for select
--
create table ttt1 (a int4, b text);
create table ttt2 (a int4, b text);

-- 'on select' will transform table to view
create rule "_RETURN" as on select to ttt1 do instead (
	select * from ttt2;
	);

-- test
insert into ttt1 values (1, 'hello');
insert into ttt2 values (10, 'world');
select * from ttt1;

drop table if exists ttt1; --error
drop view if exists ttt1;
drop table if exists ttt2;

-- 
-- Tables and rules for question
--

create table test_statement(id int);
create table escapetest (ts varchar(50));
create rule r1 as on insert to escapetest do (
	delete from test_statement;
	insert into test_statement values (1);
	insert into test_statement values (2);
	);
	
-- test
insert into escapetest(ts) values (NULL);
select * from test_statement;

drop rule if exists r1 on escapetest;
drop table if exists test_statement;
drop table if exists escapetest;

--
-- check for rules on view and table
--
drop view if exists rules_fooview;
create view rules_fooview as select 'rules_foo'::text;
drop rule "_RETURN" on rules_fooview;
drop view rules_fooview;

drop table if exists rules_fooview;
create table rules_fooview (x int, y text);
select xmin, * from rules_fooview;
create rule "_RETURN" as on select to rules_fooview do instead select 1 as x, 'aaa'::text as y;
select * from rules_fooview;
select xmin, * from rules_fooview;
drop table rules_fooview;
drop view rules_fooview;

-- unsupported rule
create table t1 (id int, name varchar(10));
create view v1 as select * from t1;
create rule r1 as on update to v1 do also alter table t1 modify name varchar(20);
drop table t1 cascade;

drop schema schema_rule_test cascade;
