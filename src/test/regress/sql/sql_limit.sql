\c postgres;
ALTER SYSTEM SET enable_sql_limit = 'on';
SELECT pg_reload_conf();
create table test_table(i int);
select count(*) from test_table where i > 10 and i < 1000;
insert into test_table select generate_series(1,100000);

create user test_table_user with password 'Gauss@123';
grant select,insert,update,delete on test_table to test_table_user;

select gs_create_sql_limit('rule_1','sqlid',0,0,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],null,null);
select gs_create_sql_limit('rule_2','select',0,0,'now()','','{select,from,test_table,where}',null,null);
select gs_create_sql_limit('rule_3','insert',0,0,'now()','','{insert,into,test_table,values}',null,null);
select gs_create_sql_limit('rule_4','update',0,0,'now()','','{update,test_table,set,where}',null,null);
select gs_create_sql_limit('rule_5','delete',0,0,'now()','','{delete,from,test_table,where}',null,null);

set role test_table_user password 'Gauss@123';
-- expect sqlid rule hit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule hit
select count(*) from test_table where i > 1;
-- expect insert rule hit
insert into test_table values (100001);
-- expect update rule hit
update test_table set i = 100002 where i = 100001;
-- expect delete rule hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule
reset role;
select gs_update_sql_limit(1,'sqlid',0,1,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],null,null);
select gs_update_sql_limit(2,'select',0,1,'now()','','{select,from,test_table,where}',null,null);
select gs_update_sql_limit(3,'insert',0,1,'now()','','{insert,into,test_table,values}',null,null);
select gs_update_sql_limit(4,'update',0,1,'now()','','{update,test_table,set,where}',null,null);
select gs_update_sql_limit(5,'delete',0,1,'now()','','{delete,from,test_table,where}',null,null);

set role test_table_user password 'Gauss@123';
-- expect sqlid rule hit, but not limit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule hit, but not limit
select count(*) from test_table where i > 1;
-- expect insert rule hit, but not limit
insert into test_table values (100001);
-- expect update rule hit, but not limit
update test_table set i = 100002 where i = 100001;
-- expect delete rule hit, but not limit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule to invalid
reset role;
select gs_update_sql_limit(1,'sqlid',0,-1,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],null,null);
select gs_update_sql_limit(2,'select',0,-1,'now()','','{select,from,test_table,where}',null,null);
select gs_update_sql_limit(3,'insert',0,-1,'now()','','{insert,into,test_table,values}',null,null);
select gs_update_sql_limit(4,'update',0,-1,'now()','','{update,test_table,set,where}',null,null);
select gs_update_sql_limit(5,'delete',0,-1,'now()','','{delete,from,test_table,where}',null,null);

set role test_table_user password 'Gauss@123';
-- expect sqlid rule not hit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule not hit
select count(*) from test_table where i > 1;
-- expect insert rule not hit
insert into test_table values (100001);
-- expect update rule not hit
update test_table set i = 100002 where i = 100001;
-- expect delete rule not hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule to sepecific user
reset role;
select gs_update_sql_limit(1,'sqlid',0,0,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],null,'{test_table_user}');
select gs_update_sql_limit(2,'select',0,0,'now()','','{select,from,test_table,where}',null,'{test_table_user}');
select gs_update_sql_limit(3,'insert',0,0,'now()','','{insert,into,test_table,values}',null,'{test_table_user}');
select gs_update_sql_limit(4,'update',0,0,'now()','','{update,test_table,set,where}',null,'{test_table_user}');
select gs_update_sql_limit(5,'delete',0,0,'now()','','{delete,from,test_table,where}',null,'{test_table_user}');

set role test_table_user password 'Gauss@123';
-- expect sqlid rule hit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule hit
select count(*) from test_table where i > 1;
-- expect insert rule hit
insert into test_table values (100001);
-- expect update rule hit
update test_table set i = 100002 where i = 100001;
-- expect delete rule hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule to sepecific user
reset role;
select gs_update_sql_limit(1,'sqlid',0,0,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],null,'{user}');
select gs_update_sql_limit(2,'select',0,0,'now()','','{select,from,test_table,where}',null,'{user}');
select gs_update_sql_limit(3,'insert',0,0,'now()','','{insert,into,test_table,values}',null,'{user}');
select gs_update_sql_limit(4,'update',0,0,'now()','','{update,test_table,set,where}',null,'{user}');
select gs_update_sql_limit(5,'delete',0,0,'now()','','{delete,from,test_table,where}',null,'{user}');

set role test_table_user password 'Gauss@123';
-- expect sqlid rule not hit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule not hit
select count(*) from test_table where i > 1;
-- expect insert rule not hit
insert into test_table values (100001);
-- expect update rule not hit
update test_table set i = 100002 where i = 100001;
-- expect delete rule not hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule to sepecific database
reset role;
select gs_update_sql_limit(1,'sqlid',0,0,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],'{db}','{user}');
select gs_update_sql_limit(2,'select',0,0,'now()','','{select,from,test_table,where}','{db}','{user}');
select gs_update_sql_limit(3,'insert',0,0,'now()','','{insert,into,test_table,values}','{db}','{user}');
select gs_update_sql_limit(4,'update',0,0,'now()','','{update,test_table,set,where}','{db}','{user}');
select gs_update_sql_limit(5,'delete',0,0,'now()','','{delete,from,test_table,where}','{db}','{user}');

set role test_table_user password 'Gauss@123';
-- expect sqlid rule not hit
select count(*) from test_table where i > 1 and i < 100000;

-- expect select rule not hit
select count(*) from test_table where i > 1;
-- expect insert rule not hit
insert into test_table values (100001);

-- expect update rule not hit
update test_table set i = 100002 where i = 100001;

-- expect delete rule not hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- update rule to sepecific database
reset role;
select gs_update_sql_limit(1,'sqlid',0,0,'now()','',ARRAY(select unique_sql_id::text from dbe_perf.statement where query like 'select count(*) from test_table where i > ? and i < ?%')::name[],'{postgres}','{user}');
select gs_update_sql_limit(2,'select',0,0,'now()','','{select,from,test_table,where}','{postgres}','{user}');
select gs_update_sql_limit(3,'insert',0,0,'now()','','{insert,into,test_table,values}','{postgres}','{user}');
select gs_update_sql_limit(4,'update',0,0,'now()','','{update,test_table,set,where}','{postgres}','{user}');
select gs_update_sql_limit(5,'delete',0,0,'now()','','{delete,from,test_table,where}','{postgres}','{user}');

set role test_table_user password 'Gauss@123';
-- expect sqlid rule not hit
select count(*) from test_table where i > 1 and i < 100000;
-- expect select rule not hit
select count(*) from test_table where i > 1;
-- expect insert rule not hit
insert into test_table values (100001);
-- expect update rule not hit
update test_table set i = 100002 where i = 100001;
-- expect delete rule not hit
delete from test_table where i = 100002;

reset role;
select * from gs_select_sql_limit(1);
select * from gs_select_sql_limit(2);
select * from gs_select_sql_limit(3);
select * from gs_select_sql_limit(4);
select * from gs_select_sql_limit(5);

select * from gs_select_sql_limit();

-- delete rule
reset role;
select gs_delete_sql_limit(1);
select gs_delete_sql_limit(2);
select gs_delete_sql_limit(3);
select gs_delete_sql_limit(4);
select gs_delete_sql_limit(5);

ALTER SYSTEM SET enable_sql_limit = 'off';
SELECT pg_reload_conf();

drop table test_table;
drop user test_table_user;

