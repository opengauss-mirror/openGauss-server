set search_path to sys;

create table test1(id int);
create view v1 as select 1;


set search_path to information_schema_tsql;
create table test1(id int);
create view v1 as select 1;

reset search_path;

alter system set upgrade_mode to 2;
select pg_sleep(2);
set isinplaceupgrade to on;

set search_path to sys;

create table test1(id int);
create view v1 as select 1;

select * from test1;
set search_path to information_schema_tsql;
create table test1(id int);
create view v1 as select 1;

select * from test1;

reset search_path;

set isinplaceupgrade to off;
alter system set upgrade_mode to 0;
