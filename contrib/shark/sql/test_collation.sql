create schema test_default_collation;
set search_path = 'test_default_collation';

drop table if exists Employees;
drop PROCEDURE if exists Proc_SelectInto;
drop table if exists new_table;

create table Employees(
    id int,
    name VARCHAR(100),
    salary money,
    is_men BOOLEAN,
    col5 BLOB,
    col6 date
    );
insert into Employees values (1, 'zhangsan', 100, true, empty_blob(), date '12-10-2010');
insert into Employees values (2, 'lisi', -50, false, empty_blob(), date '12-06-2019');
insert into Employees values (3, 'wangwu', 10000, true, empty_blob(), date '12-04-2025');

SELECT * INTO new_table from Employees;
select * from new_table order by id;

drop table if exists new_table;
drop PROCEDURE if exists Proc_SelectInto;
drop table if exists Employees;


drop table if exists tab_1130344;
drop table if exists tt_1130344;
drop table if exists like_1130344;
drop table if exists as_1130344;

create table tt_1130344(a1 sql_variant PRIMARY KEY);
insert into tt_1130344 values(2::int),('ff'::char(8)),('!'::varchar(3)),('li'::char(6)),('good'::varchar2(8)),('yes'::char);

create table tab_1130344
(
a1 sql_variant not null,
a2 sql_variant unique,
a3 sql_variant PRIMARY KEY,
a4 sql_variant default 'good',
a5 sql_variant check(a5 is not null),
a6 sql_variant REFERENCES tt_1130344(a1));

insert into tab_1130344 values(100::int,10::int,'cc'::varchar(3),'dd'::varchar2(8),'ee'::char,'ff'::char(8));
insert into tab_1130344(a1,a2,a3,a5,a6) values(200::int,40::int,'is'::char(4),'very'::char(4),'good'::varchar(4));

create table as_1130344 as select * from tab_1130344;
create table like_1130344 (like tab_1130344 including all);

select count(*) from tab_1130344;
select count(*) from as_1130344;
select count(*) from like_1130344;
drop table if exists tab_1130344;
drop table if exists tt_1130344;
drop table if exists like_1130344;
drop table if exists as_1130344;

reset search_path;
drop schema test_default_collation;