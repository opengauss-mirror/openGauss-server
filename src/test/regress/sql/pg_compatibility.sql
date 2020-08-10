-- pg compatibility case
drop database if exists pg_type_databse;
create database pg_type_databse dbcompatibility 'PG';

\c pg_type_databse
create table d_format_test(a varchar(10) not null);
insert into d_format_test values('');



-- concat test
select concat(null,'','','') is null;

select concat('','') is null;

select ''::int;