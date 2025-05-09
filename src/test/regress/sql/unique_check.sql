create schema unique_check;
set current_schema to 'unique_check';

drop table if exists warehouse;

create table warehouse (
w_id smallint not null,
w_name varchar(10), 
primary key (w_id) );

insert into warehouse values (1, 1);
insert into warehouse values (1, 1);
SET UNIQUE_CHECKS=0;
insert into warehouse values (1, 1);

drop table warehouse;

reset UNIQUE_CHECKS;
drop schema unique_check cascade;
reset current_schema;
