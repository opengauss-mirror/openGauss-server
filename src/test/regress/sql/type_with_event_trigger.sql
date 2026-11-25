create database type_with_event_trigger;

\c type_with_event_trigger

create publication pub_test for all tables with (ddl='all');

drop type type_not_exists cascade;
drop table if exists table_not_exist;
drop type if exists type_not_exist;
drop index if exists index_not_exist;

\c regression
drop database type_with_event_trigger;