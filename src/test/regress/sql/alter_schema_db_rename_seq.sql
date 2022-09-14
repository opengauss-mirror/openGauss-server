----case1 alter schema rename------
create schema alter_schema_rename_seq;

set current_schema='alter_schema_rename_seq';
create sequence seq1;
create sequence seq2;
create sequence seq3;
alter schema alter_schema_rename_seq rename to alter_schema_rename_seq_bak;

set current_schema='alter_schema_rename_seq_bak';
select nextval('seq1');
select nextval('seq2');
select nextval('seq3');

alter schema alter_schema_rename_seq_bak rename to alter_schema_rename_seq;
set current_schema='alter_schema_rename_seq';
select nextval('seq1');
select nextval('seq2');
select nextval('seq3');

alter schema test_rename_not_exist_schema rename to test_rename_not_exist_schema_new;
alter schema public rename to public_new;
alter schema pg_catalog rename to pg_catalog_new;

----clean-----
drop sequence seq1;
drop sequence seq2;
drop sequence seq3;
drop schema alter_schema_rename_seq;

-----case2 alter database rename-------
create database alter_schema_rename_seq;
\c alter_schema_rename_seq;
create schema alter_schema_rename_seq;
set current_schema='alter_schema_rename_seq';
create sequence seq1;
create sequence seq2;
create sequence seq3;
\c postgres
alter database alter_schema_rename_seq rename to alter_schema_rename_seq_bak;
\c alter_schema_rename_seq_bak;
set current_schema='alter_schema_rename_seq';
select nextval('seq1');
select nextval('seq2');
select nextval('seq3');
\c postgres
alter database alter_schema_rename_seq_bak rename to alter_schema_rename_seq;
\c alter_schema_rename_seq;
set current_schema='alter_schema_rename_seq';
select nextval('seq1');
select nextval('seq2');
select nextval('seq3');

----clean-----
drop sequence seq1;
drop sequence seq2;
drop sequence seq3;
drop schema alter_schema_rename_seq;
\c postgres
drop database alter_schema_rename_seq;

