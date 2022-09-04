/* unsupported */
create schema b_comments;
set search_path to 'b_comments';
create table test_row(a int not null comment 'test_row.a');
create table test_row(a int not null) comment 'test_row';
create index on test_row(a,b) comment 'test_row_index';
create table partition_test(c1 int, c2 int, logdate date not null) comment 'partition_test' partition by range (logdate) INTERVAL ('1 month')
(
    PARTITION partition_test_p0 VALUES LESS THAN ('2020-03-01'),
    PARTITION partition_test_p1 VALUES LESS THAN ('2020-04-01'),
    PARTITION partition_test_p2 VALUES LESS THAN ('2020-05-01')
);
create  index partition_test_index on partition_test (logdate) local
(
    partition sip1_index_local,
    partition sip2_index_local tablespace PG_DEFAULT,
    partition sip3_index_local tablespace PG_DEFAULT
) comment 'partition_test_index';
create table  test( id int comment 'test.id',  uid int comment 'test.uid',  checkid int comment 'test.checkid',  primary key ( id ) comment 'pkey.id',  unique ( uid ) comment 'ukey.uid', check(checkid > 0) comment 'check.checkid') comment 'test';
create index test_id_index on test(id) comment 'test_id_index';
create function test_function(a int) returns void as $$ begin end; $$ language plpgsql comment 'test_function';
create procedure test_procedure(int,int) comment 'test_procedure' as begin select $1 + $2;end;
/
create table test_alter(id int);
alter table test_alter comment 'test_alter';
alter table test_alter add column id2 int comment 'test_alter.id2';
alter table test_alter add constraint test_alter_id_check check (id > 0) comment 'test_alter.constraint';
alter table test_alter add constraint primary_key unique(id) comment 'test_alter.primary_key';
create function test_alter_function(a int) returns void as $$ begin end; $$ language plpgsql;
alter function test_alter_function(a int) comment 'test_alter_function';
create procedure test_alter_procedure(int,int) as begin select $1 + $2;end;
/
alter procedure test_alter_procedure(int,int) comment 'test_alter_procedure';
drop schema b_comments cascade;


create database b_comments dbcompatibility 'B';
\c b_comments
create schema b_comments;
set search_path to 'b_comments';
/* sanity check */
create table test_row(a int not null comment 'test_row.a', b int not null comment 'test_row.b') comment 'test_row';
create index on test_row(a,b) comment 'test_row_index';

/* column orientation check */
create table test_column(a int comment 'test_column.a', b int comment 'test_column.b') with (orientation='column') comment 'test_column';

/* comment position check */
create tablespace b_comments relative location 'b_comments';
create table test(id int) tablespace b_comments comment 'test';
create index on test(id) tablespace b_comments comment 'test_index' where id > 10 ;

/* partition position check*/
create table partition_test(c1 int, c2 int, logdate date not null) comment 'partition_test' partition by range (logdate) INTERVAL ('1 month')
(
    PARTITION partition_test_p0 VALUES LESS THAN ('2020-03-01'),
    PARTITION partition_test_p1 VALUES LESS THAN ('2020-04-01'),
    PARTITION partition_test_p2 VALUES LESS THAN ('2020-05-01')
);
create  index partition_test_index on partition_test (logdate) local
(
    partition sip1_index_local,
    partition sip2_index_local tablespace PG_DEFAULT,
    partition sip3_index_local tablespace PG_DEFAULT
) comment 'partition_test_index';
create table  test_new( id int comment 'test_new.id',  uid int comment 'test_new.uid',  checkid int comment 'test_new.checkid',  primary key ( id ) comment 'pkey.id',  unique ( uid ) comment 'ukey.uid', check(checkid > 0) comment 'check.checkid') comment 'test_new';
create index test_id_index on test_new(id) comment 'test_new_id_index';
create function test_function(a int) returns void as $$ begin end; $$ language plpgsql comment 'test_function';
create procedure test_procedure(int,int) comment 'test_procedure' as begin select $1 + $2;end;
/

create table test_alter(id int);
alter table test_alter comment 'test_alter';
alter table test_alter add column id2 int comment 'test_alter.id2';
alter table test_alter add constraint test_alter_id_check check (id > 0) comment 'test_alter.constraint';
alter table test_alter add constraint primary_key unique(id) comment 'test_alter.primary_key';
create function test_alter_function(a int) returns void as $$ begin end; $$ language plpgsql;
alter function test_alter_function(a int) comment 'test_alter_function';
create procedure test_alter_procedure(int,int) as begin select $1 + $2;end;
/
alter procedure test_alter_procedure(int,int) comment 'test_alter_procedure';
/* result */
select description
from pg_description
where objoid in (select relfilenode
                 from pg_class
                 where relnamespace in (select oid from pg_catalog.pg_namespace where nspname = 'b_comments'))
order by description;
drop schema b_comments cascade;
reset search_path;
\c postgres
drop database b_comments;