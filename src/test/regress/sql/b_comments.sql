/* unsupport */
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