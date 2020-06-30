set td_compatible_truncation=on;
set td_compatible_truncation=off;
drop database if exists td_db_char_cast;
create database td_db_char_cast DBCOMPATIBILITY 'C';

\c td_db_char_cast

set client_encoding='UTF8'; 
--test cast char type data the result should pad blank space.
set td_compatible_truncation=off;
show td_compatible_truncation;

create table t1(id int, info char(5));
create table t1_col(id int, info char(5)) with(orientation = column);

delete from t1;

insert into t1 values(3,  cast('一二' as char(5)));
insert into t1 select 4, cast('一二' as char(5));
insert into t1 values(5,  cast('一二' as char(6)));
insert into t1 select 6, cast('一二' as char(6));

select * from t1 order by id;
select id, length(info), lengthb(info) from t1 order by id;

delete from t1_col;
insert into t1_col values(7,  cast('一二' as char(5)));
insert into t1_col select 8, cast('一二' as char(5));
insert into t1_col values(9,  cast('一二' as char(6)));
insert into t1_col select 10, cast('一二' as char(6));
select * from t1_col order by id;
select id, length(info), lengthb(info) from t1_col order by id;

set td_compatible_truncation=on;

delete from t1;
insert into t1 values(1,'一');
insert into t1 values(2,  '一二');
insert into t1 values(5,  cast('一二' as char(6)));
insert into t1 select 6, cast('一二' as char(6));
select * from t1 order by id;
select id, length(info), lengthb(info) from t1 order by id;

delete from t1_col;
insert into t1_col values(11,  '一二');
insert into t1_col select 12, '一二';
select * from t1_col order by id;
select id, length(info), lengthb(info) from t1_col order by id;

commit;

\c regression
