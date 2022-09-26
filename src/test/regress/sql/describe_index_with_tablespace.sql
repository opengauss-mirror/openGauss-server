drop user if exists user_index_with_tablespace;
create user user_index_with_tablespace password 'Gauss_234';
create tablespace lm_tablespace_1 owner user_index_with_tablespace relative location 'my_location';
--1.建表
drop table if exists tem;
create table tem(c1 int,score number,primary key(c1));
insert into tem select generate_series(1,100);
drop table if exists lm_pre_index_022;
create table lm_pre_index_022 (
c1 int,
c2 varchar2 default 'xiaomimg',
c3 number,
c4 money,
c5 CHAR(20),
c6 CLOB,
c7 blob,
c8 DATE,
c9 BOOLEAN,
c10 TIMESTAMP,
c11 point,
columns12 cidr,primary key(c1),foreign key(c1) references tem(c1),check(c3>0),unique(c1))
with(segment=on,fillfactor=70,orientation=row) tablespace lm_tablespace_1;

--2.建索引
create index lm_pre_index_022_idx_03 on lm_pre_index_022(c7);
--3.
\d+ lm_pre_index_022
--4.
alter index lm_pre_index_022_idx_03 set tablespace lm_tablespace_1;
--5.
\d+ lm_pre_index_022