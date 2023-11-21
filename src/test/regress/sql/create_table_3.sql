-- test mysql "create table as"
-- a format
create schema a_createas;
set current_schema to 'a_createas';

create table t_base(col1 int, col2 int, col3 int);

create table t1 as select * from t_base;
select * from t1;
create table t2(col) as select * from t_base;
select * from t2;
-- fail
create table t3(col1,col2,col3,col4) as select * from t_base;
create table t4(col int) as select * from t_base;

reset current_schema;
drop schema a_createas cascade;

-- b format
create database b_createas dbcompatibility 'B';
\c b_createas

create table t_base(col1 int, col2 int, col3 int);

create table t1 as select * from t_base;
select * from t1;
create table t2(col) as select * from t_base;
select * from t2;
create table t3(col int) as select * from t_base;
select * from t3;
create table t4(col1 int) as select * from t_base;
select * from t4;

-- fail
create table t5() as select * from t_base;
create table t6(col1 int) as select col1,* from t_base;

-- duplicate key
insert into t_base values(1,1,10),(1,2,9),(2,2,8),(2,1,7),(1,1,6);
-- error
create table t7(col1 int unique) as select * from t_base;
-- ignore
create table t8(col1 int unique) ignore as select * from t_base;
select * from t8;
-- replace
create table t9(col1 int unique) replace as select * from t_base;
select * from t9 order by col3;
create table t10(col1 int unique, col2 int unique) replace as select * from t_base;
select * from t10 order by col3;

-- foreign key
create table ftable(col int primary key);
create table t11(col int, foreign key(col) references ftable(col)) as select * from t_base;
create table t12(col int references ftable(col)) as select * from t_base;
create table t13(foreign key(col1) references ftable(col)) as select * from t_base;
-- table like
create table t14(like t_base) as select * from t_base;

-- with no data
create table t15(id int, name char(8));
insert into t15(id) select generate_series(1,10);
create table t16(col1 int, id int) as select * from t15 where id with no data;
select * from t16;
create table t17(col1 int, id int unique) replace as select * from t15 where id with no data;
select * from t17;

-- union all
create table test1(id int,name varchar(10),score numeric,date1 date,c1 bytea);
insert into test1 values(1,'aaa',97.1,'1999-12-12','0101');
insert into test1 values(5,'bbb',36.9,'1998-01-12','0110');
insert into test1 values(30,'ooo',90.1,'2023-01-30','1001');
insert into test1 values(6,'hhh',60,'2022-12-22','1010');
insert into test1 values(7,'fff',71,'2001-11-23','1011');
insert into test1 values(-1,'yaya',77.7,'2008-09-10','1100');
insert into test1 values(7,'fff',71,'2001-11-23','1011');
insert into test1 values(null,null,null,null,null);
create table test2(id int,name varchar(10),score numeric,date1 date,c1 bytea);
insert into test2 values(1,'aaa',99.1,'1998-12-12','0101');
insert into test2 values(2,'hhh',36.9,'1996-01-12','0110');
insert into test2 values(3,'ddd',89.2,'2000-03-12','0111');
insert into test2 values(7,'uuu',60.9,'1997-01-01','1000');
insert into test2 values(11,'eee',71,'2011-11-20','1011');
insert into test2 values(-1,'yaya',76.7,'2008-09-10','1100');
insert into test2 values(7,'uuu',60.9,'1997-01-01','1000');
insert into test2 values(null,null,null,null,null);
create table tb1(col1 int,id int) as select * from test1 where id<4 union all select * from test2 where score>80 order by id,score;
select * from tb1 order by id;
create table tb2(col1 int,id int unique) replace as select * from test1 where id<4 union all select * from test2 where score>80 order by id,score;
select * from tb2 order by id;

-- test update
create table tb_primary(a int primary key, b int);
create table tb_unique(a int unique, b int);
insert into tb_primary values(1,2),(2,4),(3,6);
insert into tb_unique values(1,2),(2,4),(3,6);
-- error
insert into tb_primary values(1,1);
insert into tb_unique values(1,1);
-- UPDATE nothing
insert into tb_primary values(1,1) ON DUPLICATE KEY UPDATE NOTHING;
insert into tb_unique values(1,1) ON DUPLICATE KEY UPDATE NOTHING;
select * from tb_primary;
select * from tb_unique;
-- UPDATE
insert into tb_primary values(1,1) ON DUPLICATE KEY UPDATE a = 1, b = 1;
insert into tb_unique values(1,1) ON DUPLICATE KEY UPDATE a = 1, b = 1;
select * from tb_primary;
select * from tb_unique;

--fixbug
drop table if exists t_base;
create table t_base(col1 int, col2 int, col3 int);
insert into t_base values(1,2,3),(11,22,33);
create table ttt3(col int) as select * from t_base;
select * from ttt3;

CREATE TABLE table2 AS WITH RECURSIVE table1 ( pivot0 ) AS ( ( SELECT 1 UNION SELECT 1 LIMIT 1 ) UNION SELECT pivot0 + 1 FROM table1 WHERE pivot0 < 1 ) SELECT 1 ;

\c postgres
drop database b_createas;
