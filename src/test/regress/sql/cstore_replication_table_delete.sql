/*repliction columnar table to support delete*/
create schema replication_update_and_delete;
set current_schema=replication_update_and_delete;

drop table if exists base_table;
drop table if exists hash_base_table;
create table base_table(a int,b int,c int);
insert into base_table values(1,1,1);
insert into base_table values(2,1,1);
insert into base_table values(2,1,2);
insert into base_table values(2,2,12);
insert into base_table values(3,5,7);
insert into base_table values(3,5,7);
insert into base_table values(1,1,10);
insert into base_table values(2,2,10);
insert into base_table values(3,3,5);
insert into base_table values(3,3,5);
insert into base_table values(4,6,8);
insert into base_table values(4,6,8);

create table hash_base_table(a int,b int,c text,d int);
insert into hash_base_table values(1,1,'gauss',1);
insert into hash_base_table values(2,1,'gauss1',1);
insert into hash_base_table values(2,1,'gauss2',2);
insert into hash_base_table values(2,2,'gauss3',10);
insert into hash_base_table values(3,5,'gauss4',10);

/*-----case1: delete from column_replication_table where...;*/
drop table if exists col_rep_tb1;
create table col_rep_tb1(a int,b int,c int) with(orientation=column)  ;
insert into col_rep_tb1 select * from base_table;

delete from col_rep_tb1 where col_rep_tb1.a>1 and col_rep_tb1.c<10;
select count(*) from col_rep_tb1 where col_rep_tb1.a>1 and col_rep_tb1.c<10;
select * from col_rep_tb1 order by a,b,c;


/*-----case2: delete from column_replication_table using hash_table where...;*/
drop table if exists col_rep_tb2;
drop table if exists hash_tb1;
create table col_rep_tb2(a int,b int,c int) with(orientation=column)  ;
insert into col_rep_tb2 select * from base_table;

create table hash_tb1(a int,b int,c text,d int);
insert into hash_tb1 select * from hash_base_table;
explain (verbose on,costs off) delete from col_rep_tb2 using hash_tb1 where col_rep_tb2.a=hash_tb1.b and col_rep_tb2.c>2;
explain (ANALYSE on,costs off, timing off) delete from col_rep_tb2 using hash_tb1 where col_rep_tb2.a=hash_tb1.b and col_rep_tb2.c>2;
select * from col_rep_tb2 order by a,b,c;


/*-----case3: delete from column_replication_table where...in;*/
drop table if exists col_rep_tb3;
drop table if exists hash_tb2;

create table col_rep_tb3(a int,b int,c int) with(orientation=column)  ;
insert into col_rep_tb3 select * from base_table;

create table hash_tb2(a int,b int,c text,d int);
insert into hash_tb2 select * from hash_base_table;

delete from col_rep_tb3 where a in(select b from hash_tb2);
select count(*) from col_rep_tb3 where a in(select b from hash_tb2);


/*-----case 4: delete from column_replication_table1 using column_replication_table2 where...;*/
drop table if exists col_rep_tb4;
drop table if exists col_rep_tb5;
create table col_rep_tb4(a int,b int,c int) with(orientation=column)  ;
insert into col_rep_tb4 values(1,1,1);
insert into col_rep_tb4 values(2,10,1);
insert into col_rep_tb4 values(2,1,2);
insert into col_rep_tb4 values(2,12,12);
insert into col_rep_tb4 values(3,5,7);

create table col_rep_tb5(a int,b int) with(orientation=column)  ;
insert into col_rep_tb5 values(1,1);
insert into col_rep_tb5 values(2,1);
insert into col_rep_tb5 values(2,1);
insert into col_rep_tb5 values(2,12);
insert into col_rep_tb5 values(3,10);
insert into col_rep_tb5 values(5,17);

delete from col_rep_tb4 using col_rep_tb5 where col_rep_tb4.b=col_rep_tb5.b and col_rep_tb4.a>1;
select * from col_rep_tb4 order by a,b,c;


/*-----case 5:with...delete;*/
drop table if exists col_rep_tb1_with;
drop table if exists hash_tb_with;
create table col_rep_tb1_with( col1 int, col2 int ,col3 text) with(orientation=column)  ;
insert into col_rep_tb1_with values(1,1,'gauss1') ;
insert into col_rep_tb1_with values(1,2,'gauss2') ;
insert into col_rep_tb1_with values(2,1,'gauss3') ;
insert into col_rep_tb1_with values(3,10,'gauss4') ;
insert into col_rep_tb1_with values(10,5,'gauss5') ;
insert into col_rep_tb1_with values(10,20,'gauss6') ;

create table hash_tb_with(a int ,b int);
insert into hash_tb_with values (5,10);
insert into hash_tb_with values (1,1);
insert into hash_tb_with values (20,10);

with tempTable(col1,col2) as (
	select a,b from hash_tb_with
)
delete from col_rep_tb1_with using tempTable where col_rep_tb1_with.col1 = tempTable.col2 and col_rep_tb1_with.col2 =tempTable.col1 and col_rep_tb1_with.col1>1;
select * from col_rep_tb1_with order by col1,col2,col3;


/*-----case 6:delete on transction;*/
drop  table if exists create_columnar_repl_trans_001;
drop  table if exists row_base_table;
create table row_base_table
(c_boolean boolean default true,
c_tinyint tinyint default 100,
c_smallint smallint default 32766,
c_integer integer default 50000,
c_bigint bigint default 1000,
c_numeric numeric(18,2) default 225.2,
c_real real default 223,
c_double_precision double precision default 10,
c_binary_double binary_double default 100,
c_dec dec(5,3) default 3.456,
c_integer_p integer(5,3) default 2.22,
c_char char(20) not null,
c_varchar varchar(20) not null,
c_varchar2 varchar2(10) null,
c_nvarchar2 nvarchar2(10) null,
c_text text null,
c_date date default '2015-01-01' );

insert into row_base_table values(true,default,500,generate_series(1,200),default,default,100,default,default,default,default,'A','today',null,null,null,default);
insert into row_base_table values(false,default,generate_series(-100,100),default,default,default,100,default,default,default,default,'A','today',null,null,null,default);
insert into row_base_table (c_char,c_varchar)values(generate_series(1,200),100);

create table create_columnar_repl_trans_001
(c_boolean boolean default true,
c_tinyint tinyint default 100,
c_smallint smallint default 32766,
c_integer integer default 50000,
c_bigint bigint default 1000,
c_numeric numeric(18,2) default 225.2,
c_real real default 223,
c_double_precision double precision default 10,
c_binary_double binary_double default 100,
c_dec dec(5,3) default 3.456,
c_integer_p integer(5,3) default 2.22,
c_char char(20) not null,
c_varchar varchar(20) not null,
c_varchar2 varchar2(10) null,
c_nvarchar2 nvarchar2(10) null,
c_text text null,
c_date date default '2015-01-01' ) with (orientation=column, compression = high) 
 ;
insert into create_columnar_repl_trans_001 select * from row_base_table;

select count(*) from create_columnar_repl_trans_001;
create index create_index_repl_trans_001  on create_columnar_repl_trans_001(c_date);
start transaction isolation level read committed;
delete from create_columnar_repl_trans_001 where c_integer<10;
rollback;
select count(*) from create_columnar_repl_trans_001;

/*-----case 7:delete on transction2;*/
drop  table if exists create_columnar_repl_trans_002;
drop  table if exists trans_base_table;

create table trans_base_table
(
	 a_tinyint tinyint ,
	 a_smallint smallint not null,
	 a_numeric numeric(18,2) , 
	 a_decimal decimal null,
	 a_real real null,
	 a_double_precision double precision null,
	 a_dec   dec ,
	 a_integer   integer default 100,
	 a_char char(5) not null,
	 a_varchar varchar(15) null,
	 a_nvarchar2 nvarchar2(10) null,
	 a_text text   null,
	 a_date date default '2015-07-07',
	 a_time time without time zone,
	 a_timetz time with time zone default '2013-01-25 23:41:38.8',
	 a_smalldatetime smalldatetime,
	 a_money  money not null,
	 a_interval interval
);
insert into trans_base_table (a_smallint,a_char,a_text,a_money) values(generate_series(1,500),'fkdll','65sdcbas',20);
insert into trans_base_table (a_smallint,a_char,a_text,a_money) values(100,'fkdll','65sdcbas',generate_series(1,400));

create table create_columnar_repl_trans_002
(
	 a_tinyint tinyint ,
	 a_smallint smallint not null,
	 a_numeric numeric(18,2) , 
	 a_decimal decimal null,
	 a_real real null,
	 a_double_precision double precision null,
	 a_dec   dec ,
	 a_integer   integer default 100,
	 a_char char(5) not null,
	 a_varchar varchar(15) null,
	 a_nvarchar2 nvarchar2(10) null,
	 a_text text   null,
	 a_date date default '2015-07-07',
	 a_time time without time zone,
	 a_timetz time with time zone default '2013-01-25 23:41:38.8',
	 a_smalldatetime smalldatetime,
	 a_money  money not null,
	 a_interval interval,
partial cluster key(a_smallint)) with (orientation=column, compression = high)  ;

create index create_index_repl_trans_002 on create_columnar_repl_trans_002(a_smallint,a_date,a_integer);
insert into create_columnar_repl_trans_002 select * from trans_base_table;

start transaction;
alter table  create_columnar_repl_trans_002 add column a_char_01 char(20) default '中国制造';
insert into  create_columnar_repl_trans_002 (a_smallint,a_char,a_money,a_char_01) values(generate_series(1,10),'li',21.1,'高斯部');
delete from create_columnar_repl_trans_002 where a_smallint>5 and a_char_01='高斯部';
rollback;
select count(*) from create_columnar_repl_trans_002 where a_smallint>5 and a_char_01='高斯部';

start transaction;
alter table  create_columnar_repl_trans_002 add column a_char_01 char(20) default '中国制造';
insert into  create_columnar_repl_trans_002 (a_smallint,a_char,a_money,a_char_01) values(generate_series(1,10),'li',21.1,'高斯部');
delete from create_columnar_repl_trans_002 where a_smallint>5 and a_char_01='高斯部';
commit;
select  count(*) from create_columnar_repl_trans_002 where a_smallint>5 and a_char_01='高斯部';


/*-----case 8:delete on view;*/
drop  table if exists create_columnar_repl_view_001 cascade;
create table create_columnar_repl_view_001
 (
 a_tinyint tinyint ,
 a_smallint smallint not null,
 a_numeric numeric(18,2) , 
 a_decimal decimal null,
 a_real real null,
 a_double_precision double precision null,
 a_dec   dec ,
 a_integer   integer default 100,
 a_char char(5) not null,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text   null,
 a_date date default '2015-07-07',
 a_time time without time zone,
 a_timetz time with time zone default '2013-01-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_money  money not null,
 a_interval interval,
 partial cluster key(a_smallint)) with (orientation=column, compression = high)  ;

create index create_index_repl_view_001 on create_columnar_repl_view_001(a_smallint,a_date,a_integer);
insert into create_columnar_repl_view_001  select * from trans_base_table;

CREATE  OR REPLACE VIEW create_columnar_repl_view_001_01 AS  SELECT  a_smallint,a_char,a_text,a_money FROM create_columnar_repl_view_001;
select count(*) from create_columnar_repl_view_001_01 where a_smallint>400;
alter table  create_columnar_repl_view_001 add column a_char_01 char(20) default '中国制造';
insert into  create_columnar_repl_view_001 (a_smallint,a_char,a_money,a_char_01) values(generate_series(1,10),'li',21.1,'高斯部');
alter table  create_columnar_repl_view_001 modify (a_tinyint bigint) ; 

start transaction;
delete from create_columnar_repl_view_001 where a_smallint>5 and a_char_01='高斯部';
commit;
select  count(*) from create_columnar_repl_view_001 where a_smallint>5 and a_char_01='高斯部';


/*-----case9: delete from table_name [AS]alias;*/
drop  table if exists create_columnar_repl_common_002; 
create table create_columnar_repl_common_002
 (
 a_tinyint tinyint ,
 a_smallint smallint not null,
 a_numeric numeric(18,2) , 
 a_decimal decimal null,
 a_real real null,
 a_double_precision double precision null,
 a_dec   dec ,
 a_integer   integer default 100,
 a_char char(5) not null,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text   null,
 a_date date default '2015-07-07',
 a_time time without time zone,
 a_timetz time with time zone default '2013-01-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_money  money not null,
 a_interval interval ) with (orientation=column, compression = high)  ;

create index replication_common_indexes_010_1 on create_columnar_repl_common_002(a_smallint,a_date,a_integer);
create index replication_common_indexes_010_2 on create_columnar_repl_common_002(a_text);
insert into create_columnar_repl_common_002 select * from trans_base_table;;

delete from create_columnar_repl_common_002 * where a_smallint>5 and a_money=cast(20 as money);
select count(*) from create_columnar_repl_common_002 * where a_smallint>5 and a_money=cast(20 as money);
delete from create_columnar_repl_common_002 as t1 where t1.a_smallint>5 and a_text='65sdcbas';
select count(*) from create_columnar_repl_common_002 as t1 where t1.a_smallint>5 and a_text='65sdcbas';



/*-----case10: delete col_rep_partition_table;*/
drop  table if exists create_columnar_repl_partition_table_006;
drop  table if exists part_base_table;

create table part_base_table (c_boolean boolean default true,c_tinyint tinyint default 100,c_smallint smallint default 32766,c_integer integer default 50000,c_bigint bigint default 1000,c_numeric numeric(18,2) default 2222.22, c_real real default 333.33,c_double_precision double precision default 100,c_binary_double binary_double default 200,c_dec dec(5,3) default 3.456,c_integer_p integer(5,3) default 2.32766,c_char char(1) not null,c_varchar varchar(10) not null,c_varchar2 varchar2(10) null,c_nvarchar2 nvarchar2(10) null,c_text text null,c_date date default '2000-01-01');

insert into part_base_table values(false,default,generate_series(-100,100),default,default,default,random(),default,default,default,default,'A','today',null,null,null,default);

create table create_columnar_repl_partition_table_006 (c_boolean boolean default true,c_tinyint tinyint default 100,c_smallint smallint default 32766,c_integer integer default 50000,c_bigint bigint default 1000,c_numeric numeric(18,2) default 2222.22, c_real real default 333.33,c_double_precision double precision default 100,c_binary_double binary_double default 200,c_dec dec(5,3) default 3.456,c_integer_p integer(5,3) default 2.32766,c_char char(1) not null,c_varchar varchar(10) not null,c_varchar2 varchar2(10) null,c_nvarchar2 nvarchar2(10) null,c_text text null,c_date date default '2000-01-01') with (orientation=column)  
partition by range(c_smallint)
(partition col_rep_partition_table_017_p1 values less than(0),
 partition col_rep_partition_table_017_p2 values less than(32767),
 partition col_rep_partition_table_017_p3 values less than(maxvalue));

insert into create_columnar_repl_partition_table_006 select * from part_base_table;

create index  create_columnar_repl_part_index_01 on create_columnar_repl_partition_table_006(c_smallint,c_date) local;
delete from create_columnar_repl_partition_table_006 where c_smallint<50 and lower(c_char)='a';
select count(0) from create_columnar_repl_partition_table_006 where c_smallint<50 and lower(c_char)='a';


/*-----case11:truncate;*/
drop  table if exists create_columnar_repl_common_008; 
drop  table if exists rep_base_table;

create table rep_base_table
 (
 a_tinyint tinyint ,
 a_numeric numeric(18,2) , 
 a_decimal decimal not null,
 a_integer  integer default 50,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text  not null,
 a_date date ,
 a_timestamp timestamp without time zone,
 a_timestamptz timestamp with time zone default '2013-02-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_interval interval);
 
insert into rep_base_table (a_tinyint,a_numeric,a_decimal,a_text) values (generate_series(1,100),653,135.5,'hjsdkad');
insert into rep_base_table (a_tinyint,a_numeric,a_decimal,a_text,a_integer) values (100,65223,122.2,'hjsdkad',generate_series(1,500));

 create table create_columnar_repl_common_008
 (
 a_tinyint tinyint ,
 a_numeric numeric(18,2) , 
 a_decimal decimal not null,
 a_integer  integer default 50,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text  not null,
 a_date date ,
 a_timestamp timestamp without time zone,
 a_timestamptz timestamp with time zone default '2013-02-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_interval interval , partial cluster key(a_date,a_numeric)) with (orientation=column, compression = high)  ;
 insert into create_columnar_repl_common_008 select * from rep_base_table;

truncate create_columnar_repl_common_008;
select count(*)from create_columnar_repl_common_008;
delete from create_columnar_repl_common_008;


/*-----case12: delete where ...is null;*/
drop  table if exists create_columnar_repl_common_009;

create table create_columnar_repl_common_009
 (
 a_tinyint tinyint ,
 a_numeric numeric(18,2) , 
 a_decimal decimal not null,
 a_integer  integer default 50,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text  not null,
 a_date date ,
 a_timestamp timestamp without time zone,
 a_timestamptz timestamp with time zone default '2013-02-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_interval interval ,partial cluster key(a_date,a_numeric)) with (orientation=column, compression = high)  ;

insert into create_columnar_repl_common_009 select * from rep_base_table;

delete from create_columnar_repl_common_009 where a_integer=500 and a_nvarchar2 is null and a_decimal is not null;
select count(*) from create_columnar_repl_common_009 where a_integer=500 and a_nvarchar2 is null and a_interval is not null;


/*-----case13: delete ....returning. */
drop  table if exists create_columnar_repl_common_err_001;
create table create_columnar_repl_common_err_001
 (
 a_tinyint tinyint ,
 a_numeric numeric(18,2) , 
 a_decimal decimal not null,
 a_integer  integer default 50,
 a_varchar varchar(15) null,
 a_nvarchar2 nvarchar2(10) null,
 a_text text  not null,
 a_date date ,
 a_timestamp timestamp without time zone,
 a_timestamptz timestamp with time zone default '2013-02-25 23:41:38.8',
 a_smalldatetime smalldatetime,
 a_interval interval , partial cluster key(a_date,a_numeric)) with (orientation=column, compression = high)  ;
 
insert into create_columnar_repl_common_err_001 select * from rep_base_table;

delete from  create_columnar_repl_common_err_001 where  a_tinyint > 50 returning *;
select count(*) from  create_columnar_repl_common_err_001 where  a_tinyint > 50 ;


/*-----case13: update;*/
drop table if exists col_rep_tb1_update;
create table col_rep_tb1_update(a int,b int,c int) with(orientation=column)  ;
insert into col_rep_tb1_update select * from base_table;
select * from col_rep_tb1_update order by a,b,c;
update col_rep_tb1_update SET a=a*10,b=b*10 WHERE col_rep_tb1_update.c > 5 and col_rep_tb1_update.a=col_rep_tb1_update.b;
select * from col_rep_tb1_update order by a,b,c;


/*-----case14: 不能下推的场景;*/
drop table if exists col_rep_tb_concat;
create table col_rep_tb_concat(a text ,b text,c text ) with(orientation=column)  ;
insert into col_rep_tb_concat values('gauss','1','abc');
insert into col_rep_tb_concat values('gauss','1','abc');
insert into col_rep_tb_concat values('gauss','2','abc');

delete from col_rep_tb_concat where a in (select distinct on (b)b from col_rep_tb_concat);


/*-----case15: row_repliction_table has no primary key to support delete;*/
drop table if exists row_rep_tb;
drop table if exists hash_tb3;
create table row_rep_tb(a int,b int,c int)  ;
insert into row_rep_tb select * from base_table;

create table hash_tb3(a int,b int,c text,d int);
insert into hash_tb3 select * from hash_base_table;

explain (verbose on,costs off) delete from row_rep_tb using hash_tb3 where row_rep_tb.a=hash_tb3.b and row_rep_tb.c>2;
explain (ANALYSE on,costs off, timing off) delete from row_rep_tb using hash_tb3 where row_rep_tb.a=hash_tb3.b and row_rep_tb.c>2;
select * from row_rep_tb order by a,b,c;



/*-----case16: update：update row_replication_table set ... where...*/
drop table if exists row_rep_tb1_update;
create table row_rep_tb1_update(a int,b int,c int) ;
insert into row_rep_tb1_update select * from base_table;
select * from row_rep_tb1_update order by a,b,c;
update row_rep_tb1_update SET a=a*10,b=b*10 WHERE row_rep_tb1_update.c > 9 and row_rep_tb1_update.a=row_rep_tb1_update.b;
select * from row_rep_tb1_update order by a,b,c;


/*-----case17: update：update row_replication_table set ... from hash_table where ...*/
drop table if exists row_rep_tb2_update;
drop table if exists hash_table01;

create table row_rep_tb2_update(a int,b int,c int) ;
insert into row_rep_tb2_update select * from base_table;
select * from row_rep_tb2_update order by a,b,c;

create table hash_table01 (a int, b int);
insert into hash_table01 values(1,0);
insert into hash_table01 values(3,0);

update row_rep_tb2_update UP SET UP.a=UP.a*10,UP.b=UP.b*10 from hash_table01 WHERE hash_table01.a=UP.a and UP.c<10 ;
select * from row_rep_tb2_update order by a,b,c;


/*-----case18: update：update row_replication_table set ... from replication_table where ... */
drop table if exists row_rep_tb3_update;
drop table if exists row_rep_tb4_update;

create table row_rep_tb3_update(a int,b int,c int) ;
insert into row_rep_tb3_update select * from base_table;
select * from row_rep_tb3_update order by a,b,c;

create table row_rep_tb4_update (a int, b int) with (orientation=column) ;
insert into row_rep_tb4_update values(1,0);
insert into row_rep_tb4_update values(3,0);

update row_rep_tb3_update UP SET UP.a=UP.a*10,UP.b=UP.b*10 from row_rep_tb4_update WHERE row_rep_tb4_update.a=UP.a and UP.c<10 ;
select * from row_rep_tb3_update order by a,b,c;



/*-----case19: 行存复制表没有主键，update不能下推的场景;*/
drop table if exists col_rep_tb_concat1;
create table col_rep_tb_concat1(a text ,b text,c text ,d int)  ;
insert into col_rep_tb_concat1 values('gauss1','1','abc',1);
insert into col_rep_tb_concat1 values('gauss2','1','abc',1);
insert into col_rep_tb_concat1 values('gauss3','2','abc',1);

update col_rep_tb_concat1 set d=d*10 where a in (select distinct on (b)b from col_rep_tb_concat);


/*-----case20: 行存复制表有主键的update;*/
drop table if exists col_rep_tb_concat2;
create table col_rep_tb_concat2(a text PRIMARY KEY,b text,c text,d int)  ;
insert into col_rep_tb_concat2 values('gauss1','1','abc',1);
insert into col_rep_tb_concat2 values('gauss2','1','abc',1);
insert into col_rep_tb_concat2 values('gauss3','2','abc1',1);

update col_rep_tb_concat2 set d=d*10 where concat(c,b)='abc1';



drop schema replication_update_and_delete CASCADE;