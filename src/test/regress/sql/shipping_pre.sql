create schema shipping_schema;
set current_schema='shipping_schema';
create node group ship_ng0 with (datanode1);
create table shipping_test_row (a int, b int, c text, d integer[]);
create table shipping_test_col (a int, b int, c text)with (orientation = column);
create table shipping_test_replicalte (a int, b int, c text, d integer[]) distribute by replication; 

create table t1(a timestamp,acct_id char(10))distribute by hash(acct_id);
create table t2(a timestamp,b char(8),acct_id char(10))distribute by hash(acct_id);
create table t3(a timestamp,acct_id char(10))distribute by hash(acct_id);


create table t4(a int, b int, c text);
create table t5(a int, b int, c text);
insert into t5 select v%5,v%7,v%2 from generate_series(1,16) as v;
insert into t4 select v%5,v%3,v%4 from generate_series(1,8) as v;

create table t6(a timestamp, b timestamptz, c interval, d int, e text);
create table t7(a timestamp, b timestamptz, c interval, d int, e text);

insert into t6 values ('1849-10-1','1889-10-1+08','1 year', 1, 'a');
insert into t6 values ('1859-10-1','1899-10-1+08','10 year', 2, 'b');
insert into t6 values ('1869-10-1','1909-10-1+08','30 year', 3, 'c');

insert into t7 values ('1849-10-1','1889-10-1+08','1 year', 1, 'a');
insert into t7 values ('1859-10-1','1899-10-1+08','10 year', 2, 'b');
insert into t7 values ('1869-10-1','1909-10-1+08','30 year', 3, 'c');

create table t8 (a float4, b float8, c int , d text)distribute by hash(c);
create table t9 (a float4, b float8, c int , d text)distribute by hash(c);
insert into t8 values(777.1,8888888.111111111,1,'a'),(666.82,33333.111111111,2,'b');
insert into t9 values(777.1,8888888.111111111,1,'a'),(666.87,33333.111111111,2,'b');

create table t10 (a text, b int, c timetz);
insert into t10 values('12-sep-2014', 1, '01:00:00+08'),('12-sep-2015', 2,'02:00:00+09'),('12-sep-2016', 3,'03:00:00 +10');

create view view_t10 as select a from t10;

CREATE  TABLE t11 (f0 int, f1 text, f2 float8);
insert into t11 values(1,'cat1',1.21),(2,'cat1',1.24),(3,'cat1',1.18),(4,'cat1',1.26),
(5,'cat1',1.15),(6,'cat2',1.15),(7,'cat2',1.26),(8,'cat2',1.32),(9,'cat2',1.30);

create table t12 (f0 int, f1 text, f2 float8) with (orientation = column);
insert into t12 select * from t11;

create table t13 (f0 int, f1 text, f2 float8);
insert into t13 select * from t11;
create index index_ship_t13 on t13(f0);

create table t14 (f0 int, f1 text, f2 float8) with (orientation = column) to group ship_ng0;
insert into t14 select * from t11;

create table t15 (f0 int, f1 text, f2 float8);
insert into t15 select * from t11;
create index index_ship_t15 on t15(f0);

create table ship_t1(a1 int, b1 int, c1 int, d1 int) with (orientation = column) distribute by hash(a1, b1);
create table ship_t2(a2 int, b2 int, c2 int, d2 int) with (orientation = column) distribute by hash(a2, b2);
create table ship_t4(a4 int, b4 int, c4 int, d4 int) with (orientation = column) distribute by hash(a4) to group  ship_ng0;
create table ship_t5(a5 int, b5 int, c5 int, d5 int) distribute by replication to group ship_ng0;
create data source ecshipping  options (dsn 'ecshipping');

create sequence seq1 ;
create table seq_t1(a int default nextval('seq1'), b int,  c bigint default nextval('seq1'));
create table seq_t2(a int, b int , c bigint);
create table seq_t3(a int, b int , c bigint default nextval('seq1') primary key);
