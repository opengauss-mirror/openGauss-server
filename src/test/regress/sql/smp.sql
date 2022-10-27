create schema test_smp;
set search_path=test_smp;

create table t1(a int, b int, c int, d bigint);
insert into t1 values(generate_series(1, 100), generate_series(1, 10), generate_series(1, 2), generate_series(1, 50));
create table t2(a int, b int);
insert into t2 values(generate_series(1, 10), generate_series(1, 30));
create table t3(a int, b int, c int);
insert into t3 values(generate_series(1, 50), generate_series(1, 100), generate_series(1, 10));
create table t4(id int, val text);
insert into t4 values(generate_series(1,1000), random());
create index on t4(id);

analyze t1;
analyze t2;
analyze t3;
analyze t4;

set query_dop=1002;
explain (costs off) select * from t2 order by 1,2;
select * from t2 order by 1,2;

set enable_nestloop=on;
set enable_mergejoin=off;
set enable_hashjoin=off;
explain (costs off) select t1.a,t2.b from t1, t2 where t1.a = t2.a order by 1,2;
select t1.a,t2.b from t1, t2 where t1.a = t2.a order by 1,2;

set enable_nestloop=off;
set enable_hashjoin=on;
explain (costs off) select t1.a,t2.b,t3.c from t1, t2, t3 where t1.a = t2.a and t1.b = t3.c order by 1,2,3;
select t1.a,t2.b,t3.c from t1, t2, t3 where t1.a = t2.a and t1.b = t3.c order by 1,2,3;

set enable_nestloop=on;
explain (costs off) select a, avg(b), sum(c) from t1 group by a order by 1,2,3;
select a, avg(b), sum(c) from t1 group by a order by 1,2,3;

explain (costs off) select median(a) from t1;
select median(a) from t1;

explain (costs off) select first(a) from t1;
select first(a) from t1;

explain (costs off) select sum(b)+median(a) as result from t1;
select sum(b)+median(a) as result from t1;

explain (costs off) select a, count(distinct b) from t1 group by a order by 1 limit 10;
select a, count(distinct b) from t1 group by a order by 1 limit 10;

explain (costs off) select count(distinct b), count(distinct c) from t1 limit 10;
select count(distinct b), count(distinct c) from t1 limit 10;

explain (costs off) select distinct b from t1 union all select distinct a from t2 order by 1;
select distinct b from t1 union all select distinct a from t2 order by 1;

explain (costs off) select * from t1 where t1.a in (select t2.a from t2, t3 where t2.b = t3.c) order by 1,2,3;
select * from t1 where t1.a in (select t2.a from t2, t3 where t2.b = t3.c) order by 1,2,3;

explain (costs off) with s1 as (select t1.a as a, t3.b as b from t1,t3 where t1.b=t3.c) select * from t2, s1 where t2.b=s1.a order by 1,2,3,4;
with s1 as (select t1.a as a, t3.b as b from t1,t3 where t1.b=t3.c) select * from t2, s1 where t2.b=s1.a order by 1,2,3,4;

explain (costs off) select * from t1 order by a limit 10;
select * from t1 order by a limit 10;

explain (costs off) select * from t1 order by a limit 10 offset 20;
select * from t1 order by a limit 10 offset 20;

-- test limit and offset
explain (costs off) select * from t1 limit 1;

explain (costs off) select * from t1 limit 1 offset 10;

explain (costs off) select * from t1 order by 1 limit 1 offset 10;
-- test subquery recursive
explain (costs off) select (select max(id) from t4);
select (select max(id) from t4);

explain (costs off) select * from (select a, rownum as row from (select a from t3) where rownum <= 10) where row >=5;
select * from (select a, rownum as row from (select a from t3) where rownum <= 10) where row >=5;

create table col_table_001 (id int, name char[] ) with (orientation=column);
create table col_table_002 (id int, aid int,name char[] ,apple char[]) with (orientation=column);
insert into col_table_001 values(1, '{a,b,c}' );
insert into col_table_001 values(2, '{b,b,b}' );
insert into col_table_001 values(3, '{c,c,c}' );
insert into col_table_001 values(4, '{a}' );
insert into col_table_001 values(5, '{b}' );
insert into col_table_001 values(6, '{c}' );
insert into col_table_001 values(7, '{a,b,c}' );
insert into col_table_001 values(8, '{b,c,a}' );
insert into col_table_001 values(9, '{c,a,b}' );
insert into col_table_001 values(10, '{c,a,b}' );
insert into col_table_002 values(11, 1,'{a,s,d}' );
insert into col_table_002 values(12, 1,'{b,n,m}' );
insert into col_table_002 values(13, 2,'{c,v,b}' );
insert into col_table_002 values(14, 1,'{a}' );
insert into col_table_002 values(15, 1,'{b}' );
insert into col_table_002 values(15, 2,'{c}' );
insert into col_table_002 values(17, 1,'{a,s,d}','{a,b,c}' );
insert into col_table_002 values(18, 1,'{b,n,m}','{a,b,c}' );
insert into col_table_002 values(19, 2,'{c,v,b}','{a,b,c}');
insert into col_table_002 values(20, 2,'{c,v,b}','{b,c,a}');
insert into col_table_002 values(21, 21,'{c,c,b}','{b,c,a}');
select * from col_table_001 where EXISTS (select * from col_table_002 where col_table_001.name[1] =col_table_002.apple[1]) order by id;
select * from col_table_001 where EXISTS (select * from col_table_002 where col_table_001.name[1:3] =col_table_002.apple[1:3]) order by id;

CREATE TABLE bmsql_item (
i_id int NoT NULL,
i_name varchar(24),
i_price numeric(5,2),
i_data varchar( 50),
i_im_id int
);
insert into bmsql_item values ('1','sqltest_varchar_1','0.01','sqltest_varchar_1','1') ;
insert into bmsql_item values ('2','sqltest_varchar_2','0.02','sqltest_varchar_2','2') ;
insert into bmsql_item values ('3','sqltest_varchar_3','0.03','sqltest_varchar_3','3') ;
insert into bmsql_item values ('4','sqltest_varchar_4','0.04','sqltest_varchar_4','4') ;
insert into bmsql_item(i_id) values ('5');

create table bmsql_warehouse(
w_id int not null,
w_ytd numeric(12,2),
w_tax numeric(4,4),
w_name varchar(10),
w_street_1 varchar(20),
w_street_2 varchar(20),
w_city varchar(20),
w_state char(2),
w_zip char(9)
);
insert into bmsql_warehouse values('1','0.01','0.0001','sqltest_va','sqltest_varchar_1','sqltest_varchar_1','sqltest_varchar_1','sq','sqltest_b');
insert into bmsql_warehouse values('2','0.02','0.0002','sqltest_va','sqltest_varchar_2','sqltest_varchar_2','sqltest_varchar_2','sq','sqltest_b');
insert into bmsql_warehouse values('3','0.03','0.0003','sqltest_va','sqltest_varchar_3','sqltest_varchar_3','sqltest_varchar_3','sq','sqltest_b');
insert into bmsql_warehouse values('4','0.04','0.0004','sqltest_va','sqltest_varchar_4','sqltest_varchar_4','sqltest_varchar_4','sq','sqltest_b');
insert into bmsql_warehouse(w_id) values('5');

set query_dop=4;

explain (costs off) select 0.01
from bmsql_item
intersect
select first_value(i_price) over (order by 2)
from bmsql_item
where i_id <=(select w_id from bmsql_warehouse
where bmsql_item.i_name not like 'sqltest_varchar_2' order by 1 limit 1)
group by i_price;

select 0.01
from bmsql_item
intersect
select first_value(i_price) over (order by 2)
from bmsql_item
where i_id <=(select w_id from bmsql_warehouse
where bmsql_item.i_name not like 'sqltest_varchar_2' order by 1 limit 1)
group by i_price;

CREATE FUNCTION f1(text) RETURNS int LANGUAGE SQL IMMUTABLE AS $$ SELECT $1::int;$$;

CREATE TABLE bmsql_history (
hist_id int,
h_c_id int,
h_c_d_id int,
h_c_w_id int,
h_d_id int,
h_w_id int,
h_date timestamp(6),
h_amount numeric(6,2),
h_data varchar( 24)
);

insert into bmsql_history values('1','1','1','1','1','1',to_timestamp('2010-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.01','sqltest_varchar_1') ;
insert into bmsql_history values('2','2','2','2','2','2',to_timestamp('2010-01-02 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.02','sqltest_varchar_2') ;
insert into bmsql_history values('3','3','3','3','3','3',to_timestamp('2010-01-03 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.03','sqltest_varchar_3') ;
insert into bmsql_history values('4','4','4','4','4','4',to_timestamp('2010-01-03 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.04','sqltest_varchar_4') ;
insert into bmsql_history(hist_id) values('') ;

explain (costs off) select f1('0') c1
union
select distinct i_id c2
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp);

select f1('0') c1
union
select distinct i_id c2
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp);

explain (costs off) select distinct i_id c2
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp)
union
select f1('0') c1;

select distinct i_id c2
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp)
union
select f1('0') c1;

select f1(0) c1
union 
select distinct var_pop(i_id) c2 
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp);

select distinct var_pop(i_id) c2 
from bmsql_item
where i_id not like (with tmp as (select distinct asin(0) as c1 from bmsql_history where bmsql_item.i_im_id <3)
select * from tmp)
union
select f1(0) c1;

CREATE TABLE bmsql_new_order (
no_w_id int NOT NULL,
no_d_id int NOT NULL,
no_o_id int NOT NULL
);
insert into bmsql_new_order values('1','1','1');
insert into bmsql_new_order values('2','2','2');
insert into bmsql_new_order values('3','3','3');
insert into bmsql_new_order values('4','4','4');
insert into bmsql_new_order values('5','5','5');

select count(*)
from (select distinct greatest (no_d_id,no_d_id,no_d_id)
from bmsql_new_order
where no_o_id not in ( with tmp as (select w_id from bmsql_warehouse where bmsql_new_order.no_w_id >2 or w_id >=3) select * from tmp)) tb1,
( select count(*) from bmsql_item group by i_im_id,i_im_id having i_im_id like f1('0')
) tb2;

--clean
set search_path=public;
drop schema test_smp cascade;
