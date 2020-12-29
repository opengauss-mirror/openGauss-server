--
-- XC_FOR_UPDATE
--


-- create some tables
create table t1(val int, val2 int);
create table t2(val int, val2 int);
create table t3(val int, val2 int);

create table p1(a int, b int);
create table c1(a int, b int,d int, e int);

-- insert some rows in them
insert into t1 values(1,11),(2,11);
insert into t2 values(3,11),(4,11);
insert into t3 values(5,11),(6,11);

insert into p1 values(55,66),(77,88),(111,222),(123,345);
insert into c1 values(111,222,333,444),(123,345,567,789);

select * from t1 order by val;
select * from t2 order by val;
select * from t3 order by val;
select * from p1 order by a;
select * from c1 order by a;

-- create a view too
create view v1 as select * from t1 for update;

-- test a few queries with row marks
select * from t1 order by 1 for update of t1 nowait;
select * from t1, t2, t3 order by 1 for update;

select * from v1 order by val;
WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1,t2 order by 1 FOR UPDATE;

WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1;
WITH q1 AS (SELECT * from t1 order by 1) SELECT * FROM q1 FOR UPDATE;
WITH q1 AS (SELECT * from t1 order by 1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;


-- confirm that in various join scenarios for update gets to the remote query
-- single table case
explain (costs off, num_verbose on)  select * from t1 for update of t1 nowait;

-- two table case
explain (costs off, num_verbose on)  select * from t1, t2 where t1.val = t2.val for update nowait;
explain (costs off, num_verbose on)  select * from t1, t2 where t1.val = t2.val for update;
explain (costs off, num_verbose on)  select * from t1, t2 where t1.val = t2.val for share;
explain (costs off, num_verbose on)  select * from t1, t2 where t1.val = t2.val;
explain (costs off, num_verbose on)  select * from t1, t2;
explain (costs off, num_verbose on)  select * from t1, t2 for update;
explain (costs off, num_verbose on)  select * from t1, t2 for update nowait;
explain (costs off, num_verbose on)  select * from t1, t2 for share nowait;
explain (costs off, num_verbose on)  select * from t1, t2 for share;
explain (costs off, num_verbose on)  select * from t1, t2 for share of t2;

-- three table case
explain (costs off, num_verbose on)  select * from t1, t2, t3;
explain (costs off, num_verbose on)  select * from t1, t2, t3 for update;
explain (costs off, num_verbose on)  select * from t1, t2, t3 for update of t1;
explain (costs off, num_verbose on)  select * from t1, t2, t3 for update of t1,t3;
explain (costs off, num_verbose on)  select * from t1, t2, t3 for update of t1,t3 nowait;
explain (costs off, num_verbose on)  select * from t1, t2, t3 for share of t1,t2 nowait;

-- check a few subquery cases
explain (costs off, num_verbose on)  select * from (select * from t1 for update of t1 nowait) as foo;
explain (costs off, num_verbose on)  select * from t1 where val in (select val from t2 for update of t2 nowait) for update;
explain (costs off, num_verbose on)  select * from t1 where val in (select val from t2 for update of t2 nowait);

-- test multiple row marks
explain (costs off, num_verbose on)  select * from t1, t2 for share of t2 for update of t1;
-- make sure FOR UPDATE takes prioriy over FOR SHARE when mentioned for the same table
explain (costs off, num_verbose on)  select * from t1 for share of t1 for update of t1;
explain (costs off, num_verbose on)  select * from t1 for update of t1 for share of t1;
explain (costs off, num_verbose on)  select * from t1 for share of t1 for share of t1 for update of t1;
explain (costs off, num_verbose on)  select * from t1 for share of t1 for share of t1 for share of t1;
-- make sure NOWAIT is used in remote query even if it is not mentioned with FOR UPDATE clause
explain (costs off, num_verbose on)  select * from t1 for share of t1 for share of t1 nowait for update of t1;
-- same table , different aliases and different row marks for different aliases
explain (costs off, num_verbose on)  select * from t1 a,t1 b for share of a for update of b;

-- test WITH queries
-- join of a WITH table and a normal table
explain (costs off, num_verbose on)  WITH q1 AS (SELECT * from t1 FOR UPDATE) SELECT * FROM q1,t2 FOR UPDATE;

explain (costs off, num_verbose on)  WITH q1 AS (SELECT * from t1) SELECT * FROM q1;
-- make sure row marks are no ops for queries on WITH tables
explain (costs off, num_verbose on)  WITH q1 AS (SELECT * from t1) SELECT * FROM q1 FOR UPDATE;
explain (costs off, num_verbose on)  WITH q1 AS (SELECT * from t1 FOR UPDATE) SELECT * FROM q1 FOR UPDATE;

-- test case of inheried tables
select * from p1 order by 1 for update;
explain (costs off, num_verbose on)  select * from p1 for update;

select * from c1 order by 1 for update;
explain (costs off, num_verbose on)  select * from c1 for update;

-- drop objects created
drop table c1;
drop table p1;
drop view v1;
drop table t1;
drop table t2;
drop table t3;

--new add test cases from developer test and customer's case
create table t1(a int,b int,c int);
create table t2(a int,b int,c int);
create table t3(a int,b int,c int);
create table rep(a int,b int,c int) distribute by replication;
create table vt(a int,b int,c int) with(orientation = column);
create foreign table ft (id int not null) server gsmpp_server options(format 'text', location 'gsfs://127.0.0.1:99999/test.data', mode 'normal', delimiter '|');
create view view_t1 as select * from t1;
create view view_vt as select * from vt;
create view view_ft as select * from ft;
insert into t1 values(1,1,1);
insert into t1 values(2,2,2);
insert into t1 values(3,3,3);
insert into t2 values(3,3,3);
insert into t2 values(4,4,4);
insert into t3 values(1,1,1);
insert into t3 values(2,2,2);
insert into t3 values(3,3,3);

--test support table type and view for update
select * from t1 for update;
select * from view_t1 for update;
select * from vt for update;
select * from view_vt for update;
select * from ft for update;
select * from view_ft for update;

--test case from customer's application
select * from t1 m where exists (select * from t2 n where m.a+2=n.b) order by a for update;
select * from t1 t where exists (select * from t3 a where a.a = t.a) order by a for update;
select * from t1 t where exists (select 1 from (select b.a from t2 b where exists (select 1 from t3 t1 where t1.a = b.a)) c where c.a = t.a) order by a for update;

select * from t1 m where exists (select * from t2 n where m.a+2=n.b for update) for update;
select 1 from t1 t where exists (select * from t3 a where a.a = t.a for update) for update;
select * from t1 m where exists (select * from t2 n where m.a+2=n.b order by a for update) order by a for update;
select 1 from t1 t where exists (select 1 from (select b.a from t2 b where exists (select 1 from t3 t1 where t1.a = b.a)) c where c.a = t.a for update) for update;

--test unsupport case of multiple FOR UPDATE/SHARE or multiple lock table
select * from t1,(select * from t1 for update) for update of t1;
select * from (select * from t1 for update) as a, (select * from t1 for update) as b for update;

--no need add redistribute specially
explain verbose select * from t1,t2 where t1.a=t2.a for update of t1;
--no need add redistribute specially
explain verbose select * from t1,t2 where t1.a=t2.b for update of t1;
--need add redistribute specially
explain verbose select * from t1,t2 where t1.b=t2.b for update of t1;
--need add redistribute specially
explain verbose select * from t1,t2 where t1.b=t2.a for update of t1;
--no need add redistribute specially
explain verbose select * from t1,t2 where t1.a=t2.a for update of t2;
--need add redistribute specially
explain verbose select * from t1,t2 where t1.a=t2.b for update of t2;
--need add redistribute specially
explain verbose select * from t1,t2 where t1.b=t2.b for update of t2;
--no need add redistribute specially
explain verbose select * from t1,t2 where t1.b=t2.a for update of t2;
--no need add redistribute for replication table
explain verbose select * from rep,t1 for update of rep;

drop view view_t1;
drop view view_vt;
drop view view_ft;
drop table t1;
drop table t2;
drop table t3;
drop table rep;
drop table vt;
drop foreign table ft;

--test lock partition
create table lock_partition_001(val int, val2 int)
partition by range(val)
(
    partition p1 values less than (2451179) ,
    partition p2 values less than (2451544) ,
    partition p3 values less than (2451910) ,
    partition p4 values less than (2452275) ,
    partition p5 values less than (2452640) ,
    partition p6 values less than (2453005) ,
    partition p7 values less than (maxvalue)
);
create table lock_partition_002(val int, val2 int)
partition by range(val)
(
    partition p1 values less than (2451179) ,
    partition p2 values less than (2451544) ,
    partition p3 values less than (2451910) ,
    partition p4 values less than (2452275) ,
    partition p5 values less than (2452640) ,
    partition p6 values less than (2453005) ,
    partition p7 values less than (maxvalue)
);
create table lock_partition_003(val int, val2 int)
partition by range(val)
(
    partition p1 values less than (2451179) ,
    partition p2 values less than (2451544) ,
    partition p3 values less than (2451910) ,
    partition p4 values less than (2452275) ,
    partition p5 values less than (2452640) ,
    partition p6 values less than (2453005) ,
    partition p7 values less than (maxvalue)
);
insert into lock_partition_001 values(1,1);
insert into lock_partition_002 values(2,2);
insert into lock_partition_003 values(3,3);
insert into lock_partition_003 values(3,11);
insert into lock_partition_003 values(3,12);
insert into lock_partition_003 values(3,13);
select * from lock_partition_001, lock_partition_002, lock_partition_003 order by lock_partition_003.val2 for update of lock_partition_001;
explain (costs off, verbose on) select * from lock_partition_001, lock_partition_002, lock_partition_003 order by lock_partition_003.val2 for update of lock_partition_001;
drop table lock_partition_001;
drop table lock_partition_002;
drop table lock_partition_003;

create table customer
(
    c_customer_sk             integer               not null,
    c_customer_id             char(16)              not null,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)                      
) distribute by hash(c_birth_day,c_birth_month); 

create table customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             char(16)              not null,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)                     
) distribute by hash (ca_address_sk);
create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
) distribute by hash (hd_income_band_sk);
--failed for limit
select c_current_addr_sk, c_current_hdemo_sk, c_first_shipto_date_sk
from customer a where exists
(select ca_address_sk from customer_address b where a.c_current_addr_sk = b.ca_address_sk + b.ca_address_sk)
or exists
(select hd_demo_sk from household_demographics c where a.c_current_hdemo_sk = c.hd_demo_sk * hd_dep_count)
order by 1, 2, 3 limit 200 for update;
--success for targetlist not contain lock table's distribute key
select c_current_addr_sk, c_current_hdemo_sk, c_first_shipto_date_sk
from customer a where exists
(select ca_address_sk from customer_address b where a.c_current_addr_sk = b.ca_address_sk + b.ca_address_sk)
or exists
(select hd_demo_sk from household_demographics c where a.c_current_hdemo_sk = c.hd_demo_sk * hd_dep_count)
order by 1, 2, 3 for update;
--success for targetlist contain lock table's distribute key
select c_birth_day, c_birth_month, c_current_addr_sk, c_current_hdemo_sk, c_first_shipto_date_sk
from customer a where exists
(select ca_address_sk from customer_address b where a.c_current_addr_sk = b.ca_address_sk + b.ca_address_sk)
or exists
(select hd_demo_sk from household_demographics c where a.c_current_hdemo_sk = c.hd_demo_sk * hd_dep_count)
order by 1, 2, 3 for update;
drop table customer;
drop table customer_address;
