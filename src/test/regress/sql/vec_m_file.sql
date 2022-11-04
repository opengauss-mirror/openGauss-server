set work_mem='4MB';
drop table if exists bmsql_oorder;
drop table if exists bmsql_order_line;
drop table if exists bmsql_district;
drop table if exists bmsql_customer;
drop table if exists bmsql_item;

CREATE TABLE bmsql_order_line (
ol_w_id int NOT NULL,
ol_d_id int NOT NULL,
ol_o_id int NOT NULL,
ol_number int NOT NULL,
ol_i_id int NOT NULL,
ol_delivery_d timestamp(6),
ol_amount numeric(6,2),
ol_supply_w_id int,ol_quantity int,
ol_dist_info char(24)
);

insert into bmsql_order_line values('1','1','1','1','1',to_timestamp('2010-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.01','1','1','sqltest_bpchar_1') ;
insert into bmsql_order_line values('2','2','2','2','2',to_timestamp('2010-01-02 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.02','2','2','sqltest_bpchar_2') ;
insert into bmsql_order_line values('3','3','3','3','3',to_timestamp('2010-01-03 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.03','3','3','sqltest_bpchar_3') ;
insert into bmsql_order_line values('4','4','4','4','4',to_timestamp('2010-01-04 00:00:00','yyyy-mm-dd hh24:mi:ss'),'0.04','4','4','sqltest_bpchar_4') ;
insert into bmsql_order_line(ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id) values('5','5','5','5','5');

CREATE TABLE bmsql_district (
d_w_id int NOT NULL,d_id int NOT NULL,d_ytd numeric( 12,2),d_tax numeric(4,4),d_next_o_id int,d_name varchar( 10),d_street_1 varchar( 20) ,d_street_2 varchar( 20) ,d_city varchar( 20),
d_state char(2),d_zip char(9)
);
insert into bmsql_district values('1','1','0.01','0.0001','1','sqltest_va','sqltest_varchar_1','sqltest_varchar_1','sqltest_varchar_1','sq','sqltest_b') ;
insert into bmsql_district values('2','2','0.02','0.0002','2','sqltest_va','sqltest_varchar_2','sqltest_varchar_2','sqltest_varchar_2','sq','sqltest_b') ;
insert into bmsql_district values('3','3','0.03','0.0003','3','sqltest_va','sqltest_varchar_3','sqltest_varchar_3','sqltest_varchar_3','sq','sqltest_b') ;
insert into bmsql_district values('4','4','0.04','0.0004','4','sqltest_va','sqltest_varchar_4','sqltest_varchar_4','sqltest_varchar_4','sq','sqltest_b') ;
insert into bmsql_district(d_w_id,d_id) values('5','5');

CREATE TABLE bmsql_oorder (
o_w_id int NOT NULL,
o_d_id int NOT NULL,o_id int NOT NULL,o_c_id int,
o_carrier_id int,o_ol_cnt int,o_ali_local int,
o_entry_d timestamp(6)
);

insert into bmsql_oorder values('1','1','1','1','1','1','1',to_timestamp('2010-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss')) ;
insert into bmsql_oorder values('2','2','2','2','2','2','2',to_timestamp('2010-01-02 00:00:00','yyyy-mm-dd hh24:mi:ss')) ;
insert into bmsql_oorder values('3','3','3','3','3','3','3',to_timestamp('2010-01-03 00:00:00','yyyy-mm-dd hh24:mi:ss')) ;
insert into bmsql_oorder values('4','4','4','4','4','4','4',to_timestamp('2010-01-04 00:00:00','yyyy-mm-dd hh24:mi:ss')) ;
insert into bmsql_oorder(o_w_id,o_d_id,o_id) values('5','5','5') ;

CREATE TABLE bmsql_customer (
c_w_id int NOT NULL,
c_d_id int NOT NULL,
c_id int NOT NULL,
c_discount numeric(4,4),
c_credit char(2),
c_last varchar(16),
c_first varchar( 16),
c_credit_lim numeric( 12,2),
c_balance numeric(12,2),
c_ytd_payment numeric(12,2),
c_payment_cnt int,
c_delivery_cnt int,
c_street_1 varchar( 20),
c_street_2 varchar( 20),
c_city varchar(20),
c_state char(2),
c_zip char(9),
c_phone char(16),
c_since timestamp(6),
lc_middle char(2),
c_data varchar(500)
);
CREATE INDEX bmsql_customer_idx1 ON bmsql_customer (c_w_id,c_d_id,c_last,c_first);

insert into bmsql_customer values('1','1','1','0.0001','sq','sqltest_varchar_','sqltest_varchar_','0.01','0.01','0.01','1','1','sqltest_varchar_1','sqltest_varchar_1','sqltest_varchar_1','sq','sqltest_b','sqltest_bpchar_1',to_timestamp('2010-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'),'sq','sqltest_varchar_1');
insert into bmsql_customer values('2','2','2','0.0002','sq','sqltest_varchar_','sqltest_varchar_','0.02','0.02','0.02','2','2','sqltest_varchar_2','sqltest_varchar_2','sqltest_varchar_2','sq','sqltest_b','sqltest_bpchar_2',to_timestamp('2010-01-02 00:00:00','yyyy-mm-dd hh24:mi:ss'),'sq','sqltest_varchar_2');
insert into bmsql_customer values('3','3','3','0.0003','sq','sqltest_varchar_','sqltest_varchar_','0.03','0.03','0.03','3','3','sqltest_varchar_3','sqltest_varchar_3','sqltest_varchar_3','sq','sqltest_b','sqltest_bpchar_3',to_timestamp('2010-01-03 00:00:00','yyyy-mm-dd hh24:mi:ss'),'sq','sqltest_varchar_3');
insert into bmsql_customer values('4','4','4','0.0004','sq','sqltest_varchar_','sqltest_varchar_','0.04','0.04','0.04','4','4','sqltest_varchar_4','sqltest_varchar_4','sqltest_varchar_4','sq','sqltest_b','sqltest_bpchar_4',to_timestamp('2010-01-04 00:00:00','yyyy-mm-dd hh24:mi:ss'),'sq','sqltest_varchar_4');
insert into bmsql_customer(c_w_id,c_d_id,c_id) values('5','5','5');

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

set try_vector_engine_strategy=force;

select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

explain(costs off, verbose) select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

set try_vector_engine_strategy=off;

select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

explain(costs off, verbose) select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

set try_vector_engine_strategy=optimal;

select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

explain(costs off, verbose) select count(*)
from bmsql_oorder
where bmsql_oorder.o_d_id >( select c_w_id from bmsql_customer where bmsql_oorder.o_w_id >=3
union
select 1
from (select c_w_id ,c_balance ,c_last
from bmsql_customer
join bmsql_district
on c_w_id <> d_id
union
select * from
(select count(*) as count,i_price,i_name
from bmsql_item
group by 2,3)) as tb1 ,
(with tmp as (select count(*) , '123' as c5
from (with tmp1 as (select distinct '2010-01-03' as c4 ,ol_d_id as c3 from bmsql_order_line) select * from tmp1 ) where c3>=3 group by c4,c3 )select * from tmp ) tb2
group by rollup (tb1.c_balance,tb2.c5,tb1.c_last),tb1.c_balance
except
select distinct c_w_id
from bmsql_customer
where log(bmsql_oorder.o_carrier_id +5.5,bmsql_oorder.o_carrier_id+6) != bmsql_oorder.o_entry_d
or bmsql_oorder.o_ol_cnt<=3
order by 1 limit 1);

drop table if exists bmsql_oorder;
drop table if exists bmsql_order_line;
drop table if exists bmsql_district;
drop table if exists bmsql_customer;
drop table if exists bmsql_item;