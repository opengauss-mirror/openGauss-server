-- test insert
drop table if exists t1;
create table t1 (c1 int) with (storage_type=USTORE);
start transaction;
insert into t1(c1) values(1);
insert into t1(c1) values(2);
COMMIT;
select * from t1;

-- Data type variants
drop table if exists t2;
create table t2 (c1 int, c2 char(10), c3 decimal, c4 text, c5 varchar(10)) with (storage_type=USTORE);
start transaction;
insert into t2 (c1, c2, c3, c4, c5) values (1, 'bbb', 1.1, 'ddd', 'eee');
insert into t2 (c1, c2 ,c3 ,c4, c5) values (1, 'bbb2', 2.2, 'ddd2', 'eee');
commit;
select * from t2;

-- Null and Non-null columns with DEFAULT and explicit values
drop table if exists t3;
create table t3 (c1 int, col2 int NOT NULL, c3 text default 'abcd', c4 int4, c5 int not NULL) with (storage_type=USTORE);
insert into t3 values(DEFAULT, DEFAULT, DEFAULT, DEFAULT, 200);
start transaction;
insert into t3 values(DEFAULT, 100, DEFAULT, DEFAULT, 200);
insert into t3 values(DEFAULT, 100, DEFAULT, 150, 200);
insert into t3 values(DEFAULT, 100, 'dddddddd', 150, 200);
insert into t3 values(20, 100, 'dddddddd', 150, 200);
commit;
select * from t3;
select * from t3 where c1 > 10;

-- tuple with NULL attribute
BEGIN;
DROP TABLE IF EXISTS bmsql_order_line;
create table bmsql_order_line (
ol_w_id         integer   not null,
ol_d_id         integer   not null,
ol_o_id         integer   not null,
ol_number       integer   not null,
ol_i_id         integer   not null,
ol_delivery_d   timestamp,
ol_amount       decimal(6,2),
ol_supply_w_id  integer,
ol_quantity     integer,
ol_dist_info    char(24)
);

INSERT INTO bmsql_order_line ( ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id,   ol_supply_w_id, ol_delivery_d, ol_quantity,   ol_amount, ol_dist_info) VALUES ('1', '1', '2101', '1', '64393', '1', NULL, '5', '9270.85', 'sLogGwivBEUKPpbgjkaCffzQ');
COMMIT;

select count(*) from bmsql_order_line;                   -- should be 1
select count(*) from bmsql_order_line where ol_w_id = 1; -- should be 1
DROP TABLE bmsql_order_line;

drop table t1;
drop table t2;
drop table t3;
