create schema sonic_hashjoin_test_othertype;
set current_schema = sonic_hashjoin_test_othertype;

set enable_codegen to off;
set enable_nestloop to off;
set enable_mergejoin to off;
set enable_hashjoin to on;
set enable_sonic_hashjoin to on;
set analysis_options='on(HASH_CONFLICT)';

CREATE TABLE sonic_hashjoin_test_othertype.ROW_HASHJOIN_TABLE_01_NET(
 C_INT INT,
 C_BIGINT BIGINT,
 C_SMALLINT SMALLINT,
 C_MONEY MONEY,
 C_INET inet,
 c_CIDR cidr
);

CREATE TABLE sonic_hashjoin_test_othertype.VEC_HASHJOIN_TABLE_01_NET(
 C_INT INT,
 C_BIGINT BIGINT,
 C_SMALLINT SMALLINT,
 C_MONEY MONEY,
 C_INET inet,
 c_CIDR cidr
)with(orientation = column) distribute by replication
partition by range (C_INT)
(
partition location_1 values less than (0),
partition location_2 values less than (1),
partition location_3 values less than (2),
partition location_4 values less than (3),
partition location_5 values less than (4),
partition location_6 values less than (5),
partition location_7 values less than (maxvalue)
);

-- macaddr is not supported in column store
CREATE TABLE sonic_hashjoin_test_othertype.VEC_HASHJOIN_TABLE_02_NET(
 C_INT INT,
 C_BIGINT BIGINT,
 C_SMALLINT SMALLINT,
 C_MAC MACADDR
)with(orientation = column) distribute by hash(c_int);

insert into ROW_HASHJOIN_TABLE_01_NET values(1, 100000, 123,   1111, '198.24.10.0/24', '192.168.100.128/25');
insert into ROW_HASHJOIN_TABLE_01_NET values(2, 200000, 456,   '12.34'::float8::numeric::money, '198.24.10.0', '192.168/24');
insert into ROW_HASHJOIN_TABLE_01_NET values(3, 300000, 789,   12.345, '198.24.10.0', '10');
insert into ROW_HASHJOIN_TABLE_01_NET values(4, 400000, 98,    '1234567'::bigint::money, '198.10/8', '128.1.2');
insert into ROW_HASHJOIN_TABLE_01_NET values(5, 500000, 12345, '987.654'::money, '198.10.20/16', '::ffff:1.2.3.0/128');
insert into ROW_HASHJOIN_TABLE_01_NET values(6, 600000, 6785,  NULL, '198.24.10.0/24', '2001:4f8:3:ba:2e0:81ff:fe22:d1f1/128');

insert into VEC_HASHJOIN_TABLE_01_NET select * from ROW_HASHJOIN_TABLE_01_NET;
analyze VEC_HASHJOIN_TABLE_01_NET;

select * from VEC_HASHJOIN_TABLE_01_NET t1 join VEC_HASHJOIN_TABLE_01_NET t2 on t1.c_int = t2.c_int order by 1;
select * from VEC_HASHJOIN_TABLE_01_NET t1 join VEC_HASHJOIN_TABLE_01_NET t2 on t1.c_int = t2.c_int and t1.c_money = t2.c_money and t1.c_inet = t2.c_inet and t1.c_cidr = t2.c_cidr order by 1;
