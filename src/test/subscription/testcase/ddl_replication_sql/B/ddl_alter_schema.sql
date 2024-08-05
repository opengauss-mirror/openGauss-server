create SCHEMA test_sche;

ALTER SCHEMA test_sche WITH BLOCKCHAIN;
ALTER SCHEMA test_sche WITHOUT BLOCKCHAIN;

ALTER SCHEMA test_sche RENAME TO test_sche1;
ALTER SCHEMA test_sche1 OWNER TO regtest_unpriv_user;

ALTER SCHEMA test_sche1 CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

create SCHEMA test_sche2;

create table t1(id int);
ALTER table t1 set SCHEMA test_sche2;

