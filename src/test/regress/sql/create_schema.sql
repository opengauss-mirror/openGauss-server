create role samedb_schema_cn_role_02_001 password 'Ttest_234';

create schema authorization samedb_schema_cn_role_02_001
create table samedb_schema_cn_role_02_001.cn_table_00 (cn_a int, cn_b text , cn_c date ,cn_d interval, cn_e serial)
create view cn_view_00 as
select cn_b,cn_d, cn_c from cn_table_00;

\d+ samedb_schema_cn_role_02_001.cn_table_00

alter schema samedb_schema_cn_role_02_001 rename to  samedb_schema_cn_role_02_001_bak;

\d+ samedb_schema_cn_role_02_001_bak.cn_table_00

alter view samedb_schema_cn_role_02_001_bak.cn_view_00 rename to cn_view_00_bak;

drop view  samedb_schema_cn_role_02_001_bak.cn_view_00_bak;

drop table samedb_schema_cn_role_02_001_bak.cn_table_00;

drop schema samedb_schema_cn_role_02_001_bak cascade;

create schema test_ns_schema_1
create unique index abc_a_index on abc(a)
create view abc_view1 as
	select a + 1 as a, b + 1 as b from abc
create table abc(
	a serial,
	b int
);

--illegal schema name, start with 'pg_'
create schema pg_error_schema;
--create user will create a schema which name is same as username, so it is also illegal.
create user pg_error_username password 'test-1234';
drop role samedb_schema_cn_role_02_001;

drop schema test_ns_schema_1 cascade;

CREATE ROLE test_mul_role IDENTIFIED BY 'Aa123456';
create schema if not exists test_mul_schema AUTHORIZATION test_mul_role;
create schema if not exists test_mul_schema AUTHORIZATION test_mul_role;
create schema if not exists test_mul_schema AUTHORIZATION test_mul_role;
drop schema test_mul_schema cascade;
drop role test_mul_role;
