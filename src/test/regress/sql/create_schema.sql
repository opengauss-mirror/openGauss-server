create role samedb_schema_cn_role_02_001 password 'Ttest_234';

create schema authorization samedb_schema_cn_role_02_001
create table samedb_schema_cn_role_02_001.cn_table_00 (cn_a int, cn_b text , cn_c date ,cn_d interval)
create view cn_view_00 as
select cn_b,cn_d, cn_c from cn_table_00;

alter schema samedb_schema_cn_role_02_001 rename to  samedb_schema_cn_role_02_001_bak;

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

drop role samedb_schema_cn_role_02_001;

drop schema test_ns_schema_1 cascade;
