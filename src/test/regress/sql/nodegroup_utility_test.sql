/*---------------------------------------------------------------------------------------
 *
 *  Nodegroup Utility test case
 *
 * Portions Copyright (c) 2016, Huawei
 *
 *
 * IDENTIFICATION
 *    src/test/regress/sql/nodegroup_utility_test.sql
 *---------------------------------------------------------------------------------------
 */
create schema nodegroup_utility_test;
set current_schema = nodegroup_utility_test;

create node group ngroup1234 with (datanode1,datanode2,datanode3,datanode4);
create node group ngroup5678 with (datanode5,datanode6,datanode7,datanode8);
create table t1ng1234(c1 int, c2 int) distribute by hash (c2) to group ngroup1234;
create table t1ng5678(c1 int, c2 int) distribute by hash (c2) to group ngroup5678;

insert into t1ng1234 select v,v from generate_series(1,100) as v;
insert into t1ng5678 select * from t1ng1234;

create index ix1 on t1ng1234(c1);
create index ix2 on t1ng5678(c2);

-- test ALTER
alter table t1ng1234 alter c1 set not null;

-- test CLUSTER
cluster t1ng1234 using ix1;
cluster t1ng5678 using ix2;

-- test ANALYZE
analyze t1ng1234;
analyze t1ng5678;

-- test vacuum
vacuum t1ng1234;

-- test vacuum full
vacuum full t1ng1234;

-- test reindex;
reindex table t1ng1234;

-- test GRANT
grant all on t1ng1234 to public;

-- test REVOKE
revoke all on t1ng1234 from public;

-- test LOCK table
start transaction;
lock table t1ng1234 in access share mode;
end transaction;

-- test truncate
-- should fail as do not allow truncate multiple tables from different node group
truncate t1ng1234,t1ng5678;

truncate t1ng1234;

truncate t1ng5678;

-- conner case that we found
-- #1 grant tables under schema
GRANT ALL ON ALL TABLES IN SCHEMA public TO public;

-- #2. vacuum catalog table
set xc_maintenance_mode = on;
vacuum full pg_class;
vacuum pg_class;
vacuum;
vacuum full;
set xc_maintenance_mode = off;

-- will fail as we don't allow drop tables from different node group
drop table t1ng1234, t1ng5678;

drop table t1ng1234;
drop table t1ng5678;
reset expected_computing_nodegroup;
drop node group ngroup1234;
drop node group ngroup5678;

SET current_schema=public;


drop schema nodegroup_utility_test cascade;

/*
 * utility misc
 * 		alter table rename, alter table set schema
 */

create schema schema_test;
set current_schema = schema_test;

create node group test_ng1 with(datanode1);
create table test_t1(x int) to group test_ng1;

-- set schema
alter table test_t1 set schema public;
set current_schema = public;

-- rename
alter table test_t1 rename to test_t2;
drop table test_t2;
drop node group test_ng1;
drop schema schema_test;

