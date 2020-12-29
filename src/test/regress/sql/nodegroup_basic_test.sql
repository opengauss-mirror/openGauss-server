/*---------------------------------------------------------------------------------------
 *
 * Test basic nodegroup functionality
 *        #1. CREATE/DROP NDOE GROUP with give node lists
 *        #2. CREATE/DROP TABLE TO NDOE GROUP
 *        #3. SIMPLE QUERY ON table which is one *created* NodeGroup
 *        #4. Basic nagtive tests that we don't support for now(will remove)
 *        #5. There is no installation group
 *
 * Portions Copyright (c) 2016, Huawei
 *
 *
 * IDENTIFICATION
 *	  src/test/regress/sql/nodegroup_basic_test.sql
 *---------------------------------------------------------------------------------------
 */

/*
 * Checkpoint 1
 * Verify the initial node/nodegroup information
 */
create schema nodegroup_basic_test;
set current_schema = nodegroup_basic_test;

set expected_computing_nodegroup='group1';
select node_name, node_type from pgxc_node order by 1,2;

-- Before any operation, we need verify whether the data content in pgxc_group is correct
-- we should see all node group information in pgxc_group in each coordinator nodes
execute direct on (coordinator1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

-- we shouldn't see any node group information in pgxc_group in each data nodes
execute direct on (datanode1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode3) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode4) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode5) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode6) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode7) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode8) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode9) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode10) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode11) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode12) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

-- Test nodegroup create
create node group ngroup1 with (datanode1, datanode3, datanode5, datanode7);
create node group ngroup2 with (datanode2, datanode4, datanode6, datanode8, datanode10, datanode12);
create node group ngroup3 with (datanode1, datanode2, datanode3, datanode4, datanode5, datanode6);

/*
 * Checkpoint 2
 *
 * After creating node group, reverify nodegroup information in pgxc_group
 * ngroup1: datanode1 datanode3 datanode5 datanode7
 * ngroup2: datanode2 datanode4 datanode6 datanode8 datanode10 datanode12
 * ngroup3: datanode1 datanode2 datanode3 datanode4 datanode5 datanode6
 *
 * Note: 'group1' is the default gaussdb nodegroup(for whole cluster) created at initdb time
 *       verify nodegroup's catalog information is correctly populated
 */
-- we should see all node group information in pgxc_group in each coordinator nodes
execute direct on (coordinator1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

-- we shouldn't see any node group information in pgxc_group in each data nodes
execute direct on (datanode1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode3) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode4) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode5) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode6) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode7) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode8) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode9) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode10) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode11) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode12) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

-- T1 in default group
create table t1(c1 int, c2 int) distribute by hash(c1);
insert into t1 select v,v from generate_series(1,30) as v;

create table t1ng1(c1 int, c2 int) distribute by hash(c1) to group ngroup1;
create table t1ng2(c1 int, c2 int) distribute by hash(c1) to group ngroup2;
create table t1ng3(c1 int, c2 int) distribute by hash(c1) to group ngroup3;

insert into t1ng1 select v,v from generate_series(1,30) as v;
insert into t1ng2 select v,v from generate_series(1,30) as v;
insert into t1ng3 select v,v from generate_series(1,30) as v;

select * from t1 order by 1;
select * from t1ng1 order by 1;
select * from t1ng2 order by 1;
select * from t1ng3 order by 1;

-- test if index can be create/dropped
create index t1ix on t1(c2);
create index t1ng1ix on t1ng1(c2);
create index t1ng2ix on t1ng2(c2);
create index t1ng3ix on t1ng3(c2);

create view t1v as select * from t1;
create view t1ng1v as select * from t1ng1;
create view t1ng2v as select * from t1ng2;
create view t1ng3v as select * from t1ng3;

-- verify access via index
set enable_seqscan=false;
show enable_seqscan;
select c2 from t1 order by 1;
select c2 from t1ng1 order by 1;
select c2 from t1ng2 order by 1;
select c2 from t1ng3 order by 1;
set enable_seqscan=true;
show enable_seqscan;

-- verify access via view
select * from t1v order by 1;
select * from t1ng1v order by 1;
select * from t1ng2v order by 1;
select * from t1ng3v order by 1;

-- simple update/delete tests
delete from t1 where c1 < 15;
delete from t1ng1 where c1 < 15;
delete from t1ng2 where c1 < 15;
delete from t1ng3 where c1 < 15;

update t1 set c2 = 10*c2;
update t1ng1 set c2 = 10*c2;
update t1ng2 set c2 = 10*c2;
update t1ng3 set c2 = 10*c2;

-- verify update/delete results via base table (results should be identical)
select * from t1 order by 1;
select * from t1ng1 order by 1;
select * from t1ng2 order by 1;
select * from t1ng3 order by 1;

-- verify update/delete results via view (results should be identical)
select * from t1v order by 1;
select * from t1ng1v order by 1;
select * from t1ng2v order by 1;
select * from t1ng3v order by 1;

-- verify delete with shuffle
create table dt1 (a int, b int, c int) distribute by hash (a) to group ngroup3;
create table dt2 (a int, b int) distribute by hash(a) to group ngroup3;
insert into dt1 values (1,1,1);
insert into dt1 values (2,2,2);
insert into dt2 values (1,1);
insert into dt2 values (3,3);
delete from dt1 using dt2 where dt1.b = dt2.a and dt1.c = dt2.b;
select * from dt1 order by a, b, c;
select * from dt2 order by a, b;
drop table dt1;
drop table dt2;

-- verify aggregation happens on correct nodes
explain (costs off) select count(*),c1 from t1 group by c1;
explain (costs off) select count(*),c2 from t1 group by c2;

explain (costs off) select count(*),c1 from t1ng1 group by c1;
explain (costs off) select count(*),c2 from t1ng1 group by c2;

explain (costs off) select count(*),c1 from t1ng2 group by c1;
explain (costs off) select count(*),c2 from t1ng2 group by c2;

explain (costs off) select count(*),c1 from t1ng3 group by c1;
explain (costs off) select count(*),c2 from t1ng3 group by c2;

/*
 * Checkpint 3
 * check point to see if index,tables created and they are correctl populated into pgxc_class
 */
execute direct on (coordinator1) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (coordinator2) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (coordinator3) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode1) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode2) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode3) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode4) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode5) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode5) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode6) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode7) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode8) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode9) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode10) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode11) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';

execute direct on (datanode12) '
    select (select pgxc_node_str()) as nodename, nspname, relname, relkind, pclocatortype, pchashalgorithm, pchashbuckets, pgroup, redistributed, redis_order, pcattnum
    from pg_class left join pgxc_class on (pg_class.oid = pgxc_class.pcrelid)
                       join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
    where pg_class.relname like ''t1%'' order by 1,2,3,4;
';


-- verify index drop
drop index t1ix;
drop index t1ng1ix;
-- t1ng2ix and t1ng3ix are dropped along with t1ng2 and t1ng3's table drop

-- simple multi-nodegroup join case
select * from t1 join t1ng1 on t1.c1 = t1ng1.c1 order by 1;
select * from t1 join t1ng2 on t1.c1 = t1ng2.c1 order by 1;

-- should fail as we do not support drop multiple tables from different nodegroup
drop table t1ng1,t1ng2,t1ng3;

-- should fail as we do not support drop node group where exists tables
drop node group ngroup1;
drop node group ngroup2;
drop node group ngroup3;

-- should fail as default_computing node group is pointing to existing node group
set expected_computing_nodegroup=ngroup1;
reset expected_computing_nodegroup;

-- drop views
drop view t1ng1v;
drop view t1ng2v;
drop view t1ng3v;

-- drop tables
drop table t1ng1;
drop table t1ng2;
drop table t1ng3;

-- should be OK as we already have all underlying tables dropped
drop node group ngroup1;
drop node group ngroup2;
drop node group ngroup3;

/*
 * Checkpoint 4
 * After drop all testing objects, we should see nothing in pgxc_group(besides t1 as it is created under default nodegroup)
 */
-- check coordinator nodes
execute direct on (coordinator1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (coordinator2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

-- check data nodes
execute direct on (datanode1) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode2) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode3) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode4) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode5) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode6) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode7) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode8) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode9) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode10) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode11) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';
execute direct on (datanode12) 'select group_name, in_redistribution, group_members from pgxc_group order by 1,2';

drop view t1v;
drop table t1;

drop schema nodegroup_basic_test cascade;



/*
 * Checkpoint 5
 * There is no installation group
 */

set current_schema = public;
set xc_maintenance_mode=on;
start transaction;

execute direct on (coordinator1) 'update pgxc_group set is_installation=false where group_name=''group1''';
execute direct on (coordinator2) 'update pgxc_group set is_installation=false where group_name=''group1''';
execute direct on (coordinator3) 'update pgxc_group set is_installation=false where group_name=''group1''';

execute direct on (coordinator1) 'select group_name, is_installation from pgxc_group';
execute direct on (coordinator2) 'select group_name, is_installation from pgxc_group';
execute direct on (coordinator3) 'select group_name, is_installation from pgxc_group';

commit;
reset xc_maintenance_mode;


-- should succeed as current MPPDB is none-multi nodegroup system, we identify
-- first node group as installation old group
create table t1(c1 int, c2 int);
drop table t1;

-- should fail in multi node group environment
create node group group2 with (datanode1);
select group_name, is_installation from pgxc_group order by 1;
create table t2(c1 int, c2 int);

drop node group group2;

set xc_maintenance_mode=on;
start transaction;

execute direct on (coordinator1) 'update pgxc_group set is_installation=true where group_name=''group1''';
execute direct on (coordinator2) 'update pgxc_group set is_installation=true where group_name=''group1''';
execute direct on (coordinator3) 'update pgxc_group set is_installation=true where group_name=''group1''';

execute direct on (coordinator1) 'select group_name, is_installation from pgxc_group';
execute direct on (coordinator2) 'select group_name, is_installation from pgxc_group';
execute direct on (coordinator3) 'select group_name, is_installation from pgxc_group';

commit;
reset xc_maintenance_mode;
