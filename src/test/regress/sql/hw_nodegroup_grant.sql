create node group group_test_t12 with (datanode1, datanode2);
create node group group_test_t23 with (datanode2, datanode3);

create node group group_test_t123456 with (datanode1, datanode2, datanode3,datanode4, datanode5,datanode6);
create node group group_test_t234567 with (datanode7, datanode2, datanode3,datanode4, datanode5,datanode6);

--create user

create user test_group_t1 password 'huawei@123';
create user test_group_t2 password 'huawei@123';

--error test
grant create on node group group1  to test_group_t1;
grant create on node group group_test_t122 to test_group_t2;
set role test_group_t1 password 'huawei@123';
set default_storage_nodegroup = group_test_t12;
create table tbl_1(a int);
grant create on node group group_test_t12 to test_group_t2;
reset role;

--add llt
select aclitemin('test_group_t1=p/test_group_t1');
select acldefault('G', u1.usesysid) from (select usesysid from pg_user where usename = 'test_group_t1') u1;
grant create on node group group_test_t12 to test_group_t1 with grant option;

--case1: test grant create to test_group_t1 on test_group_t12
grant create on node group group_test_t12 to test_group_t1;

set role test_group_t1 password 'huawei@123';
create table tbl_2(a int, b int) to group group_test_t12;
create table tbl_1(a int) to group group_test_t23;

--test select/insert/update/delete privilige
insert into tbl_2 values(1, 1),(2, 2);
select count(*) from tbl_2;
update tbl_2 set b = 10 where a = 2;
delete from tbl_2;

--test has_nodegroup_privilege function
select * from has_nodegroup_privilege('test_group_t1', 'group_test_t12', 'create');
select * from has_nodegroup_privilege('test_group_t1', 'group_test_t23', 'create');
select * from has_nodegroup_privilege('group_test_t12', 'create');
select has_nodegroup_privilege(t1.oid, 'usage') from (select oid from pgxc_group where group_name = 'group_test_t12') as t1;

reset role;

--test user2 use user1 table
set role test_group_t1 password 'huawei@123';
grant usage on schema test_group_t1 to test_group_t2;
set role test_group_t2 password 'huawei@123';
select * from test_group_t1.tbl_2;
reset role;

--test_group_t2 has usage on group_test_t12, but can not create table
grant usage on node group group_test_t12 to test_group_t2;
set role test_group_t2 password 'huawei@123';
select * from test_group_t1.tbl_2;
set role test_group_t1 password 'huawei@123';
grant select on table tbl_2 to  test_group_t2;
set role test_group_t2 password 'huawei@123';
select * from test_group_t1.tbl_2;
create table tbl_4(a int, b int) to group group_test_t12;

reset role;


--test insert into select

grant create on node group group_test_t123456 to test_group_t1;
grant create on node group group_test_t23 to test_group_t1;
set role test_group_t1 password 'huawei@123'; 
drop table tbl_2;
create table tbl_3(a int) to group group_test_t23;
create table tbl_2(a int) to group group_test_t123456;

explain (verbose on, costs off) insert into  tbl_2 select * from tbl_3;

--test usage priviliges
show expected_computing_nodegroup;
set expected_computing_nodegroup = group_test_t234567;
show expected_computing_nodegroup;

CREATE TABLE aa(a INT) DISTRIBUTE BY HASH(a);
--test view and subquery
create view bb as select * from aa;
select * from bb;
create view cc as select count(*) from pg_proc;
select count(*) from cc; 
create view dd as select proname ,a from pg_proc, aa;
select * from dd;

--test function

CREATE OR REPLACE FUNCTION fuc02( ) RETURN int
AS 
  h int;
BEGIN
   select a into h from aa;
END;
/
select * from fuc02();

CREATE OR REPLACE FUNCTION fuc03( ) RETURN int
AS 
  h int;
BEGIN
   select proname into h from pg_proc;
END;
/
select * from fuc03();

create table tbl_4(a int, b int) to group group_test_t234567;
reset role;

grant compute on node group group_test_t234567 to test_group_t1;
revoke all on node group group_test_t123456 from test_group_t1;
set role test_group_t1 password 'huawei@123'; 
set expected_computing_nodegroup = group_test_t234567;
create table tbl_5(a int, b int) to group group_test_t234567;

explain  (verbose on, costs off)select count(*) from tbl_2 join tbl_3 on tbl_2.a=tbl_3.a;

select * from has_nodegroup_privilege('test_group_t1', 'group_test_t234567', 'create');
select * from has_nodegroup_privilege('test_group_t1', 'group_test_t234567', 'usage');
select * from has_nodegroup_privilege('group_test_t234567', 'compute');
select has_nodegroup_privilege('test_group_t1', t1.oid, 'compute') from (select oid from pgxc_group where group_name = 'group_test_t234567') as t1;
select has_nodegroup_privilege(t1.oid, 'compute with grant option') from (select oid from pgxc_group where group_name = 'group_test_t234567') as t1;

select has_nodegroup_privilege(u1.oid, 'group_test_t234567', 'usage with grant option') from (select oid from pg_roles where rolname = 'test_group_t1') as u1;

select has_nodegroup_privilege(u1.oid, t1.oid , 'create with grant option') from (select oid from pg_roles where rolname = 'test_group_t1') as u1,
(select oid from pgxc_group where group_name = 'group_test_t234567') as t1;

--null test
select * from has_nodegroup_privilege('test_group_t1', 123456, 'usage');
select has_nodegroup_privilege(123456, 'usage') is null;
select * from has_nodegroup_privilege(123456, 12345, 'usage');
reset role;

--test superuser
select * from has_nodegroup_privilege('group_test_t12', 'create');
select * from has_nodegroup_privilege(current_user,'group_test_t12', 'create');
select * from has_nodegroup_privilege('group_test_t12', 'create with grant option');

select * from has_nodegroup_privilege(NULL, 'create');
select * from has_nodegroup_privilege('group_test_t12', 'create');
select * from has_nodegroup_privilege(NULL, 12345,'create');
select * from has_nodegroup_privilege(12345, NULL);
select * from has_nodegroup_privilege(12, NULL, 'create');
select * from has_nodegroup_privilege(12345,2345, NULL);

drop user test_group_t1 cascade;
drop user test_group_t2 cascade;

drop node group group_test_t12;
drop node group group_test_t23;
drop node group group_test_t123456;
drop node group group_test_t234567;

