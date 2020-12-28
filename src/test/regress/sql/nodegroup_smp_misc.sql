/*---------------------------------------------------------------------------------------
 *
 * Test basic nodegroup dml
 *        #0. misc
 *        #1. insert ... select ...
 *        #2. update ... where in ...
 *        #3. delete ... where in ...
 *        #4. to be continue...
 *
 * Portions Copyright (c) 2017, Huawei
 *
 *
 * IDENTIFICATION
 *	  src/test/regress/sql/nodegroup_misc.sql
 *---------------------------------------------------------------------------------------
 */

set query_dop = 1002;
create schema nodegroup_misc;
set current_schema = nodegroup_misc;

set expected_computing_nodegroup='group1';

-- create node group
create node group ng1 with(datanode1, datanode11, datanode2, datanode12);
create node group ng2 with(datanode4, datanode8, datanode5, datanode10);
create node group ng3 with(datanode1,datanode2,datanode3,datanode4,datanode5,datanode6,datanode7,datanode8,datanode9,datanode10,datanode11,datanode12);
create node group ng4 with(datanode7);
select group_name, array_upper(group_members,1)+1 as member_count from pgxc_group order by 1,2;


/*
 * Checkpoint 0
 * misc
 */

-- test ereport create table like
create node group ng5 with(datanode1,datanode2,datanode3,datanode4,datanode5,datanode6,datanode7,datanode8,datanode9);
create table t1(id int, y int) to  group ng5;
create table t2 (like t1) to group ng1;
drop table t2;
create table t2 (like t1);
drop table t2;
create table t2 (like t1) to group ng5;
\d+ t2;

drop table t1, t2;

-- test alter node
create table a(id int) to group ng5;
insert into a select generate_series(1, 40);

alter table a add node(datanode10);
alter table a add node(datanode10, datanode12);
alter table a add node(datanode10, datanode11, datanode12);

-- drop table a
set xc_maintenance_mode = on;
execute direct on (datanode10) 'create table a(id int)';
execute direct on (datanode11) 'create table a(id int)';
execute direct on (datanode12) 'create table a(id int)';
reset xc_maintenance_mode;
drop table a;
\dt;

drop node group ng5;

-- test set default_storage_nodegroup
create node group ng5 with(datanode2, datanode3);
set default_storage_nodegroup='ng5';
create table t5(x int);
\d+ t5;

reset default_storage_nodegroup;
create table t6(x int);
\d+ t6;

drop table t5;
drop table t6;
drop node group ng5;

-- test drop node group
create node group test_group with(datanode2);
create database test_database;

\c test_database
create table test_table(x int) to group test_group;

\c regression
drop node group test_group;

drop database test_database;
drop node group test_group;

create table DISTINCT_034(COL_DP DOUBLE PRECISION)distribute by replication;
insert into DISTINCT_034 values(pi()),(3.14159265358979);
select distinct COL_DP from DISTINCT_034 order by 1;

drop table DISTINCT_034;

create foreign table customer_load
(
    a int,
    b int
)
SERVER gsmpp_server
OPTIONS (
    delimiter '|',
    encoding 'utf8',
    format 'text',
    location 'gsfs://192.168.168.4:21111/customer.dat',
    mode 'Normal'
) to group ng4;

-- test create sequence owned by a table which is not installation group
create table t1(x int) to group ng4;

create sequence s1
    increment by 1
    minvalue 1 maxvalue 30
    start 1
    cache 5
    owned by t1.x;

drop table t1;

-- should fail : test create sequence to group
create sequence s1 to group group1;

create node group ng5 with(datanode1);
create user user1 password 'huawei@123';
grant create on node group ng5 to user1;
drop node group ng5;
drop user user1;

-- test to node : should fail
create table t5 (x int) to node (datanode1);
-- test function
create table x(x int) to group ng1;
create table y(x int);

-- should fail: test create table using table type not in installation group
create table z(x x, y int);

-- should fail: test create plpgsql function using return table type not in installation group
 CREATE FUNCTION fx() returns setof x as '
DECLARE
 rec RECORD;
BEGIN
 FOR rec IN (select * from x where x<=4)   LOOP
  RETURN NEXT rec;
 END LOOP;
 RETURN;
END;' language plpgsql;

-- should fail: test create sql function using return table type not in installation group
 CREATE FUNCTION fx() returns setof x as '
DECLARE
 rec RECORD;
BEGIN
 FOR rec IN (select * from x where x<=4)   LOOP
  RETURN NEXT rec;
 END LOOP;
 RETURN;
END;' language 'sql';

-- should fail: test create plpgsql function using declare table type not in installation group
 CREATE FUNCTION fx() returns setof y as '
DECLARE
 rec x;
BEGIN
 FOR rec IN (select * from x where x<=4)   LOOP
  RETURN NEXT rec;
 END LOOP;
 RETURN;
END;' language plpgsql;

-- should fail: test create plpgsql function using argument table type not in installation group
 CREATE FUNCTION fx(x x) returns setof y as '
DECLARE
 rec RECORD;
BEGIN
 FOR rec IN (select * from x where x<=4)   LOOP
  RETURN NEXT rec;
 END LOOP;
 RETURN;
END;' language plpgsql;

-- should fail: test create sql function using argument table type not in installation group
 CREATE FUNCTION fx(x x) returns setof y as '
DECLARE
 rec RECORD;
BEGIN
 FOR rec IN (select * from x where x<=4)   LOOP
  RETURN NEXT rec;
 END LOOP;
 RETURN;
END;' language 'sql';

-- should fail: test create procedure using argument table type not in installation group
CREATE OR REPLACE PROCEDURE prc_add
(
    param1    IN   INTEGER,
    param2    IN OUT  INTEGER,
    param4    OUT  x
)
AS
BEGIN
   param2:= param1 + param2;
END;
/

drop table x;
drop table y;

create table x(x int) to group ng1;
insert into x select generate_series(1, 100);
create schema scheme_skewness;
create table scheme_skewness.x(x int);
insert into scheme_skewness.x select generate_series(1, 100);
select table_skewness('x');
select table_skewness('scheme_skewness.x');
drop table x;
drop table scheme_skewness.x;

/*
 * Checkpoint 1
 * insert
 */

------------------------------------------------------------
-- test "insert ... select ..." 
-- from two different kinds of table on the same nodegroup
------------------------------------------------------------

-- create table
create table t1(x int, y int) distribute by hash(x) to group ng2;
create table t2(x int, y int) distribute by replication to group ng2;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng2;
create table t4(x int, y int) with(orientation = column, compression=middle) distribute by replication to group ng2;

-- insert 
insert into t1 select v, v*10 from generate_series(1,20) as v;
insert into t2 select v, v*10 from generate_series(1,10) as v;
insert into t3 select v, v*10 from generate_series(11,20) as v;
insert into t4 select v, v*10 from generate_series(6,15) as v;

insert into t1 select x, y from t1;
insert into t1 select x, y from t2;
insert into t1 select x, y from t3;
insert into t1 select x, y from t4;

insert into t2 select x, y from t1;
insert into t2 select x, y from t2;
insert into t2 select x, y from t3;
insert into t2 select x, y from t4;

insert into t3 select x, y from t1;
insert into t3 select x, y from t2;
insert into t3 select x, y from t3;
insert into t3 select x, y from t4;

insert into t4 select x, y from t1;
insert into t4 select x, y from t2;
insert into t4 select x, y from t3;
insert into t4 select x, y from t4;

select count(*) from t1;
select count(*) from t2;
select count(*) from t3;
select count(*) from t4;

-- drop table
drop table t1;
drop table t2;
drop table t3;
drop table t4;

------------------------------------------------------------
-- test "insert ... select ..." 
-- from two different kinds of table on different nodegroup
------------------------------------------------------------

-- create table
create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by replication to group ng2;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng3;
create table t4(x int, y int) with(orientation = column, compression=middle) distribute by replication to group ng4;

-- insert
insert into t1 select v, v*10 from generate_series(1,20) as v;
insert into t2 select v, v*10 from generate_series(1,10) as v;
insert into t3 select v, v*10 from generate_series(11,20) as v;;
insert into t4 select v, v*10 from generate_series(6,15) as v;;

insert into t1 select x, y from t2;
insert into t1 select x, y from t3;
insert into t1 select x, y from t4;

insert into t2 select x, y from t1;
insert into t2 select x, y from t3;
insert into t2 select x, y from t4;

insert into t3 select x, y from t1;
insert into t3 select x, y from t2;
insert into t3 select x, y from t4;

insert into t4 select x, y from t1;
insert into t4 select x, y from t2;
insert into t4 select x, y from t3;

select count(*) from t1;
select count(*) from t2;
select count(*) from t3;
select count(*) from t4;


drop table t1;
drop table t2;
drop table t3;
drop table t4;

------------------------------------------------------------
-- test "insert ... select ..." 
-- from the two same kinds of table on different nodegroup
------------------------------------------------------------

create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
select count(*) from t1;

drop table t1;
drop table t2;


create table t1(x int, y int) distribute by replication to group ng1;
create table t2(x int, y int) distribute by replication to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
select count(*) from t1;

drop table t1;
drop table t2;


create table t1(x int, y int) with(orientation = column) distribute by hash(x) to group ng1;
create table t2(x int, y int) with(orientation = column) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
select count(*) from t1;

drop table t1;
drop table t2;


create table t1(x int, y int) with(orientation = column, compression=middle) to group ng1;
create table t2(x int, y int) with(orientation = column, compression=middle) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
select count(*) from t1;

drop table t1;
drop table t2;



/*
 * Checkpoint 2
 * update
 */

------------------------------------------------------------
-- test "update .. where in ..." 
-- from two different kinds of table on the same nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int primary key, y int) distribute by replication to group ng1;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng1;


insert into t1 select v, v from generate_series(1,40) as v;
insert into t2 select v, v from generate_series(21,40) as v;
insert into t3 select * from t1 where x%2 = 0;

update t1 set y = y*10 where x in(select x from t1);
update t1 set y = y*10 where x in(select x from t2);
update t1 set y = y*10 where x in(select x from t3);

update t2 set y = y*10 where x in(select x from t1);
update t2 set y = y*10 where x in(select x from t2);
update t2 set y = y*10 where x in(select x from t3);

update t3 set y = y*10 where x in(select x from t1);
update t3 set y = y*10 where x in(select x from t2);
update t3 set y = y*10 where x in(select x from t3);

select * from t1 order by 1;
select * from t2 order by 1;
select * from t3 order by 1;

drop table t1;
drop table t2;
drop table t3;


------------------------------------------------------------
-- test "update .. where in ..." 
-- from two different kinds of table on different nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int primary key, y int) distribute by replication to group ng2;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng3;


insert into t1 select v, v from generate_series(1,40) as v;
insert into t2 select v, v from generate_series(21,40) as v;
insert into t3 select * from t1 where x%2 = 0;

update t1 set y = y*10 where x in(select x from t2);
update t1 set y = y*10 where x in(select x from t3);

update t2 set y = y*10 where x in(select x from t1);
update t2 set y = y*10 where x in(select x from t3);

update t3 set y = y*10 where x in(select x from t1);
update t3 set y = y*10 where x in(select x from t2);

select * from t1 order by 1;
select * from t2 order by 1;
select * from t3 order by 1;

drop table t1;
drop table t2;
drop table t3;


------------------------------------------------------------
-- test "update .. where in ..." 
-- from the two same kinds of table on different nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
update t1 set y = y*10 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


create table t1(x int primary key, y int) distribute by replication to group ng1;
create table t2(x int primary key, y int) distribute by replication to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
update t1 set y = y*10 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


create table t1(x int, y int) with(orientation = column) distribute by hash(x) to group ng1;
create table t2(x int, y int) with(orientation = column) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2;
update t1 set y = y*10 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


/*
 * Checkpoint 3
 * delete && truncate
 */

 ------------------------------------------------------------
-- test "delete ... where in ..."
-- from two different kinds of table on the same nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by replication to group ng1;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng1;
create table t4(x int, y int) with(orientation = column) distribute by replication to group ng1;

insert into t1 select v, v from generate_series(1,40) as v;
insert into t2 select v, v from generate_series(21,40) as v;
insert into t3 select * from t1 where x%2 = 0;
insert into t4 select * from t2 where x%2 = 0;

delete from t1 where x in(select x from t1);
insert into t1 select v, v from generate_series(1,40) as v;
delete from t1 where x in(select x from t2);
insert into t1 select v, v from generate_series(1,40) as v;
delete from t1 where x in(select x from t3);
insert into t1 select v, v from generate_series(1,40) as v;
delete from t1 where x in(select x from t4);
insert into t1 select v, v from generate_series(1,40) as v;

delete from t2 where x in(select x from t1);
insert into t2 select v, v from generate_series(21,40) as v;
delete from t2 where x in(select x from t2);
insert into t2 select v, v from generate_series(21,40) as v;
delete from t2 where x in(select x from t3);
insert into t2 select v, v from generate_series(21,40) as v;
delete from t2 where x in(select x from t4);

delete from t3 where x in(select x from t1);
insert into t3 select * from t1 where x%2 = 0;
delete from t3 where x in(select x from t2);
insert into t3 select * from t1 where x%2 = 0;
delete from t3 where x in(select x from t3);
insert into t3 select * from t1 where x%2 = 0;
delete from t3 where x in(select x from t4);
insert into t3 select * from t1 where x%2 = 0;

delete from t4 where x in(select x from t1);
insert into t4 select * from t1 where x%2 = 0;
delete from t4 where x in(select x from t2);
insert into t4 select * from t1 where x%2 = 0;
delete from t4 where x in(select x from t3);
insert into t4 select * from t1 where x%2 = 0;
delete from t4 where x in(select x from t4);
insert into t4 select * from t1 where x%2 = 0;

select * from t1 order by 1;
select * from t2 order by 1;
select * from t3 order by 1;
select * from t4 order by 1;
drop table t1;
drop table t2;
drop table t3;
drop table t4;


------------------------------------------------------------
-- test "delete ... where in ..." 
-- from two different kinds of table on different nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by replication to group ng2;
create table t3(x int, y int) with(orientation = column) distribute by hash(x) to group ng3;
create table t4(x int, y int) with(orientation = column) distribute by replication to group ng4;


insert into t1 select v, v from generate_series(1,40) as v;
insert into t2 select v, v from generate_series(21,40) as v;
insert into t3 select * from t1 where x%2 = 0;
insert into t3 select * from t2 where x%2 = 0;

delete from t1 where x in(select x from t2);
insert into t1 select v, v from generate_series(1,40) as v;
delete from t1 where x in(select x from t3);
insert into t1 select v, v from generate_series(1,40) as v;
delete from t1 where x in(select x from t4);
insert into t1 select v, v from generate_series(1,40) as v;

delete from t2 where x in(select x from t1);
insert into t2 select v, v from generate_series(21,40) as v;
delete from t2 where x in(select x from t3);
insert into t2 select v, v from generate_series(21,40) as v;
delete from t2 where x in(select x from t4);

delete from t3 where x in(select x from t1);
insert into t3 select * from t1 where x%2 = 0;
delete from t3 where x in(select x from t2);
insert into t3 select * from t1 where x%2 = 0;
delete from t3 where x in(select x from t4);
insert into t3 select * from t1 where x%2 = 0;

delete from t3 where x in(select x from t1);
insert into t3 select * from t2 where x%2 = 0;
delete from t3 where x in(select x from t2);
insert into t3 select * from t2 where x%2 = 0;
delete from t3 where x in(select x from t3);
insert into t3 select * from t2 where x%2 = 0;

select * from t1 order by 1;
select * from t2 order by 1;
select * from t3 order by 1;
select * from t4 order by 1;

drop table t1;
drop table t2;
drop table t3;
drop table t4;


------------------------------------------------------------
-- test "delete ... where in ..." 
-- from two different kinds of table on the same nodegroup
------------------------------------------------------------


create table t1(x int, y int) distribute by hash(x) to group ng1;
create table t2(x int, y int) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2 where x%2=0;
delete from t1 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


create table t1(x int primary key, y int) distribute by replication to group ng1;
create table t2(x int primary key, y int) distribute by replication to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2 where x%2=0;
delete from t1 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


create table t1(x int, y int) with(orientation = column) distribute by hash(x) to group ng1;
create table t2(x int, y int) with(orientation = column) distribute by hash(x) to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2 where x%2=0;
delete from t1 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;


create table t1(x int, y int) with(orientation = column) distribute by replication to group ng1;
create table t2(x int, y int) with(orientation = column) distribute by replication to group ng2;

insert into t2 select v, v*10 from generate_series(1,20) as v;
insert into t1 select x, y from t2 where x%2=0;
delete from t1 where x in(select x from t2);
select * from t1 order by 1;

drop table t1;
drop table t2;

-- drop ng
drop node group ng1;
drop node group ng2;
drop node group ng3;
drop node group ng4;

drop schema nodegroup_misc cascade;
reset query_dop;

-- test preserved group names
create node group query with(datanode1);
create node group optimal with(datanode1);
create node group installation with(datanode1);

drop node group query;
drop node group optimal;
drop node group installation;
