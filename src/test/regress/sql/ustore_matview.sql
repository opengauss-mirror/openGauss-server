--prepare
create table t1(c1 int, c2 int)with(storage_type=ustore);
create table t2(c1 int, c2 int)with(storage_type=ustore);
create table t3(c1 int, c2 int)with(storage_type=ustore);

--single table
truncate table t1;
create incremental materialized view mv1 as select * from t1 where t1.c2 = 1;

insert into t1 values(1, 1);
insert into t1 values(2, 2);
insert into t1 values(3, 3);

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
select * from mv1 order by 1;

insert into t1 values(1, 1);
insert into t1 values(1, 1);
delete from t1 where c2 = 2 or c2 = 3;

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
select * from mv1 order by 1;

update t1 set c2 = 2 where c1 = 1;
update t1 set c2 = 1 where c1 = 1;

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
select * from mv1 order by 1;

drop materialized view mv1;

--unionall
truncate table t1;
truncate table t2;
truncate table t3;

create incremental materialized view mv1 as select * from t1 union all select * from t2 union all select * from t3;
create incremental materialized view mv2 as select * from t1 where t1.c2 = 1 union all select * from t2 where t2.c2 = 1 union all select * from t3;

insert into t1 values(1, 1);
insert into t1 values(2, 2);
insert into t2 values(1, 1);
insert into t2 values(2, 2);
insert into t3 values(1, 1);
insert into t3 values(2, 2);

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
REFRESH INCREMENTAL MATERIALIZED VIEW mv2;
select * from mv1 order by 1;
select * from mv2 order by 1;

update t1 set c2 = 2 where c1 = 1;
update t2 set c2 = 2 where c1 = 1;
update t3 set c2 = 2 where c1 = 1;
update t1 set c2 = 1 where c1 = 1;
update t2 set c2 = 1 where c1 = 1;
update t3 set c2 = 1 where c1 = 1;

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
REFRESH INCREMENTAL MATERIALIZED VIEW mv2;
select * from mv1 order by 1;
select * from mv2 order by 1;

delete from t1 where c2 = 2;
delete from t2 where c2 = 2;
delete from t3 where c2 = 2;
insert into t1 values(2, 2);
insert into t2 values(2, 2);
insert into t3 values(2, 2);

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
REFRESH INCREMENTAL MATERIALIZED VIEW mv2;
select * from mv1 order by 1;
select * from mv2 order by 1;

insert into t1 values(1, 1);
delete from t1 where c2 = 1;

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
select * from mv1 order by 1;

delete from t2 where c2 = 2;
delete from t3 where c2 = 2;

REFRESH INCREMENTAL MATERIALIZED VIEW mv2;
select * from mv2 order by 1;

insert into t1 values(3, 1);
insert into t2 values(3, 1);
insert into t3 values(3, 1);

REFRESH INCREMENTAL MATERIALIZED VIEW mv1;
REFRESH INCREMENTAL MATERIALIZED VIEW mv2;
select * from mv1 order by 1;
select * from mv2 order by 1;

REFRESH MATERIALIZED VIEW mv1;
REFRESH MATERIALIZED VIEW mv1;
select * from mv1 order by 1;
select * from mv2 order by 1;

--clear
drop materialized view mv1;
drop materialized view mv2;
drop table t1;
drop table t2;
drop table t3;

create table nt1(c1 int, c2 int)with(storage_type=ustore);
create table nt2(c1 int, c2 char)with(storage_type=ustore);
create  materialized view nmv as select nt1.c1 c1, nt1.c2 c2, nt2.c2 c3 from nt1 inner join nt2 on nt1.c1 = nt2.c1;
insert into nt1 values(1,1),(2,2),(3,3);
insert into nt2 values(2,'b'),(3,'c'),(4,'d');
create view view1 as select nt1.c1 c1, nt1.c2 c2, nt2.c2 c3 from nt1 inner join nt2 on nt1.c1 = nt2.c1;
refresh materialized view nmv;
select * from view1 except select * from nmv;
drop materialized view nmv;
select * from gs_matview;
select * from gs_matviews;
drop view view1;
drop table nt1;
drop table nt2;
