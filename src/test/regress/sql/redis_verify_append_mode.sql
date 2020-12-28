drop table if exists t1;
drop table if exists t2;
create table t1 (c1 int, c2 int);

-- test append_mode on to read_only and back

alter table t1 set (append_mode=on, rel_cn_oid=12345);

\d+ t1

alter table t1 set (append_mode=read_only);

\d+ t1

alter table t1 set (append_mode=on, rel_cn_oid=12345);

\d+ t1

alter table t1 set (append_mode=read_only);

\d+ t1

-- test drop table in read only mode and it fails

drop table t1;

-- test drop table in iud mode and it works

alter table t1 set (append_mode=on, rel_cn_oid=12345);

drop table t1;

-- test partition table

create table t2 (c1 int, c2 int) distribute by hash (c1)
partition by range (c2)
(
  partition p0 values less than (10),
  partition p1 values less than (20),
  partition p2 values less than (30),
  partition p3 values less than (maxvalue)
);

insert into t2 values(1, 8);
insert into t2 values(2, 11);
insert into t2 values(3, 21);
insert into t2 values(4, 31);

-- test truncate partition in iud mode it works

alter table t2 set (append_mode=on, rel_cn_oid=12345);

\d+ t2

alter table t2 truncate partition p0;

-- test truncate partition in read only mode and it fails

alter table t2 set (append_mode=read_only);

\d+ t2

alter table t2 truncate partition p1;

-- test iud at iud mode

alter table t2 set (append_mode=on, rel_cn_oid=12345);

\d+ t2

CREATE SCHEMA data_redis;

insert into t2 values(1,2);
delete t2 where c2 = 31;
update t2 set c2 = 5 where c1=1;
vacuum t2;

-- test iud at read only mode

alter table t2 set (append_mode=read_only);
\d+ t2

insert into t2 values(1,2);
delete t2 where c2 = 31;
update t2 set c2 = 5 where c1=1;
vacuum t2;

-- clean up
alter table t2 set (append_mode=off);
drop table t2;

create table x (x int, y int) ;
insert into x select v, v from generate_series(1, 10) as v;

alter table x set (append_mode=read_only);

update x set y=21;
delete from x;
insert into x select v, v from generate_series(1, 10) as v;
drop table x;
DROP SCHEMA data_redis CASCADE;
