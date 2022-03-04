create schema merge_subquery2;
set current_schema=merge_subquery2;

create table merge_subquery_utest1(id int, val int) with(storage_type=ustore);
create table merge_subquery_utest2(id int, val int) with(storage_type=ustore);
insert into merge_subquery_utest1 values(generate_series(1, 10), generate_series(1, 5));
insert into merge_subquery_utest2 values(generate_series(1, 5), generate_series(21, 25));
insert into merge_subquery_utest2 values(generate_series(11, 15), generate_series(11, 15));

explain merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg2.val+mg1.val)
when not matched then
  insert values(mg2.id, mg2.val);

START TRANSACTION;
merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg2.val+mg1.val)
when not matched then
  insert values(mg2.id, mg2.val);
select * from merge_subquery_utest1;
ROLLBACK;

explain merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select sum(val) from merge_subquery_utest2 mg3)
when not matched then
  insert values(mg2.id, mg2.val);

START TRANSACTION;
merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select sum(val) from merge_subquery_utest2 mg3)
when not matched then
  insert values(mg2.id, mg2.val);
select * from merge_subquery_utest1;
ROLLBACK;

explain merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg3.val from (select * from merge_subquery_utest1) as mg3 where mg3.id in (select id from merge_subquery_utest2) limit 1)
when not matched then
  insert values(mg2.id, mg2.val);  

START TRANSACTION;
merge into merge_subquery_utest1 mg1
using merge_subquery_utest2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg3.val from (select * from merge_subquery_utest1) as mg3 where mg3.id in (select id from merge_subquery_utest2) limit 1)
when not matched then
  insert values(mg2.id, mg2.val);
select * from merge_subquery_utest1;
ROLLBACK;

-- subpartition
create table partition_table(id int, val1 int, val2 int, val3 int)
partition by range (id) subpartition by list (val1)
(
  partition p_1 values less than(5)
  (
    subpartition p_11 values ('1','2'),
	subpartition p_12 values ('3','4'),
	subpartition p_13 values ('5')
  ),
  partition p_2 values less than(10)
  (
    subpartition p_21 values ('1','2'),
	subpartition p_22 values ('3','4'),
	subpartition p_23 values ('5')
  ),
  partition p_3 values less than(20)
  (
    subpartition p_31 values ('1','2'),
	subpartition p_32 values ('3','4'),
	subpartition p_33 values ('5')
  )
);
insert into partition_table values(generate_series(1, 10), generate_series(1,5), generate_series(1,2), generate_series(1,10));

explain
merge into partition_table t1
using merge_subquery_utest2 t2 on t1.id=t2.id
when matched then
  update set t1.val2 = (select t2.val + t1.val2) and
         t1.val3 = (select t3.id from merge_subquery_utest1 t3 where id=3)
when not matched then
  insert values(t2.id, t2.val, (select t4.val from merge_subquery_utest1 t4 where id=7), t2.val*2);

START TRANSACTION;
merge into partition_table t1
using merge_subquery_utest2 t2 on t1.id=t2.id
when matched then
  update set t1.val2 = (select t2.val + t1.val2) and
         t1.val3 = (select t3.id from merge_subquery_utest1 t3 where id=3)
when not matched then
  insert values(t2.id, t2.val, (select t4.val from merge_subquery_utest1 t4 where id=7), t2.val*2);
select * from partition_table;
ROLLBACK;
