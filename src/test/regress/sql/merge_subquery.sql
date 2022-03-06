create table merge_subquery_test1(id int, val int);
create table merge_subquery_test2(id int, val int);
insert into merge_subquery_test1 values(generate_series(1, 10), generate_series(1, 5));
insert into merge_subquery_test2 values(generate_series(1, 5), generate_series(21, 25));
insert into merge_subquery_test2 values(generate_series(11, 15), generate_series(11, 15));

explain merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg2.val+mg1.val)
when not matched then
  insert values(mg2.id, mg2.val);
  
merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select mg2.val+mg1.val)
when not matched then
  insert values(mg2.id, mg2.val);

select * from merge_subquery_test1;

delete from merge_subquery_test1 where id > 10;
  
explain merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select sum(val) from merge_subquery_test2 mg3)
when not matched then
  insert values(mg2.id, mg2.val);

merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=(select sum(val) from merge_subquery_test2 mg3)
when not matched then
  insert values(mg2.id, mg2.val);

select * from merge_subquery_test1;

delete from merge_subquery_test1 where id > 10;

explain merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=mg2.val
when not matched then
  insert values(mg2.id, (select mg2.val * 2));

merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=mg2.val
when not matched then
  insert values(mg2.id, (select mg2.val * 2));

select * from merge_subquery_test1;

delete from merge_subquery_test1 where id > 10;

explain merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=mg2.val
when not matched then
  insert values(mg2.id, (select mg3.val from merge_subquery_test1 mg3 limit 1));

merge into merge_subquery_test1 mg1
using merge_subquery_test2 mg2 on mg1.id=mg2.id
when matched then
  update set mg1.val=mg2.val
when not matched then
  insert values(mg2.id, (select mg3.val from merge_subquery_test1 mg3 limit 1));

select * from merge_subquery_test1;

drop table merge_subquery_test1;
drop table merge_subquery_test2;
