--
-- check for over-optimization of whole-row Var referencing an Append plan
--
create table int4_test(f1 int4);
select (select q from (select 1,2,3 where f1>0 union all select 4,5,6 where f1<=0) q) from int4_test;
select (select q from (select 1,2,3 where f1>0 union all select 4,5,6.0 where f1<=0) q) from int4_test;
insert into int4_test(f1) values(' 0 ');
insert into int4_test(f1) values('123456 ');
select * from int4_test;
select (select q from (select 1,2,3 where f1>0 union all select 4,5,6 where f1<=0) q) from int4_test;
select (select q from (select 1,2,3 where f1>0 union all select 4,5,6.0 where f1<=0) q) from int4_test;