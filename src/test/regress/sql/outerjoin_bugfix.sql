create schema outerjoin_bugfix;
set search_path = outerjoin_bugfix;

drop table if exists foo_tab1;
drop table if exists foo_tab2;

create table foo_tab1(f1 int, f2 int);
insert into foo_tab1 values(1,1);
create table foo_tab2(c1 int, c2 int);

analyze foo_tab1;
analyze foo_tab2;

--
-- check handling of join aliases when flattening multiple levels of subquery
--
explain (verbose on, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from foo_tab1 i1) ss1
    ) foo2
   left join
    (select c1 as join_key from foo_tab2 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from foo_tab1 i1) ss1
    ) foo2
   left join
    (select c1 as join_key from foo_tab2 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

-- check the original case
insert into foo_tab2 values(2,2);
set enable_hashjoin = off;

explain (verbose on, costs off)
select t2.c2, dt1.col2
from (
    select col1, col2
    from (
        select f1 as col1,
               case when f2 = 1 then 1
               else 999
               end as col2
        from foo_tab1
    ) dt1 join foo_tab2 t2 on dt1.col1 = t2.c1
) dt1 full join foo_tab2 t2 on dt1.col2 = t2.c2;

select t2.c2, dt1.col2
from (
    select col1, col2
    from (
        select f1 as col1,
               case when f2 = 1 then 1
               else 999
               end as col2
        from foo_tab1
    ) dt1 join foo_tab2 t2 on dt1.col1 = t2.c1
) dt1 full join foo_tab2 t2 on dt1.col2 = t2.c2;

set enable_mergejoin = off;

explain (verbose on, costs off)
select t2.c2, dt1.col2
from (
    select col1, col2
    from (
        select f1 as col1,
               case when f2 = 1 then 1
               else 999
               end as col2
        from foo_tab1
    ) dt1 join foo_tab2 t2 on dt1.col1 = t2.c1
) dt1 full join foo_tab2 t2 on dt1.col2 = t2.c2;

select t2.c2, dt1.col2
from (
    select col1, col2
    from (
        select f1 as col1,
               case when f2 = 1 then 1
               else 999
               end as col2
        from foo_tab1
    ) dt1 join foo_tab2 t2 on dt1.col1 = t2.c1
) dt1 full join foo_tab2 t2 on dt1.col2 = t2.c2;

drop schema outerjoin_bugfix cascade;