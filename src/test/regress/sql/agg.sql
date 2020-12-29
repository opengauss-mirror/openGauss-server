create schema aggregate;
set current_schema='aggregate';
create table t1 (a int , b int);
insert into t1 values(1,2);

set enable_hashagg = off;
--force hash agg, if used sort agg will report error.
select a , count(distinct  generate_series(1,2)) from t1 group by a;
explain (verbose, costs off)
select a , count(distinct  generate_series(1,2)) from t1 group by a;

drop table t1;
drop schema aggregate;
