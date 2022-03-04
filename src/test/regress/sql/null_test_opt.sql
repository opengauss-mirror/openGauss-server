create schema null_test_opt_nsp;

set search_path = null_test_opt_nsp;

create table t(a int, b int not null);

insert into t values(1, 1);

explain (costs off) select * from t where b is null order by 1, 2;

select * from t where b is null order by 1, 2;

explain (costs off) select * from t where b is not null order by 1, 2;

select * from t where b is not null order by 1, 2;

explain (costs off) select * from t where b is null or b is not null order by 1, 2;

select * from t where b is null or b is not null order by 1, 2;

explain (costs off) select * from t where b is null or a = 1 order by 1, 2;

select * from t where b is null or a = 1 order by 1, 2;

explain (costs off) select * from t where b is null and a = 1 order by 1, 2;

select * from t where b is null and a = 1 order by 1, 2;

explain (costs off) select * from t tt1 join t tt2 on tt1.a = tt2.b where tt1.a is null;

select * from t tt1 join t tt2 on tt1.a = tt2.b where tt1.a is null;

drop schema null_test_opt_nsp cascade;