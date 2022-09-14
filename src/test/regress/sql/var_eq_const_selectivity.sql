DROP SCHEMA var_eq_const_selectivity CASCADE;
CREATE SCHEMA var_eq_const_selectivity;
SET CURRENT_SCHEMA TO var_eq_const_selectivity;
drop table test_const;
drop table t2;
-- ustore
create table test_const (a int, b int) with (storage_type = ustore);
create index idx on test_const(a, b);
insert into test_const values(101, generate_series(1, 300));
insert into test_const values(generate_series(1, 100), generate_series(1, 10000));
insert into test_const values(150, generate_series(1, 150000));
insert into test_const values(151, generate_series(1, 100000));
analyze test_const;
create table t2 (c int, d int) with (storage_type = ustore);
create index idx2 on t2(c);
insert into t2 values(200, generate_series(1, 2000));
analyze t2;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 150;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 200;
-- set with HINT
explain select/*+set (var_eq_const_selectivity on)*/ a, b from test_const where a = 200;
show var_eq_const_selectivity;
-- JOIN tables
explain (costs off) select a, b from test_const, t2 where a = c and a = 200;
explain (costs off) select count(*) from test_const, t2 where a = c and a = 200;
select count(*) from test_const, t2 where a = c and a = 200;
explain (costs off) select a, b from test_const, t2 where a = c and a = 200 limit 1;
select a, b from test_const, t2 where a = c and a = 200 limit 1;
explain (costs off) select a, b from test_const left outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const full outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const left join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right join t2 on a = c where a = 200;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 150;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 200;
-- JOIN tables
explain (costs off) select a, b from test_const, t2 where a = c and a = 200;
explain (costs off) select count(*) from test_const, t2 where a = c and a = 200;
select count(*) from test_const, t2 where a = c and a = 200;
explain (costs off) select a, b from test_const, t2 where a = c and a = 200 limit 1;
select a, b from test_const, t2 where a = c and a = 200 limit 1;
explain (costs off) select a, b from test_const left outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const full outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const left join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right join t2 on a = c where a = 200;

drop table test_const;
drop table t2;
-- astore
create table test_const (a int, b int) with (storage_type = astore);
create index idx on test_const(a, b);
insert into test_const values(101, generate_series(1, 300));
insert into test_const values(generate_series(1, 100), generate_series(1, 10000));
insert into test_const values(150, generate_series(1, 150000));
insert into test_const values(151, generate_series(1, 100000));
analyze test_const;
create table t2 (c int, d int) with (storage_type = astore);
create index idx2 on t2(c);
insert into t2 values(200, generate_series(1, 2000));
analyze t2;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 150;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 200;
-- JOIN tables
explain (costs off) select a, b from test_const, t2 where a = c and a = 200;
explain (costs off) select count(*) from test_const, t2 where a = c and a = 200;
select count(*) from test_const, t2 where a = c and a = 200;
explain (costs off) select a, b from test_const, t2 where a = c and a = 200 limit 1;
select a, b from test_const, t2 where a = c and a = 200 limit 1;
explain (costs off) select a, b from test_const left outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const full outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const left join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right join t2 on a = c where a = 200;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 150;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 200;
-- JOIN tables
explain (costs off) select a, b from test_const, t2 where a = c and a = 200;
explain (costs off) select count(*) from test_const, t2 where a = c and a = 200;
select count(*) from test_const, t2 where a = c and a = 200;
explain (costs off) select a, b from test_const, t2 where a = c and a = 200 limit 1;
select a, b from test_const, t2 where a = c and a = 200 limit 1;
explain (costs off) select a, b from test_const left outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const full outer join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const left join t2 on a = c where a = 200;
explain (costs off) select a, b from test_const right join t2 on a = c where a = 200;

drop table test_const;
-- boundary value for INT1
create table test_const (a INT1, b int) with (storage_type = astore);
create index idx on test_const(a, b);
insert into test_const values(101, generate_series(1,300));
insert into test_const values(generate_series(1,100), generate_series(1,10000));
insert into test_const values(255, generate_series(1,150000));
insert into test_const values(151, generate_series(1,100000));
analyze test_const;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 255::INT1;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101::INT1;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11::INT1;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 0::INT1;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = 255::INT1;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = 101::INT1;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = 11::INT1;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 0::INT1;

drop table test_const;
-- boundary value for INT2
create table test_const (a INT2, b int) with (storage_type = astore);
create index idx on test_const(a, b);
insert into test_const values(-32768, generate_series(1, 300));
insert into test_const values(generate_series(-32766, -32667), generate_series(1, 10000));
insert into test_const values(-20000, generate_series(1, 150000));
insert into test_const values(20000, generate_series(1, 100000));
analyze test_const;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -32768;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -32667;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 200;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -32768;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -32667;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 200;

drop table test_const;
-- boundary value for INT4
create table test_const (a INT4, b int) with (storage_type = ustore);
create index idx on test_const(a, b);
insert into test_const values(-2147483648, generate_series(1, 300));
insert into test_const values(generate_series(-2147483647, -2147483548), generate_series(1, 10000));
insert into test_const values(-20000, generate_series(1, 150000));
insert into test_const values(20000, generate_series(1, 100000));
analyze test_const;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -2147483648;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -2147483647;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 2147483647;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -2147483648;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -2147483647;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 2147483647;

drop table test_const;
-- boundary value for INT8
create table test_const (a INT8, b int) with (storage_type = ustore);
create index idx on test_const(a, b);
insert into test_const values(-9223372036854775808, generate_series(1, 300));
insert into test_const values(generate_series(-9223372036854775807,-9223372036854775708), generate_series(1, 10000));
insert into test_const values(-20000, generate_series(1, 150000));
insert into test_const values(20000, generate_series(1, 100000));
analyze test_const;
set var_eq_const_selectivity = off;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -9223372036854775808;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -9223372036854775807;
-- const does not belong to MCV or histogram
explain (costs off) select a, b from test_const where a = 9223372036854775807;
set var_eq_const_selectivity = on;
-- const falls into MCV
explain (costs off) select a, b from test_const where a = -20000;
-- const falls into histogram bucket with equal bounds
explain (costs off) select a, b from test_const where a = -9223372036854775808;
-- const falls into histogram bucket with unequal bounds
explain (costs off) select a, b from test_const where a = -9223372036854775807;
-- const does not belong to MCV or histogram
explain select a, b from test_const where a = 9223372036854775807;
reset var_eq_const_selectivity;
drop table test_const;
drop table t2;
DROP SCHEMA var_eq_const_selectivity CASCADE;
