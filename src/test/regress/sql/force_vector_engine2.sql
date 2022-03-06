create schema test_force_vector2;
set current_schema=test_force_vector2;
create table force_vector_test(id int, val1 int, val2 numeric(10,5));
insert into force_vector_test values(generate_series(1, 10000), generate_series(1, 1000), generate_series(1, 2000));
analyze force_vector_test;
-- partition table
create table force_vector_partition(id int, val1 int, val2 text)
partition by range(id) (
  partition force_vector_p1 values less than (2001),
  partition force_vector_p2 values less than (4001),
  partition force_vector_p3 values less than (6001),
  partition force_vector_p4 values less than (8001),
  partition force_vector_p5 values less than (MAXVALUE)
);
insert into force_vector_partition values(generate_series(1, 10000), generate_series(1, 2000), generate_series(1, 5000));
analyze force_vector_partition;

explain (analyze on, timing off) select /*+ set(try_vector_engine_strategy force) */ id, val1*2, val2+val1 as val3 from force_vector_test where id < 5000 and val1 < 500 order by id limit 10;
explain (analyze on, timing off) select /*+ set(try_vector_engine_strategy force) */ id, avg(val1), sum(val2) from force_vector_partition group by id order by id limit 10;

drop table force_vector_test;
drop schema test_force_vector2 cascade;
