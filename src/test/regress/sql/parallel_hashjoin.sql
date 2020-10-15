create table parallel_hashjoin_test_a (id int);
create table parallel_hashjoin_test_b (id int);
insert into parallel_hashjoin_test_a select n from generate_series(1,1000) n;
insert into parallel_hashjoin_test_b select n from generate_series(1,10) n;
analyse parallel_hashjoin_test_a;
analyse parallel_hashjoin_test_b;
explain (costs off) select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10;
select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10;

set parallel_setup_cost = 1;
set min_parallel_table_scan_size=0;
set parallel_tuple_cost = 0.01;
set enable_nestloop=off;

explain (costs off) select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10;
select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10;

reset parallel_setup_cost;
reset min_parallel_table_scan_size;
reset parallel_tuple_cost;
reset enable_nestloop;

