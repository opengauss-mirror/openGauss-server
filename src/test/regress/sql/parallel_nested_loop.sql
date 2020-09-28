create table parallel_nested_loop_test_a (id int);
create table parallel_nested_loop_test_b (id int);

insert into parallel_nested_loop_test_a select n from generate_series(1,2) n;
insert into parallel_nested_loop_test_b select n from generate_series(1,5) n;

explain (costs off) select * from parallel_nested_loop_test_a left outer join parallel_nested_loop_test_b on parallel_nested_loop_test_a.id = 1;
select * from parallel_nested_loop_test_a left outer join parallel_nested_loop_test_b on parallel_nested_loop_test_a.id = 1;

set parallel_setup_cost = 0;
set min_parallel_table_scan_size=0;

explain (costs off) select * from parallel_nested_loop_test_a left outer join parallel_nested_loop_test_b on parallel_nested_loop_test_a.id = 1;
select * from parallel_nested_loop_test_a left outer join parallel_nested_loop_test_b on parallel_nested_loop_test_a.id = 1;

drop table parallel_nested_loop_test_a;
drop table parallel_nested_loop_test_b;

reset parallel_setup_cost;
reset min_parallel_table_scan_size;