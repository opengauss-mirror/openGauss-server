set enable_material to off;
create table parallel_hashjoin_test_a (id int);
create table parallel_hashjoin_test_b (id int);
insert into parallel_hashjoin_test_a select n from generate_series(1,1000) n;
insert into parallel_hashjoin_test_b select n from generate_series(1,10) n;
analyse parallel_hashjoin_test_a;
analyse parallel_hashjoin_test_b;
explain (costs off) select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10 order by parallel_hashjoin_test_a.id;
select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10 order by parallel_hashjoin_test_a.id;

set force_parallel_mode=on;
set parallel_setup_cost = 1;
set min_parallel_table_scan_size=0;
set parallel_tuple_cost = 0.01;
set enable_nestloop=off;

explain (costs off) select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10 order by parallel_hashjoin_test_a.id;
select * from parallel_hashjoin_test_a left outer join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id where parallel_hashjoin_test_a.id < 10 order by parallel_hashjoin_test_a.id;
-- Forbid parallel Hash Right Join or Hash Full Join.
explain (costs off)select * from parallel_hashjoin_test_a full join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id order by parallel_hashjoin_test_a.id limit 10;
select * from parallel_hashjoin_test_a full join parallel_hashjoin_test_b on parallel_hashjoin_test_a.id = parallel_hashjoin_test_b.id order by parallel_hashjoin_test_a.id limit 10;

-- parallel increase hash buckets
DROP TABLE IF EXISTS par_hash_incr_bucket_a;
DROP TABLE IF EXISTS par_hash_incr_bucket_b;
DROP TABLE IF EXISTS par_hash_incr_bucket_c;
DROP TABLE IF EXISTS par_hash_incr_bucket_d;
create table par_hash_incr_bucket_a(a int,b int,c int,d int,e int);
create table par_hash_incr_bucket_b(a int,b int,c int,d int,e int);
create table par_hash_incr_bucket_c(a int,b int,c int,d int,e int);
create table par_hash_incr_bucket_d(a int,b int,c int,d int,e int);
insert into par_hash_incr_bucket_a select n, n , n , n , n from generate_series(1,100000) n;
insert into par_hash_incr_bucket_b select n, n , n , n , n from generate_series(1,100000) n;
insert into par_hash_incr_bucket_c select n, n , n , n , n from generate_series(1,100000) n;
insert into par_hash_incr_bucket_d select n, n , n , n , n from generate_series(1,100000) n;
explain (costs off) select count(*) from par_hash_incr_bucket_a cross join par_hash_incr_bucket_b cross join par_hash_incr_bucket_c cross join par_hash_incr_bucket_d
where par_hash_incr_bucket_c.a = par_hash_incr_bucket_d.b
 and par_hash_incr_bucket_a.c = par_hash_incr_bucket_b.d
 and par_hash_incr_bucket_b.d = par_hash_incr_bucket_c.a
 and par_hash_incr_bucket_b.e %2 =0;
select count(*) from par_hash_incr_bucket_a cross join par_hash_incr_bucket_b cross join par_hash_incr_bucket_c cross join par_hash_incr_bucket_d
where par_hash_incr_bucket_c.a = par_hash_incr_bucket_d.b
 and par_hash_incr_bucket_a.c = par_hash_incr_bucket_b.d
 and par_hash_incr_bucket_b.d = par_hash_incr_bucket_c.a
 and par_hash_incr_bucket_b.e %2 =0;
DROP TABLE IF EXISTS par_hash_incr_bucket_a;
DROP TABLE IF EXISTS par_hash_incr_bucket_b;
DROP TABLE IF EXISTS par_hash_incr_bucket_c;
DROP TABLE IF EXISTS par_hash_incr_bucket_d;

drop table parallel_hashjoin_test_a;
drop table parallel_hashjoin_test_b;
reset parallel_setup_cost;
reset min_parallel_table_scan_size;
reset parallel_tuple_cost;
reset enable_nestloop;
reset force_parallel_mode;


