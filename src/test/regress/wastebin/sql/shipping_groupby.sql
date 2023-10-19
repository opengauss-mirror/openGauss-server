set current_schema='shipping_schema';
set enable_sort = off;
set enable_hashagg = on;
explain (num_costs off)
select count(distinct(b)) from shipping_test_row group by d;

set enable_sort = on;
set enable_hashagg = on;
explain (num_costs off)
select count(distinct(b)) from shipping_test_row group by d;

explain (num_costs off)
select count(distinct(d)) from shipping_test_row group by d;
