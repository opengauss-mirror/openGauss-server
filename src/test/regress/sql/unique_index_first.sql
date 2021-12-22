create schema unique_index_first;
set current_schema to unique_index_first;

create table unique_index_first (col1 int, col2 int, col3 int, col4 int, col5 int);
alter table unique_index_first add primary key (col1, col2, col3, col4);
create index index_no_unique on unique_index_first using btree(col1, col2, col3, col5);

--1. A btree plan containing unique columns is prefered.
explain (costs off) select * from unique_index_first where col2 = 2 and col4 = 4 and col3 = 3 and col1 = 1;

--2. Only equivalence constraint can active the unique_btree_index rule.
explain (costs off) select * from unique_index_first where col2 = 2 and col4 < 4 and col3 = 3 and col1 = 1;

--3. test seqscan
set enable_indexscan=off;
set enable_bitmapscan=off;

explain (costs off) select * from unique_index_first where col2 = 2 and col4 = 4 and col3 = 3 and col1 = 1;
explain (costs off) select * from unique_index_first where col2 = 2 and col4 < 4 and col3 = 3 and col1 = 1;

reset sql_beta_feature;
drop schema unique_index_first cascade;
