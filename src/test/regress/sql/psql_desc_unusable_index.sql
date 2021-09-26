-- validate description texts of unusable/reindexed indexes
create table test_table(a int, b int);
create index test_table_idx1 on test_table(a,b);
\d+ test_table;
alter index test_table_idx1 unusable;
\d+ test_table;
reindex index test_table_idx1;
\d+ test_table;
drop table test_table;
