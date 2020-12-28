set enable_global_stats = true;

---
--case 1: delete from normal table
--
drop table t_row;
drop table t_col;
create table t_row(c1 int, c2 int default NULL);
insert into t_row select generate_series(1,1999);
create table t_col(c1 int, c2 int default NULL) with (orientation = orc) tablespace hdfs_ts ;
insert into t_col select * from t_row;
delete from t_col;
insert into t_col select * from t_row;
delete from t_col where c1>1000;
select * from t_col where c1 < 1000 order by c1;
drop table t_row;
drop table t_col;

-----
---case 4: delete where condition using index scan
----
drop schema storage cascade;
create schema STORAGE;
CREATE TABLE STORAGE.IDEX_PARTITION_TABLE_001(COL_INT int) with(orientation = orc) tablespace hdfs_ts ;
insert into STORAGE.IDEX_PARTITION_TABLE_001 values(1000);
delete from STORAGE.IDEX_PARTITION_TABLE_001 where col_int=1000;
select * from STORAGE.IDEX_PARTITION_TABLE_001 ;
drop schema storage cascade;

-----
--- partital sort for delete
-----
create table hw_delete_row_1(id int, cu int, num int);
insert into hw_delete_row_1 values (1, generate_series(1, 10000), generate_series(1, 10000));

create table hw_delete_c3 (id int, cu int, num int) with (orientation = orc) tablespace hdfs_ts distribute by hash(id);
insert into hw_delete_c3 select * from hw_delete_row_1;
delete from hw_delete_c3;

drop table hw_delete_row_1;
drop table hw_delete_c3;
