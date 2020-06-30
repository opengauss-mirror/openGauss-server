
drop table if exists t000_t1;
create table t000_t1(f1 int, f2 int);
alter table t000_t1 set (append_mode=on, rel_cn_id=12345);
alter table t000_t1 set (append_mode=off);
drop table if exists t000_t1;
