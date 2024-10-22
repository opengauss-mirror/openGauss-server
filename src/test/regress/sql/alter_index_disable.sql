create table alter_index_disable (a int, b int);
insert into alter_index_disable select generate_series(1, 10000) as a, generate_series(1, 10000) as b;
insert into alter_index_disable values (10001, 10001);

-- only function based indexes are supported
create index idx_alter_index_disable on alter_index_disable(a);
alter index idx_alter_index_disable disable;
drop index idx_alter_index_disable;

-- create function based index
create index func_idx_alter_index_disable on alter_index_disable(to_char(a));
explain select * from alter_index_disable where to_char(a) = '1';

-- disable function based index
alter index func_idx_alter_index_disable disable;
explain select * from alter_index_disable where to_char(a) = '1';

-- insert data when index was disabled
delete from alter_index_disable where a = 10001 and b = 10001;
select * from alter_index_disable where a = 10001 and b = 10001;
-- SQL bypass
insert into alter_index_disable values(10002, 10002);
select * from alter_index_disable where a = 10002 and b = 10002;

-- enable function based index
alter index func_idx_alter_index_disable enable;
explain select * from alter_index_disable where to_char(a) = '1';

drop index func_idx_alter_index_disable;
drop table alter_index_disable cascade;
