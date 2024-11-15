create table alter_index_disable (a int, b int);
insert into alter_index_disable select generate_series(1, 10000) as a, generate_series(1, 10000) as b;
insert into alter_index_disable values (10001, 10001);
CREATE OR REPLACE PROCEDURE insert_data
IS
BEGIN
    INSERT INTO alter_index_disable(a, b) VALUES (10003, 10003);
END;
/

-- only function based indexes are supported
create index idx_alter_index_disable on alter_index_disable(a);
alter index idx_alter_index_disable disable;
drop index idx_alter_index_disable;

-- create function based index
create index func_idx_alter_index_disable on alter_index_disable(to_char(a));
explain (costs off) select * from alter_index_disable where to_char(a) = '1';

-- disable function based index
alter index func_idx_alter_index_disable disable;
explain (costs off) select * from alter_index_disable where to_char(a) = '1';

-- insert data when index was disabled
delete from alter_index_disable where a = 10001 and b = 10001;
select * from alter_index_disable where a = 10001 and b = 10001;

-- SQL bypass
explain (costs off) insert into alter_index_disable values(10002, 10002);
insert into alter_index_disable values(10002, 10002);
select * from alter_index_disable where a = 10002 and b = 10002;

-- stored procedure
CALL insert_data();
select * from alter_index_disable where a = 10003 and b = 10003;

-- enable function based index
alter index func_idx_alter_index_disable enable;
explain (costs off) select * from alter_index_disable where to_char(a) = '1';

\h alter index

drop procedure insert_data;
drop index func_idx_alter_index_disable;
drop table alter_index_disable cascade;