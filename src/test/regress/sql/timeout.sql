-----------------------------------------------------------------------------------------------------
-- Verify results
-----------------------------------------------------------------------------------------------------
create schema dsitribute_timeout;
set current_schema = dsitribute_timeout;
create table timeout_table_01(c1 int, c2 int, c3 char(10));
create table timeout_table_02(c1 int, c2 int, c3 char(10));
insert into timeout_table_01 select generate_series(1,1000), generate_series(1,1000), 'row'|| generate_series(1,1000);
insert into timeout_table_02 select generate_series(500,1000,10), generate_series(500,1000,10), 'row'|| generate_series(1,51);
analyze timeout_table_01;
analyze timeout_table_02;
set statement_timeout=1000;
select * from timeout_table_01, timeout_table_02, (select * from timeout_table_01 a inner join timeout_table_01 b on(a.c1 = b.c1) left join timeout_table_02 c on (a.c3 = c.c3) right join timeout_table_01 d on (c.c2 = d.c1));
select * from timeout_table_01, timeout_table_02, (select * from timeout_table_01 a inner join timeout_table_01 b on(a.c1 = b.c1) left join timeout_table_02 c on (a.c3 = c.c3) right join timeout_table_01 d on (c.c2 = d.c1));
select * from timeout_table_01, timeout_table_02, (select * from timeout_table_01 a inner join timeout_table_01 b on(a.c1 = b.c1) left join timeout_table_02 c on (a.c3 = c.c3) right join timeout_table_01 d on (c.c2 = d.c1));
select * from timeout_table_01, timeout_table_02, (select * from timeout_table_01 a inner join timeout_table_01 b on(a.c1 = b.c1) left join timeout_table_02 c on (a.c3 = c.c3) right join timeout_table_01 d on (c.c2 = d.c1));
