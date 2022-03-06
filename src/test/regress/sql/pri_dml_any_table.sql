CREATE USER test_select_any_table_role PASSWORD 'Gauss@1234';

CREATE SCHEMA pri_select_schema;
set search_path=pri_select_schema;
--create table
CREATE table pri_select_schema.tb_pri (id int, name VARCHAR(10));
insert into pri_select_schema.tb_pri values(1,'joe');

SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
select * from pri_select_schema.tb_pri;
insert into pri_select_schema.tb_pri values(2,'ly');
update pri_select_schema.tb_pri set name = 'gauss' where id = 1;
delete pri_select_schema.tb_pri;

reset role;
GRANT select any table to test_select_any_table_role;
SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
select * from pri_select_schema.tb_pri;
insert into pri_select_schema.tb_pri values(1,'joe');
update pri_select_schema.tb_pri set name = 'gauss' where id = 1;
delete pri_select_schema.tb_pri;

reset role;
revoke select any table from test_select_any_table_role;
GRANT insert any table to test_select_any_table_role;
SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
select * from pri_select_schema.tb_pri;
insert into pri_select_schema.tb_pri values(2,'johy');
update pri_select_schema.tb_pri set name = 'gauss' where id = 1;
delete pri_select_schema.tb_pri;

reset role;
revoke insert any table from test_select_any_table_role;
GRANT update any table to test_select_any_table_role;
SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
select * from pri_select_schema.tb_pri;
insert into pri_select_schema.tb_pri values(3,'lili');
--failed
update pri_select_schema.tb_pri set name = 'gauss' where id = 1;
delete pri_select_schema.tb_pri;
reset role;
grant select on table  pri_select_schema.tb_pri to test_select_any_table_role;
SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
update pri_select_schema.tb_pri set name = 'gauss' where id = 1;

reset role;
revoke select on table  pri_select_schema.tb_pri from test_select_any_table_role;
revoke update any table from test_select_any_table_role;
GRANT delete any table to test_select_any_table_role;
SET ROLE test_select_any_table_role PASSWORD 'Gauss@1234';
select * from pri_select_schema.tb_pri;
insert into pri_select_schema.tb_pri values(3,'lili');
update pri_select_schema.tb_pri set name = 'gauss' where id = 3;
delete pri_select_schema.tb_pri;

reset role;
drop table pri_select_schema.tb_pri;
DROP SCHEMA pri_select_schema cascade;
DROP USER test_select_any_table_role cascade;