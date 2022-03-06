CREATE USER test_any_role PASSWORD 'Gauss@1234';
GRANT insert any table to test_any_role;
GRANT update any table to test_any_role;
GRANT delete any table to test_any_role;
CREATE USER test_user PASSWORD 'Gauss@1234';

--db4ai
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
insert into db4ai.snapshot(id) values(1);
update db4ai.snapshot set id = 2 where id = 1;
delete from db4ai.snapshot;
select * from db4ai.snapshot;

reset role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
insert into db4ai.snapshot(id) values(1);
update db4ai.snapshot set id = 2 where id = 1;
delete from db4ai.snapshot;
select * from db4ai.snapshot;

--information_schema
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
insert into information_schema.sql_features(feature_id) values(1);
update information_schema.sql_features set feature_name = 'Embedded Ada1' where feature_id = 'B011';
delete from information_schema.sql_features;
select * from information_schema.sql_features where feature_id = 'B011';

reset role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
insert into information_schema.sql_features(feature_id) values(1);
update information_schema.sql_features set feature_name = 'Embedded Ada1' where feature_id = 'B011';
delete from information_schema.sql_features;
select * from information_schema.sql_features where feature_id = 'B011';

--dbe_perf
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
select count(*) from dbe_perf.user_transaction;
delete from dbe_perf.user_transaction;

reset role;
GRANT select any table to test_any_role;
GRANT delete any table to test_any_role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
select count(*) from dbe_perf.user_transaction;
delete from dbe_perf.user_transaction;

--cstore
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
select count(*) from sys.sys_dummy;
delete from sys.sys_dummy;
reset role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
select count(*) from sys.sys_dummy;
delete from sys.sys_dummy;

--pg_catalog
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
select count(*) from pg_catalog.pg_authid;

reset role;
GRANT select any table to test_any_role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
select count(*) from pg_catalog.pg_authid;

--sys
reset role;
SET ROLE test_user PASSWORD 'Gauss@1234';
select count(*) from sys.my_jobs;

reset role;
GRANT select any table to test_any_role;
SET ROLE test_any_role PASSWORD 'Gauss@1234';
select count(*) from sys.my_jobs;
reset role;
drop user test_any_role cascade;
drop user test_user cascade;
