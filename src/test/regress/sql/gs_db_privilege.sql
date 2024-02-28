--test analyze verify skip table
create database ana;
\c ana;
create user u1 password 'Aa@12345';
create user u2 password 'Aa@12345';
set role u2 password 'Aa@12345';
create table tb2(id int);
set role u1 password 'Aa@12345';
analyze verify fast;
analyze verify complete;
\c regression;
drop database ana;

-- prepare
CREATE ROLE db_priv_user PASSWORD '1234567i*';
CREATE ROLE db_priv_user1 PASSWORD '1234567i*';
CREATE ROLE db_priv_user2 PASSWORD '1234567i*';
CREATE ROLE db_priv_user3 PASSWORD '1234567i*';
CREATE ROLE db_priv_user4 PASSWORD '1234567i*';
CREATE ROLE db_priv_user5 PASSWORD '1234567i*';

-- system relation privilege check
SET ROLE db_priv_user PASSWORD '1234567i*';
SELECT * FROM gs_db_privilege ORDER BY oid;
SELECT * FROM gs_db_privileges ORDER BY rolename;
SELECT has_any_privilege('db_priv_user','UPDATE ANY TABLE');

-- pg_shdepend
RESET ROLE;
CREATE DATABASE db_priv_base;
\c db_priv_base
CREATE ROLE db_priv_user0 PASSWORD '1234567i*';
CREATE ROLE db_priv_user00 PASSWORD '1234567i*';

select d.datname,a.rolname,p.privilege_type from pg_shdepend s join pg_authid a on s.refobjid=a.oid
  join pg_database d on s.dbid=d.oid join gs_db_privilege p on s.objid=p.oid; --noting

GRANT SELECT ANY TABLE,DROP ANY TABLE TO db_priv_user, db_priv_user0, db_priv_user00;
select d.datname,a.rolname,p.privilege_type from pg_shdepend s join pg_authid a on s.refobjid=a.oid
  join pg_database d on s.dbid=d.oid join gs_db_privilege p on s.objid=p.oid order by a.rolname,p.privilege_type; --6 lines

DROP USER db_priv_user00;
DROP USER db_priv_user00 CASCADE;

DROP USER db_priv_user0;
REVOKE SELECT ANY TABLE FROM db_priv_user0;
DROP USER db_priv_user0;
REVOKE DROP ANY TABLE FROM db_priv_user0;
DROP USER db_priv_user0;

\c postgres
GRANT SELECT ANY TABLE TO db_priv_user;
select d.datname,a.rolname,p.privilege_type from pg_shdepend s join pg_authid a on s.refobjid=a.oid
  join pg_database d on s.dbid=d.oid join gs_db_privilege p on s.objid=p.oid; --1 line

DROP USER db_priv_user CASCADE;

\c db_priv_base
DROP USER db_priv_user CASCADE;
REVOKE SELECT ANY TABLE,DROP ANY TABLE FROM db_priv_user;
DROP USER db_priv_user CASCADE;

\c postgres
DROP USER db_priv_user;
DROP USER db_priv_user CASCADE;

\c regression
DROP DATABASE db_priv_base;

--syntax and gs_db_privilege
RESET ROLE;
GRANT SELECT ANY TABLES TO db_priv_user3; --failed
REVOKE SELECT ANY TABLES FROM db_priv_user3; --failed

GRANT DELETE ANY TABLE TO PUBLIC; --failed
REVOKE DELETE ANY TABLE FROM PUBLIC; --failed

GRANT SELECT ANY TABLE TO db_priv_user; --failed
REVOKE SELECT ANY TABLE FROM db_priv_user; --failed

GRANT SELECT ANY TABLE,DROP ANY TABLE TO db_priv_user1,db_priv_user2;
GRANT update any table TO db_priv_user3, db_priv_user4 WITH ADMIN OPTION;
SELECT * FROM gs_db_privileges ORDER BY rolename;

GRANT SELECT ANY TABLE TO db_priv_user1; --no change
GRANT SELECT ANY TABLE TO db_priv_user2 WITH ADMIN OPTION; --change to yes
REVOKE ADMIN OPTION FOR DROP ANY TABLE FROM db_priv_user1,db_priv_user2; --no change
REVOKE ADMIN OPTION FOR update ANY TABLE FROM db_priv_user3; --change to no
REVOKE update ANY TABLE FROM db_priv_user4; --delete
SELECT * FROM gs_db_privileges ORDER BY rolename;

REVOKE SELECT ANY TABLE,DROP ANY TABLE,update any table FROM db_priv_user1,db_priv_user2,db_priv_user3,db_priv_user4;
SELECT * FROM gs_db_privileges ORDER BY rolename;

--privileges for grant
RESET ROLE;
GRANT SELECT ANY TABLE TO db_priv_user1 WITH ADMIN OPTION;
GRANT INSERT ANY TABLE TO db_priv_user1 WITH ADMIN OPTION;
GRANT UPDATE ANY TABLE TO db_priv_user1;
GRANT DELETE ANY TABLE TO db_priv_user1;

SET ROLE db_priv_user1 PASSWORD '1234567i*';
GRANT SELECT ANY TABLE,UPDATE ANY TABLE,INSERT ANY TABLE TO db_priv_user2; --failed
GRANT INSERT ANY TABLE,DELETE ANY TABLE TO db_priv_user2; --failed
GRANT SELECT ANY TABLE TO db_priv_user2 WITH ADMIN OPTION;
GRANT INSERT ANY TABLE TO db_priv_user2;
GRANT UPDATE ANY TABLE TO db_priv_user2; --failed
GRANT DELETE ANY TABLE TO db_priv_user2; --failed

SET ROLE db_priv_user2 PASSWORD '1234567i*';
GRANT SELECT ANY TABLE TO db_priv_user3;
GRANT INSERT ANY TABLE TO db_priv_user3; --failed
GRANT UPDATE ANY TABLE TO db_priv_user3; --failed
GRANT DELETE ANY TABLE TO db_priv_user3; --failed

SET ROLE db_priv_user3 PASSWORD '1234567i*';
GRANT SELECT ANY TABLE TO db_priv_user4; --failed
GRANT INSERT ANY TABLE TO db_priv_user4; --failed
GRANT UPDATE ANY TABLE TO db_priv_user4; --failed
GRANT DELETE ANY TABLE TO db_priv_user4; --failed

RESET ROLE;
SELECT * FROM gs_db_privileges ORDER BY rolename;
GRANT db_priv_user2 TO db_priv_user3;
SET ROLE db_priv_user3 PASSWORD '1234567i*';
GRANT SELECT ANY TABLE TO db_priv_user4;
GRANT INSERT ANY TABLE TO db_priv_user4; --failed
GRANT UPDATE ANY TABLE TO db_priv_user4; --failed
GRANT DELETE ANY TABLE TO db_priv_user4; --failed

RESET ROLE;
GRANT db_priv_user3 TO db_priv_user4;
SET ROLE db_priv_user4 PASSWORD '1234567i*';
GRANT SELECT ANY TABLE TO db_priv_user5;
GRANT INSERT ANY TABLE TO db_priv_user5; --failed
GRANT UPDATE ANY TABLE TO db_priv_user5; --failed
GRANT DELETE ANY TABLE TO db_priv_user5; --failed

REVOKE ADMIN OPTION FOR SELECT ANY TABLE FROM db_priv_user2;
GRANT SELECT ANY TABLE TO db_priv_user5;--failed

RESET ROLE;
GRANT db_priv_user1 TO db_priv_user5;
SET ROLE db_priv_user5 PASSWORD '1234567i*';
REVOKE SELECT ANY TABLE FROM db_priv_user1,db_priv_user2,db_priv_user3,db_priv_user4,db_priv_user5;
REVOKE INSERT ANY TABLE FROM db_priv_user1,db_priv_user2,db_priv_user3,db_priv_user4,db_priv_user5;
REVOKE UPDATE ANY TABLE FROM db_priv_user1,db_priv_user2,db_priv_user3,db_priv_user4,db_priv_user5;

--function has_any_privilege
RESET ROLE;
GRANT UPDATE ANY TABLE TO db_priv_user1 WITH ADMIN OPTION;
SELECT * FROM gs_db_privileges ORDER BY rolename, admin_option;

SELECT has_any_privilege('db_priv_user','SELECT ANY TABLE'); --error
SELECT has_any_privilege('db_priv_user1','SELECT ANY   TABLE'); --error
SELECT has_any_privilege('db_priv_user1','SELECT ANY TABLES'); --error

SELECT has_any_privilege('db_priv_user1','UPDATE ANY TABLE WITH ADMIN OPtION'); --t
SELECT has_any_privilege('db_priv_user1','update ANY TABLE WITH ADMIN OPtION'); --t
SELECT has_any_privilege('db_priv_user1','UPDATE ANY TABLE WITH admin OPtION'); --t

SELECT has_any_privilege('db_priv_user1','update ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user1','UPDATE ANY TABLE WITH ADMIN OPTION'); --t
SELECT has_any_privilege('db_priv_user1','DELETE ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user1','DELETE ANY TABLE WITH ADMIN OPTION'); --f
SELECT has_any_privilege('db_priv_user1','CREATE ANY TABLE'); --f
SELECT has_any_privilege('db_priv_user1','CREATE ANY TABLE WITH ADMIN OPTION'); --f

SELECT has_any_privilege('db_priv_user1','SELECT ANY TABLE, DELETE ANY TABLE WITH ADMIN OPTION'); --f
SELECT has_any_privilege('db_priv_user1','SELECT ANY TABLE, UPDATE ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user1','CREATE ANY TABLE WITH ADMIN OPTION, DELETE ANY TABLE'); --t

SELECT has_any_privilege('db_priv_user5','update ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user5','UPDATE ANY TABLE WITH ADMIN OPTION'); --t
SELECT has_any_privilege('db_priv_user5','DELETE ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user5','DELETE ANY TABLE WITH ADMIN OPTION'); --f
SELECT has_any_privilege('db_priv_user5','CREATE ANY TABLE'); --f
SELECT has_any_privilege('db_priv_user5','CREATE ANY TABLE WITH ADMIN OPTION'); --f

SELECT has_any_privilege('db_priv_user5','SELECT ANY TABLE, DELETE ANY TABLE WITH ADMIN OPTION'); --f
SELECT has_any_privilege('db_priv_user5','SELECT ANY TABLE, UPDATE ANY TABLE'); --t
SELECT has_any_privilege('db_priv_user5','CREATE ANY TABLE WITH ADMIN OPTION, DELETE ANY TABLE'); --t

--audit
RESET ROLE;
SELECT type,result,object_name,detail_info from pg_query_audit('2021-11-30','2099-12-28')
  WHERE type='grant_role' AND object_name='db_priv_user0';
SELECT type,result,object_name,detail_info from pg_query_audit('2021-11-30','2099-12-28')
  WHERE type='revoke_role' AND object_name='db_priv_user0';

--clean
RESET ROLE;
DROP USER db_priv_user1 CASCADE;
DROP USER db_priv_user2,db_priv_user3,db_priv_user4,db_priv_user5;
