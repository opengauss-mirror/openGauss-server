set client_min_messages=FATAL;
CREATE ROLE account1 IDENTIFIED BY '1q2w3e4r!';
CREATE ROLE account2 IDENTIFIED BY '1q2w3e4r@';

SET ROLE account2;
SET ROLE account3;
SET ROLE account2 PASSWORD '1q2w3e4r#';
SET ROLE account2 PASSWORD '1q2w3e4r@';
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r#' REPLACE '1q2w3e4r@';
\c
set client_min_messages=FATAL;
SET ROLE account2 PASSWORD '1q2w3e4r@';
SET ROLE account1 PASSWORD '1q2w3e4r!';
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r#' REPLACE '1q2w3e4r@';
\c
set client_min_messages=FATAL;
ALTER ROLE account2 ACCOUNT UNLOCK;
ALTER ROLE account2 ACCOUNT LOCK;
SET ROLE account2 PASSWORD '1q2w3e4r#';
SET ROLE account2 PASSWORD '1q2w3e4r@';
SET ROLE account2;
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r#' REPLACE '1q2w3e4r@';
\c
set client_min_messages=FATAL;
ALTER ROLE account2 ACCOUNT UNLOCK;
SET ROLE account2 PASSWORD '1q2w3e4r#';
SET ROLE account1 PASSWORD '1q2w3e4r!';
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r@' REPLACE '1q2w3e4r@';
\c
set client_min_messages=FATAL;
SET ROLE account1 PASSWORD '1q2w3e4r#';
SET ROLE account1 PASSWORD '1q2w3e4r#';
DROP ROLE account1;
DROP ROLE account2;
create role adminuser1 with sysadmin password 'Ttest@123';
create role adminuser2 with sysadmin password 'Ttest@123';

--test DropUserStatus
\c
set client_min_messages=FATAL;
set role adminuser1 password 'Ttest@123';
alter role adminuser2 account lock;
reset role;
alter role adminuser2 account unlock;
alter role adminuser2 account lock;
drop user adminuser1;
drop user adminuser2;

create user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_0 with createrole password 'Ttest@123';
create user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_4 with createrole password 'Ttest@123';
create user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_6 password 'Ttest@123';
set role USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_0 password 'Ttest@123';
alter role USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_4 account lock;
alter role USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_6 account lock;
\c
set client_min_messages=FATAL;
drop user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_0;
drop user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_4;
drop user USER_ACCOUNT_ADMIN_MAN_LOCK_UNLOCK_SEPDUTY_OFF_005_6;
reset client_min_messages;

-- test describe role
create user "AAAAaaaaaaaa1_desc" with sysadmin valid begin '1111-1-1' valid until '2222-2-2' password 'Ttest@123';
select rolname,rolvalidbegin,rolvaliduntil,rolsystemadmin from pg_roles where rolname like 'AAAAaaaaaaaa%' order by 1;
create user "AAAAaaaaaaaa2_desc" with createdb login auditadmin valid until '2242-2-22' password 'Ttest@123';
select rolname,rolvalidbegin,rolvaliduntil,rolsystemadmin,rolauditadmin,rolcreatedb from pg_roles where rolname like 'AAAAaaaaaaaa%' order by 1;
drop user "AAAAaaaaaaaa1_desc";
drop user "AAAAaaaaaaaa2_desc";
