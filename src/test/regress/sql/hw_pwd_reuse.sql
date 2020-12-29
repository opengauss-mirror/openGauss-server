
CREATE ROLE pwd_reuse_account1 IDENTIFIED BY '1q2w3e4r!';
CREATE ROLE account2 IDENTIFIED BY '1q2w3e4r@';

SET ROLE account2 PASSWORD '1q2w3e4r@';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r#' REPLACE '1q2w3e4r@';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r$' REPLACE '1q2w3e4r#';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r%' REPLACE '1q2w3e4r$';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r^' REPLACE '1q2w3e4r%';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r&' REPLACE '1q2w3e4r^';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r*' REPLACE '1q2w3e4r&';
select pg_sleep(0.0001);
ALTER ROLE account2 IDENTIFIED BY '1q2w3e4r@' REPLACE '1q2w3e4r*';

\c
DROP ROLE pwd_reuse_account1;
DROP ROLE account2;

CREATE ROLE test_pwd_role IDENTIFIED BY "Gauss@123";
drop role test_pwd_role;
CREATE ROLE test_pwd_role password "Gauss@123";
drop role test_pwd_role;
CREATE user test_pwd_user IDENTIFIED BY "Gauss@123";
drop user test_pwd_user;
CREATE user test_pwd_user password "Gauss@123";
drop user test_pwd_user;
