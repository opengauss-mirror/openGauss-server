create database large_object_test_db;
\c large_object_test_db
SELECT lo_create(100);
CREATE USER u1 with password 'qwer@1234';
CREATE USER u2 with password 'qwer@1234';
GRANT SELECT ON LARGE OBJECT 100 to u1;
SET SESSION AUTHORIZATION u1 PASSWORD 'qwer@1234';
SELECT SESSION_USER, CURRENT_USER;
select lo_open(100, CAST(x'20000' | x'40000' AS integer));
select lo_open(100, CAST(x'40000' AS integer));
SET SESSION AUTHORIZATION u2 PASSWORD 'qwer@1234';
SELECT SESSION_USER, CURRENT_USER;
select lo_open(100, CAST(x'20000' | x'40000' AS integer));
select lo_open(100, CAST(x'40000' AS integer));

\c regression
reset session AUTHORIZATION;
SELECT SESSION_USER, CURRENT_USER;
drop database large_object_test_db;
drop user u1;
drop user u2;

