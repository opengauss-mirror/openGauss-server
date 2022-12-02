--
-- 处理ceil函数返回-0问题
--

create database ceil_nzero_test DBCOMPATIBILITY 'A';
\c ceil_nzero_test
select ceil(tan(-0.5));
\c postgres
drop database ceil_nzero_test;

create database ceil_nzero_test DBCOMPATIBILITY 'B';
\c ceil_nzero_test
select ceil(tan(-0.5));
\c postgres
drop database ceil_nzero_test;

create database ceil_nzero_test DBCOMPATIBILITY 'C';
\c ceil_nzero_test
select ceil(tan(-0.5));
\c postgres
drop database ceil_nzero_test;

create database ceil_nzero_test DBCOMPATIBILITY 'PG';
\c ceil_nzero_test
select ceil(tan(-0.5));
\c postgres
drop database ceil_nzero_test;
