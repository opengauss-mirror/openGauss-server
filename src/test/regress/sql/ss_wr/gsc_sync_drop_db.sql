create database testdb1;
\c testdb1
\c postgres
select database_name from gs_gsc_dbstat_info() where database_name like '%testdb1%';
drop database testdb1;
