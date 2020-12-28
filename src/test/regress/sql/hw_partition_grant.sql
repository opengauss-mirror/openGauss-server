-- create a range partitioned table
create table rangePartTable_grant (c1 int, c2 float, c3 real, c4 text) 
partition by range (c1, c2, c3, c4)
(
	partition 	rangePartTable_grant_p1 values less than (10, 10.00, 19.156, 'h'),
	partition 	rangePartTable_grant_p2 values less than (20, 20.89, 23.75, 'k'),
	partition 	rangePartTable_grant_p3 values less than (30, 30.45, 32.706, 's')
);
create   index  index_rangePartTable_grant_local1  on rangePartTable_grant  (c1, c2)  local
(
	partition 	p1_index_local_rangePartTable_grant_local1 tablespace PG_DEFAULT,
    partition   p2_index_local_rangePartTable_grant_local1 tablespace PG_DEFAULT,
   	partition   p3_index_local_rangePartTable_grant_local1 tablespace PG_DEFAULT
); 


-- create a interval partitioned table
create table intervalPartTable_grant (c1 timestamp, c2 float, c3 real, c4 text) 
partition by range (c1)
(
	partition 	intervalPartTable_grant_p1 values less than ('2012-01-01'),
	partition 	intervalPartTable_grant_p2 values less than ('2012-02-01'),
	partition 	intervalPartTable_grant_p3 values less than ('2012-03-01')
);
create   index  index_intervalPartTable_local1  on intervalPartTable_grant  (c1, c2)  local
(
	partition 	p1_index_intervalPartTable_grant_local1 tablespace PG_DEFAULT,
    partition   p2_index_intervalPartTable_grant_local1 tablespace PG_DEFAULT,
   	partition   p3_index_intervalPartTable_grant_local1 tablespace PG_DEFAULT
); 

--  aliase of CREATE ROLE: create user for table level
CREATE USER role_table_all PASSWORD 'gauss@123';
CREATE USER role_table_insert PASSWORD 'gauss@123';
CREATE USER role_table_delete PASSWORD 'gauss@123';
CREATE USER role_table_update PASSWORD 'gauss@123';
CREATE USER role_table_select PASSWORD 'gauss@123';
CREATE USER role_table_truncate PASSWORD 'gauss@123';
CREATE USER role_table_references PASSWORD 'gauss@123';
CREATE USER role_table_trigger PASSWORD 'gauss@123';

-- CREATE ROLE aliase : create user for column level
CREATE USER role_column_all PASSWORD 'gauss@123';
CREATE USER role_column_insert PASSWORD 'gauss@123';
CREATE USER role_column_update PASSWORD 'gauss@123';
CREATE USER role_column_select PASSWORD 'gauss@123';
CREATE USER role_column_references PASSWORD 'gauss@123';

-- range partitioned table: GRANT PRIVILEGES to role on table level
GRANT ALL PRIVILEGES 
	ON TABLE rangePartTable_grant
	TO role_table_all;

GRANT SELECT 
	ON TABLE rangePartTable_grant
	TO role_table_select;

GRANT INSERT 
	ON TABLE rangePartTable_grant
	TO role_table_insert;	

GRANT UPDATE 
	ON TABLE rangePartTable_grant
	TO role_table_update;
GRANT DELETE 
	ON TABLE rangePartTable_grant
	TO role_table_delete;
	
GRANT TRUNCATE 
	ON TABLE rangePartTable_grant
	TO role_table_truncate;
	
GRANT REFERENCES 
	ON TABLE rangePartTable_grant
	TO role_table_references;
	
GRANT TRIGGER
	ON TABLE rangePartTable_grant
	TO role_table_trigger;

-- range partitioned table: GRANT PRIVILEGES to role on column level
GRANT ALL PRIVILEGES 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	TO role_column_all;
	
GRANT INSERT 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	TO role_column_insert;

GRANT UPDATE 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	TO role_column_update;

GRANT SELECT 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	TO role_column_select;
	
GRANT REFERENCES 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	TO role_column_references;

SET ROLE role_table_select PASSWORD 'gauss@123';
insert into rangePartTable_grant values (8, 8.00, 8.156, 'd');
RESET ROLE;

SET ROLE role_table_insert PASSWORD 'gauss@123';
insert into rangePartTable_grant values (8, 8.00, 8.156, 'd');
RESET ROLE;

SET ROLE role_table_select PASSWORD 'gauss@123';
insert into rangePartTable_grant values (8, 8.00, 8.156, 'd');
RESET ROLE;

SET ROLE role_table_select PASSWORD 'gauss@123';
select * from rangePartTable_grant;
RESET ROLE;

-- interval partitioned table: GRANT PRIVILEGES to role on table level
GRANT ALL PRIVILEGES 
	ON TABLE intervalPartTable_grant
	TO role_table_all;

GRANT SELECT 
	ON TABLE intervalPartTable_grant
	TO role_table_select;

GRANT INSERT 
	ON TABLE intervalPartTable_grant
	TO role_table_insert;	

GRANT UPDATE 
	ON TABLE intervalPartTable_grant
	TO role_table_update;
GRANT DELETE 
	ON TABLE intervalPartTable_grant
	TO role_table_delete;
	
GRANT TRUNCATE 
	ON TABLE intervalPartTable_grant
	TO role_table_truncate;
	
GRANT REFERENCES 
	ON TABLE intervalPartTable_grant
	TO role_table_references;
	
GRANT TRIGGER
	ON TABLE intervalPartTable_grant
	TO role_table_trigger;

-- interval partitioned table: GRANT PRIVILEGES to role on column level
GRANT ALL PRIVILEGES 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	TO role_column_all;
	
GRANT INSERT 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	TO role_column_insert;

GRANT UPDATE 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	TO role_column_update;

GRANT SELECT 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	TO role_column_select;
	
GRANT REFERENCES 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	TO role_column_references;

-- revoke table level privileges
REVOKE ALL PRIVILEGES 
	ON rangePartTable_grant
	FROM role_table_all;
REVOKE ALL PRIVILEGES 
	ON intervalPartTable_grant
	FROM role_table_all;
REVOKE INSERT 
	ON rangePartTable_grant
	FROM role_table_insert;
REVOKE INSERT
	ON intervalPartTable_grant
	FROM role_table_insert;
REVOKE UPDATE 
	ON rangePartTable_grant
	FROM role_table_update;
REVOKE UPDATE
	ON intervalPartTable_grant
	FROM role_table_update;
REVOKE SELECT 
	ON rangePartTable_grant
	FROM role_table_select;
REVOKE SELECT
	ON intervalPartTable_grant
	FROM role_table_select;
REVOKE DELETE 
	ON rangePartTable_grant
	FROM role_table_delete;
REVOKE DELETE
	ON intervalPartTable_grant
	FROM role_table_delete;
REVOKE TRUNCATE 
	ON rangePartTable_grant
	FROM role_table_truncate;
REVOKE TRUNCATE
	ON intervalPartTable_grant
	FROM role_table_truncate;
REVOKE REFERENCES 
	ON rangePartTable_grant
	FROM role_table_references;
REVOKE REFERENCES
	ON intervalPartTable_grant
	FROM role_table_references;
REVOKE TRIGGER 
	ON rangePartTable_grant
	FROM role_table_trigger;
REVOKE TRIGGER
	ON intervalPartTable_grant
	FROM role_table_trigger;

-- revoke column level privileges
REVOKE ALL PRIVILEGES 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	FROM role_column_all;
	
REVOKE INSERT 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	FROM role_column_insert;

REVOKE UPDATE 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	FROM role_column_update;

REVOKE SELECT 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	FROM role_column_select;
	
REVOKE REFERENCES 
	(c1, c2, c3, c4)
	ON TABLE rangePartTable_grant
	FROM role_column_references;
	
REVOKE ALL PRIVILEGES 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	FROM role_column_all;
	
REVOKE INSERT 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	FROM role_column_insert;

REVOKE UPDATE 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	FROM role_column_update;

REVOKE SELECT 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	FROM role_column_select;
	
REVOKE REFERENCES 
	(c1, c2, c3, c4)
	ON TABLE intervalPartTable_grant
	FROM role_column_references;
-- cleanup
drop schema role_table_all;

drop role role_table_all;
drop schema role_table_insert;
drop role role_table_insert;
drop schema role_table_update;
drop role role_table_update;
drop schema role_table_select;
drop role role_table_select;
drop schema role_table_delete;
drop role role_table_delete;
drop schema role_table_truncate;
drop role role_table_truncate;
drop schema role_table_references;
drop role role_table_references;
drop schema role_table_trigger;
drop role role_table_trigger;

drop schema role_column_all;
drop role role_column_all;
drop schema role_column_insert;
drop role role_column_insert;
drop schema role_column_update;
drop role role_column_update;
drop schema role_column_select;
drop role role_column_select;
drop schema role_column_references;
drop role role_column_references;

drop table rangePartTable_grant cascade;
drop table intervalPartTable_grant cascade;