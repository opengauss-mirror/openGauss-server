CREATE TABLESPACE gs_basebackup_tablespace relative LOCATION 'gs_basebackup_tablespace';
CREATE TABLE tablespace_table_test(a int) TABLESPACE gs_basebackup_tablespace;
INSERT INTO tablespace_table_test VALUES(1);