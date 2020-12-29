-- Tests the new reloption- table_access_method added while creating a new table.
-- Creates table with different options(HEAP, USTORE, HBUCKT)
-- 1. No option
-- CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool);
-- 2. HEAP
--  CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool)  WITH (TABLE_ACCESS_METHOD = HEAP);
-- 3. USTORE
-- CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool)  WITH (TABLE_ACCESS_METHOD = USTORE);
-- 4.HBUCKT
-- CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool)  WITH (TABLE_ACCESS_METHOD = HBUCKT);
-- 5. INVALID option
-- CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool)  WITH (TABLE_ACCESS_METHOD = INVALID);

-- 1. No option, defaults to HEAP TABLE
CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool);

INSERT INTO RELOPTIONS_TBL VALUES ('a', true);

INSERT INTO RELOPTIONS_TBL VALUES ('A', false);

-- any of the following three input formats are acceptable
INSERT INTO RELOPTIONS_TBL VALUES ('1', true);

INSERT INTO RELOPTIONS_TBL VALUES (2, false);

INSERT INTO RELOPTIONS_TBL VALUES ('3', true);

-- zero-length char
INSERT INTO RELOPTIONS_TBL VALUES ('', true);

-- try char's of greater than 1 length
INSERT INTO RELOPTIONS_TBL VALUES ('cd', false);
INSERT INTO RELOPTIONS_TBL VALUES ('c     ', true);

SELECT * FROM RELOPTIONS_TBL ORDER BY C1;

-- fetch the reloptions for this table from pg_class
SELECT reloptions FROM pg_class WHERE relname = 'reloptions_tbl';

DROP TABLE RELOPTIONS_TBL;

-- 2. HEAP OPTION
CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool) WITH (TABLE_ACCESS_METHOD = HEAP);

INSERT INTO RELOPTIONS_TBL VALUES ('a', true);

INSERT INTO RELOPTIONS_TBL VALUES ('A', false);

-- any of the following three input formats are acceptable
INSERT INTO RELOPTIONS_TBL VALUES ('1', true);

INSERT INTO RELOPTIONS_TBL VALUES (2, false);

INSERT INTO RELOPTIONS_TBL VALUES ('3', true);

-- zero-length char
INSERT INTO RELOPTIONS_TBL VALUES ('', true);

-- try char's of greater than 1 length
INSERT INTO RELOPTIONS_TBL VALUES ('cd', false);
INSERT INTO RELOPTIONS_TBL VALUES ('c     ', true);

SELECT * FROM RELOPTIONS_TBL ORDER BY C1;

-- fetch the reloptions for this table from pg_class, 'table_access_method = heap' should be visible in the result.
SELECT reloptions FROM pg_class WHERE relname = 'reloptions_tbl';

DROP TABLE RELOPTIONS_TBL;

-- 2. USTORE OPTION
--  This is limited to only creation of table and verifying the reloptions from the pg_class, can't insert or select
--   as related API are not implemented yet.
CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool) WITH (TABLE_ACCESS_METHOD = USTORE);

-- fetch the reloptions for this table from pg_class, 'table_access_method = ustore' should be visible in the result.
SELECT reloptions FROM pg_class WHERE relname = 'reloptions_tbl';

DROP TABLE RELOPTIONS_TBL;

-- 3. HBUCKT OPTION
--  This is limited to only creation of table and verifying the reloptions from the pg_class, can't insert or select
--   as related API are not implemented yet.
CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool) WITH (TABLE_ACCESS_METHOD =  HBUCKT);

-- fetch the reloptions for this table from pg_class, 'table_access_method = hbuckt' should be visible in the result.
SELECT reloptions FROM pg_class WHERE relname = 'reloptions_tbl';

DROP TABLE RELOPTIONS_TBL;

-- 4. INVALID OPTION
--  This should throw an error while table creation as specified option is not valid!
CREATE TABLE RELOPTIONS_TBL(C1 char, B1 bool) WITH (TABLE_ACCESS_METHOD =  INVALID);

 
