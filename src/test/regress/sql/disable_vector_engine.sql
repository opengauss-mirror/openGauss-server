----
--- Create Talbe
----
CREATE SCHEMA DISABLE_VECTOR_ENGINE;
SET CURRENT_SCHEMA=DISABLE_VECTOR_ENGINE;
SET ENABLE_VECTOR_ENGINE = OFF;

CREATE TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01
(
DEPNAME VARCHAR,
EMPNO BIGINT,
SALARY INT,
ENROLL DATE,
TIMESET TIMETZ)
WITH(ORIENTATION = COLUMN);
CREATE INDEX IDX_TABLE_01 ON DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01(DEPNAME);

CREATE TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02
(
DEPNAME VARCHAR,
SALARY INT,
ENROLL DATE)
WITH(ORIENTATION = COLUMN);

CREATE TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_03
(
DEPNAME VARCHAR,
SALARY INT,
ENROLL DATE)
WITH(ORIENTATION = COLUMN)
PARTITION BY RANGE(SALARY)
(
PARTITION SALARY1 VALUES LESS THAN(2000),
PARTITION SALARY2 VALUES LESS THAN(3000)
);

CREATE TABLE DISABLE_VECTOR_ENGINE.ROW_TABLE_01(DEPNAME VARCHAR, SALARY INT, ENROLL DATE);

-- PartIterator
EXPLAIN (COSTS OFF) SELECT DISTINCT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_03;

-- ModifyTable
EXPLAIN (COSTS OFF) INSERT INTO DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01;
EXPLAIN (COSTS OFF) INSERT INTO DISABLE_VECTOR_ENGINE.ROW_TABLE_01 SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02;

-- Append, Result
EXPLAIN (COSTS OFF) SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 UNION ALL SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01;

-- SetOp
EXPLAIN (COSTS OFF) SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 EXCEPT ALL SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01;

-- Group, Unique, CStore Index Scan 
EXPLAIN (COSTS OFF) SELECT DISTINCT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 GROUP BY DEPNAME;
SET ENABLE_SEQSCAN = OFF;
SET ENABLE_HASHAGG = OFF;
EXPLAIN (COSTS OFF) SELECT DISTINCT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01;
RESET ENABLE_SEQSCAN;
RESET ENABLE_HASHAGG;

-- LockRows
EXPLAIN (COSTS OFF) SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 FOR UPDATE;

-- CteScan


-- HashJoin, MergeJoin, NestLoop, Materialize
EXPLAIN (COSTS OFF) SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 T1, DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02 T2 WHERE T1.DEPNAME=T2.DEPNAME;
SET ENABLE_HASHJOIN = OFF;
EXPLAIN (COSTS OFF) SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 T1, DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02 T2 WHERE T1.DEPNAME=T2.DEPNAME;
SET ENABLE_MERGEJOIN = OFF;
EXPLAIN (COSTS OFF) SELECT * FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 T1, DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02 T2 WHERE T1.DEPNAME=T2.DEPNAME;
RESET ENABLE_HASHJOIN;
RESET ENABLE_MERGEJOIN;

-- MergeAppend
EXPLAIN (COSTS OFF) (SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01 ORDER BY DEPNAME ) UNION ALL (SELECT DEPNAME FROM DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02 ORDER BY DEPNAME) ORDER BY 1;

-- Explain bogus varno bug
CREATE TABLE DISABLE_VECTOR_ENGINE.T1(A INT, B INT);
CREATE TABLE DISABLE_VECTOR_ENGINE.T2(A INT, B INT) ;
CREATE TABLE DISABLE_VECTOR_ENGINE.COL_TABLE(A INT, B INT) WITH (ORIENTATION=COLUMN);

EXPLAIN (COSTS OFF, VERBOSE ON) INSERT INTO DISABLE_VECTOR_ENGINE.COL_TABLE SELECT P.A,P.B FROM DISABLE_VECTOR_ENGINE.T1 P INNER JOIN DISABLE_VECTOR_ENGINE.T2 Q ON( P.A=Q.A);

RESET ENABLE_VECTOR_ENGINE;

-- Agg and Window functions not not yet implemented in Vector Engine, fallback
EXPLAIN (COSTS OFF, VERBOSE ON) SELECT BOOL_AND(SALARY) + 1 FROM VECTOR_TABLE_01;

EXPLAIN (COSTS OFF, VERBOSE ON) SELECT DEPNAME FROM VECTOR_TABLE_01 GROUP BY DEPNAME HAVING STRING_AGG(SALARY,';') IS NOT NULL;

EXPLAIN (COSTS OFF, VERBOSE ON) SELECT DEPNAME FROM VECTOR_TABLE_01 GROUP BY DEPNAME HAVING STRING_AGG(SALARY,';') IS NOT NULL UNION ALL SELECT DEPNAME FROM VECTOR_TABLE_01 GROUP BY DEPNAME HAVING STRING_AGG(SALARY,';') IS NOT NULL;

EXPLAIN (COSTS OFF, VERBOSE ON) SELECT LAST_VALUE(SALARY) OVER ( PARTITION BY DEPNAME ORDER BY SALARY, EMPNO) - 1 FROM VECTOR_TABLE_01;

EXPLAIN (COSTS OFF, VERBOSE ON) SELECT AVG(SALARY) OVER ( PARTITION BY DEPNAME ORDER BY SALARY, EMPNO) - 1 FROM VECTOR_TABLE_01;


-- string_agg, fallback
CREATE TABLE DISABLE_VECTOR_ENGINE.REGION
(
    R_REGIONKEY  INT NOT NULL
  , R_NAME       CHAR(25) NOT NULL
  , R_COMMENT    VARCHAR(152)
)
WITH (ORIENTATION = COLUMN);

EXPLAIN (COSTS OFF, VERBOSE ON)
SELECT
        R_REGIONKEY, 
        SUBSTR(STRING_AGG(R_NAME , ';'), 
        0, 
        LENGTH(STRING_AGG(R_NAME , ';')) - 1) R_NAME, 
        SUBSTR(STRING_AGG(A.R_COMMENT , ';'), 
        0, 
        LENGTH(STRING_AGG(R_COMMENT , ';')) - 1)  R_COMMENT 
FROM (SELECT R_REGIONKEY, 
                REPLACE(R_NAME, CHR(13) || CHR(10),'') R_NAME, 
                R_COMMENT 
        FROM 
        ( 
            SELECT R_REGIONKEY, R_NAME, R_COMMENT , 
                ROW_NUMBER() OVER(PARTITION BY R_REGIONKEY ORDER BY R_REGIONKEY) RN 
            FROM (SELECT R_REGIONKEY, R_NAME, R_COMMENT 
                        FROM REGION
                        GROUP BY R_REGIONKEY, R_NAME, R_COMMENT 
            ) 
        ) 
    WHERE RN < 7) A 
GROUP BY R_REGIONKEY; 

-- Result is a leaf node
CREATE TABLE  DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_01(
  W_NAME  CHAR(10),
  W_STREET_1  CHARACTER VARYING(20),
  W_ZIP  CHAR(9),
  W_ID  INTEGER, PARTIAL CLUSTER KEY(W_NAME))
WITH (ORIENTATION=COLUMN, MAX_BATCHROW= 30700, COMPRESSION = HIGH)

PARTITION BY RANGE (W_ID)
(
  PARTITION FVT_DISTRIBUTE_QUERY_TABLES_01_P1 VALUES LESS THAN (6),
  PARTITION FVT_DISTRIBUTE_QUERY_TABLES_01_P2 VALUES LESS THAN (8),
  PARTITION FVT_DISTRIBUTE_QUERY_TABLES_01_P3 VALUES LESS THAN (MAXVALUE)
);

CREATE TABLE  DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_02(
  C_ID   VARCHAR,
  C_STREET_1   VARCHAR(20),
  C_CITY   TEXT,
  C_ZIP   VARCHAR(9),
  C_D_ID   NUMERIC,
  C_W_ID   TEXT, PARTIAL CLUSTER KEY(C_ID,C_W_ID))
WITH (ORIENTATION=COLUMN, MAX_BATCHROW= 30700, COMPRESSION = HIGH)
;

CREATE TABLE  DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_03(
  D_W_ID   INTEGER,
  D_NAME   CHARACTER VARYING(10),
  D_STREET_2   CHARACTER VARYING(20),
  D_CITY  CHARACTER VARYING(20),
  D_ID   INTEGER)
WITH (ORIENTATION=COLUMN, MAX_BATCHROW= 38700, COMPRESSION = YES)
;

CREATE TABLE  DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_04(
  W_ID   INTEGER,
  W_NAME   VARCHAR(20),
  W_ZIP  INTEGER, PARTIAL CLUSTER KEY(W_ID))
WITH (ORIENTATION=COLUMN)
;

SET ENABLE_HASHJOIN = OFF;
EXPLAIN (COSTS OFF)
SELECT TABLE_01.W_NAME,
       TABLE_02.C_D_ID < 8 T2,
       TABLE_02.C_CITY,
       TABLE_03.D_W_ID,
       TABLE_04.W_NAME
FROM DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_01 AS TABLE_01
FULL OUTER JOIN DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_02 AS TABLE_02
  ON COALESCE(TABLE_01.W_ID, 1) = TABLE_02.C_W_ID
  AND TABLE_02.C_ID NOT IN (10, 11, 12, 18, 39, 80, 88, 99, NULL)
INNER JOIN DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_03 AS TABLE_03
  ON TABLE_01.W_ID = TABLE_03.D_W_ID
FULL OUTER JOIN DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_04 AS TABLE_04
  ON TABLE_04.W_ID = TABLE_03.D_W_ID
WHERE COALESCE(TABLE_03.D_ID, 2) > 1
   AND COALESCE(TABLE_03.D_W_ID, 3) < 9
ORDER BY TABLE_01.W_NAME,
          T2,
          TABLE_02.C_CITY,
          TABLE_03.D_W_ID,
          TABLE_04.W_NAME; 
RESET ENABLE_HASHJOIN;

-- Subquery exists in fromlist of join tree
CREATE TABLE DISABLE_VECTOR_ENGINE.STORE
(
    s_store_sk                INTEGER               not null,
    s_store_id                CHAR(16)              not null,
    s_rec_start_date          DATE                          ,
    s_rec_end_date            DATE                          ,
    s_closed_date_sk          INTEGER                       ,
    s_store_name              VARCHAR(50)                   ,
    s_number_employees        INTEGER                       ,
    s_floor_space             INTEGER                       ,
    s_hours                   CHAR(20)                      ,
    s_manager                 VARCHAR(40)                   ,
    s_market_id               INTEGER                       ,
    s_geography_class         VARCHAR(100)                  ,
    s_market_desc             VARCHAR(100)                  ,
    s_market_manager          VARCHAR(40)                   ,
    s_division_id             INTEGER                       ,
    s_division_name           VARCHAR(50)                   ,
    s_company_id              INTEGER                       ,
    s_company_name            VARCHAR(50)                   ,
    s_street_number           VARCHAR(10)                   ,
    s_street_name             VARCHAR(60)                   ,
    s_street_type             CHAR(15)                      ,
    s_suite_number            CHAR(10)                      ,
    s_city                    VARCHAR(60)                   ,
    s_county                  VARCHAR(30)                   ,
    s_state                   CHAR(2)                       ,
    s_zip                     CHAR(10)                      ,
    s_country                 VARCHAR(20)                   ,
    s_gmt_offset              DECIMAL(5,2)                  ,
    s_tax_precentage          DECIMAL(5,2)
)
WITH (ORIENTATION = COLUMN)
;
  
CREATE TABLE DISABLE_VECTOR_ENGINE.CUSTOMER
(
    c_customer_sk             INTEGER               NOT NULL,
    c_customer_id             CHAR(16)              NOT NULL,
    c_current_cdemo_sk        INTEGER                       ,
    c_current_hdemo_sk        INTEGER                       ,
    c_current_addr_sk         INTEGER                       ,
    c_first_shipto_date_sk    INTEGER                       ,
    c_first_sales_date_sk     INTEGER                       ,
    c_salutation              CHAR(10)                      ,
    c_first_name              CHAR(20)                      ,
    c_last_name               CHAR(30)                      ,
    c_preferred_cust_flag     CHAR(1)                       ,
    c_birth_day               INTEGER                       ,
    c_birth_month             INTEGER                       ,
    c_birth_year              INTEGER                       ,
    c_birth_country           VARCHAR(20)                   ,
    c_login                   CHAR(13)                      ,
    c_email_address           CHAR(50)                      ,
    c_last_review_date        CHAR(10)
)
WITH (ORIENTATION = COLUMN)
;

EXPLAIN (COSTS OFF)
SELECT COUNT(8)
  FROM (SELECT c_birth_day,
               c_birth_month,
               LPAD(LOWER(c_customer_id), 20, '-')
          FROM customer
         WHERE c_birth_day%2 = 0
           AND ('1999-03-13', c_birth_day, c_customer_sk, 5,
                COALESCE(c_birth_month, 9)) <= ANY
         (SELECT s_rec_end_date,
                       s_market_id,
                       s_closed_date_sk,
                       TRUNC(ABS(s_gmt_offset)),
                       LEAD(s_store_sk) over a
                  FROM store window a AS(PARTITION BY s_rec_end_date, s_market_id ORDER BY s_rec_end_date, s_market_id, s_closed_date_sk, s_gmt_offset))
           AND c_birth_country LIKE '%J%'
           AND c_last_name LIKE '%a%'
         GROUP BY 3, 2, 1
         ORDER BY 1, 2, 3 DESC);

-- unsupported feature in ctelist
EXPLAIN (COSTS OFF)
INSERT INTO store(s_store_sk)
WITH v1 as materialized (
	SELECT c_customer_sk,
	SUM(c_customer_sk) over
		(PARTITION BY c_customer_sk, c_customer_id)
		avg_customer_sk,
	STDDEV_SAMP(c_customer_sk) stdev
	FROM customer
	GROUP BY c_customer_sk, c_customer_id),
	v2 as materialized (
	SELECT v1.c_customer_sk FROM v1)
	SELECT * FROM v2;

EXPLAIN (COSTS OFF)
INSERT INTO store(s_store_sk)
WITH v1 as(
	SELECT c_customer_sk,
	SUM(c_customer_sk) over
		(PARTITION BY c_customer_sk, c_customer_id)
		avg_customer_sk,
	STDDEV_SAMP(c_customer_sk) stdev
	FROM customer
	GROUP BY c_customer_sk, c_customer_id),
	v2 as(
	SELECT v1.c_customer_sk FROM v1)
	SELECT * FROM v2;


explain (costs off)
INSERT INTO store(s_store_sk)
WITH v1 as materialized (
        SELECT c_customer_sk,
        SUM(c_customer_sk) over
                (PARTITION BY c_customer_sk, c_customer_id)
                avg_customer_sk,
        STDDEV_pop(c_customer_sk) stdev
        FROM customer
        GROUP BY c_customer_sk, c_customer_id),
        v2 as materialized (
        SELECT v1.c_customer_sk FROM v1)
        SELECT * FROM v2;

explain (costs off)
INSERT INTO store(s_store_sk)
WITH v1 as(
        SELECT c_customer_sk,
        SUM(c_customer_sk) over
                (PARTITION BY c_customer_sk, c_customer_id)
                avg_customer_sk,
        STDDEV_pop(c_customer_sk) stdev
        FROM customer
        GROUP BY c_customer_sk, c_customer_id),
        v2 as(
        SELECT v1.c_customer_sk FROM v1)
        SELECT * FROM v2;

----
--- Drop Tables
----
DROP TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_01;
DROP TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_02;
DROP TABLE DISABLE_VECTOR_ENGINE.VECTOR_TABLE_03;
DROP TABLE DISABLE_VECTOR_ENGINE.ROW_TABLE_01;
DROP TABLE DISABLE_VECTOR_ENGINE.REGION;
DROP TABLE DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_01;
DROP TABLE DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_02;
DROP TABLE DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_03;
DROP TABLE DISABLE_VECTOR_ENGINE.FVT_DISTRIBUTE_QUERY_TABLES_04;
DROP TABLE DISABLE_VECTOR_ENGINE.STORE;
DROP TABLE DISABLE_VECTOR_ENGINE.CUSTOMER;
DROP SCHEMA DISABLE_VECTOR_ENGINE CASCADE;
