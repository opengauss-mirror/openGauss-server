/*
 * This file is used to test the function of ExecVecMaterial()
 */
----
--- Create Table and Insert Data
----
create schema vector_material_engine_001;
set current_schema=vector_material_engine_001;
SET TIME ZONE 'PRC';set datestyle to iso;
SET ENABLE_HASHJOIN=FALSE;
SET ENABLE_NESTLOOP=FALSE;
SET log_min_messages=debug1;
CREATE TABLE vector_material_engine_001.ROW_MATERIAL_TABLE_01(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE 
); 
 
 CREATE TABLE vector_material_engine_001.VECTOR_MATERIAL_TABLE_01(
 C_CHAR_1 CHAR(1),
 C_CHAR_2 CHAR(10),
 C_CHAR_3 CHAR(100),
 C_VARCHAR_1 VARCHAR(1),
 C_VARCHAR_2 VARCHAR(10),
 C_VARCHAR_3 VARCHAR(1024),
 C_INT BIGINT,
 C_BIGINT BIGINT,
 C_SMALLINT BIGINT,
 C_FLOAT FLOAT,
 C_NUMERIC numeric(20,5),
 C_DP double precision,
 C_DATE DATE,
 C_TS_WITHOUT TIMESTAMP WITHOUT TIME ZONE,
 C_TS_WITH TIMESTAMP WITH TIME ZONE , PARTIAL CLUSTER KEY(C_INT)
)WITH (ORIENTATION=COLUMN) ; 

CREATE TABLE vector_material_engine_001.ROW_MATERIAL_TABLE_02(
   C_INT INTEGER
);

CREATE TABLE vector_material_engine_001.VECTOR_MATERIAL_TABLE_02(
   C_INT INTEGER, 
   PARTIAL CLUSTER KEY(C_INT)
)WITH (ORIENTATION=COLUMN) ;

CREATE TABLE vector_material_engine_001.ROW_MATERIAL_TABLE_03(
   C_INT INTEGER
  ,C_CHAR_2 VARCHAR(1024)
);

CREATE TABLE vector_material_engine_001.VECTOR_MATERIAL_TABLE_03(
   C_INT INTEGER
  ,C_CHAR_2 VARCHAR(1024)
  ,PARTIAL CLUSTER KEY(C_INT,C_CHAR_2)
)WITH (ORIENTATION=COLUMN);

CREATE INDEX MATERIAL_INDEX_023_1 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_01(C_CHAR_1,C_CHAR_2,C_CHAR_3,C_VARCHAR_1,C_VARCHAR_2,C_VARCHAR_3,C_INT,C_BIGINT,C_SMALLINT,C_FLOAT,C_NUMERIC,C_DP,C_DATE,C_TS_WITHOUT,C_TS_WITH);
CREATE INDEX MATERIAL_INDEX_023_2 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_02(C_INT);
CREATE INDEX MATERIAL_INDEX_023_3 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_03(C_INT,C_CHAR_2);

CREATE TABLE src(a int);
INSERT INTO src VALUES (1);
INSERT INTO vector_material_engine_001.ROW_MATERIAL_TABLE_01 SELECT 'D', 'text_'|| generate_series(0, 98), 'VARCHAR_'||generate_series(0, 98),'d', 'textDA_'||generate_series(0, 98),'VARC_'||generate_series(0, 98),generate_series(0, 98),10000+generate_series(0, 98),generate_series(0, 98),1.012+generate_series(0, 98),2.01+generate_series(0, 98),3.01+generate_series(0, 98),'2010-10-10','2018-01-01 15:59:59','2030-10-01 15:59:59' FROM src;

insert into VECTOR_MATERIAL_TABLE_01 select * from row_material_table_01;

INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 SELECT generate_series(98, 150) FROM src;

insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_02;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_02;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_02;

INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'nowbegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL0');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'rightbegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL1');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'keybegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL2');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'truebegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL3');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'laterbegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL4');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'falsebegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL5');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'howbegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL6');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'keybegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL7');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'phonebegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL8');
INSERT INTO vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 VALUES(36,'timebegincostEXECVECMATERIALALLINCASEINEXECVECMATERIALALLEXECVECMATERIALALLEXECVECMATERIALALL9');
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_03;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_03;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_03;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_03;
insert into vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 select * from vector_material_engine_001.VECTOR_MATERIAL_TABLE_03;


----
--- case 1: Execute On Disk
----
set work_mem=64;
SELECT * FROM vector_material_engine_001.VECTOR_MATERIAL_TABLE_01 JOIN vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_02.C_INT=98 JOIN vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_03.C_INT=36 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 asc limit 10;
reset work_mem;

----
--- case 2: Execute In Memory
----
SELECT * FROM vector_material_engine_001.VECTOR_MATERIAL_TABLE_01 JOIN vector_material_engine_001.VECTOR_MATERIAL_TABLE_02 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_02.C_INT=98 JOIN vector_material_engine_001.VECTOR_MATERIAL_TABLE_03 ON vector_material_engine_001.VECTOR_MATERIAL_TABLE_03.C_INT=36 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18 asc limit 10;

----
---  case 3: coltpch03 for testing vecmaterialrescan
----
set current_schema=vector_engine;
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = 'BUILDING'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < '1995-03-15'::date
        and l_shipdate > '1995-03-15'::date
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate
limit 10
;

RESET log_min_messages;
drop schema vector_material_engine_001 cascade;
