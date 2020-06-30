
--
-- FLOAT4
--

CREATE FOREIGN TABLE FLOAT4_TBL (i serial primary key, f1  float4) SERVER mot_server ;

INSERT INTO FLOAT4_TBL(f1) VALUES ('    0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1004.30   ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     -34.84    ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e+20');
INSERT INTO FLOAT4_TBL(f1) VALUES ('1.2345678901234e-20');

-- test for over and under flow
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('10e-70');
INSERT INTO FLOAT4_TBL(f1) VALUES ('-10e-70');

-- bad input
INSERT INTO FLOAT4_TBL(f1) VALUES ('');
INSERT INTO FLOAT4_TBL(f1) VALUES ('       ');
INSERT INTO FLOAT4_TBL(f1) VALUES ('xyz');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.0.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5 . 0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('5.   0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('     - 3.0');
INSERT INTO FLOAT4_TBL(f1) VALUES ('123            5');

-- special inputs
SELECT 'NaN'::float4;
SELECT 'nan'::float4;
SELECT '   NAN  '::float4;
SELECT 'infinity'::float4;
SELECT '          -INFINiTY   '::float4;
-- bad special inputs
SELECT 'N A N'::float4;
SELECT 'NaN x'::float4;
SELECT ' INFINITY    x'::float4;

SELECT 'Infinity'::float4 + 100.0;
SELECT 'Infinity'::float4 / 'Infinity'::float4;
SELECT 'nan'::float4 / 'nan'::float4;
SELECT 'nan'::numeric::float4;

SELECT '' AS five, f1 FROM FLOAT4_TBL ORDER BY f1;

SELECT '' AS four, f.f1 FROM FLOAT4_TBL f WHERE f.f1 <> '1004.3' ORDER BY f1;

SELECT '' AS one, f.f1 FROM FLOAT4_TBL f WHERE f.f1 = '1004.3';

SELECT '' AS three, f.f1 FROM FLOAT4_TBL f WHERE '1004.3' > f.f1 ORDER BY f1;

SELECT '' AS three, f.f1 FROM FLOAT4_TBL f WHERE  f.f1 < '1004.3' ORDER BY f1;

SELECT '' AS four, f.f1 FROM FLOAT4_TBL f WHERE '1004.3' >= f.f1 ORDER BY f1;

SELECT '' AS four, f.f1 FROM FLOAT4_TBL f WHERE  f.f1 <= '1004.3' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 * '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 + '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 / '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

SELECT '' AS three, f.f1, f.f1 - '-10' AS x FROM FLOAT4_TBL f
   WHERE f.f1 > '0.0' ORDER BY f1;

-- test divide by zero
SELECT '' AS bad, f.f1 / '0.0' from FLOAT4_TBL f;

SELECT '' AS five, f1 FROM FLOAT4_TBL ORDER BY f1;

-- test the unary float4abs operator 
SELECT '' AS five, f.f1, @f.f1 AS abs_f1 FROM FLOAT4_TBL f ORDER BY f1;

UPDATE FLOAT4_TBL
   SET f1 = FLOAT4_TBL.f1 * '-1'
   WHERE FLOAT4_TBL.f1 > '0.0';

SELECT '' AS five, f1 FROM FLOAT4_TBL ORDER BY f1;

--test overstep the boundary float
CREATE FOREIGN TABLE overstep_float_tbl (f1  float4) SERVER mot_server ;
INSERT INTO overstep_float_tbl(f1) VALUES ('1.2345678901234e+20'); 
INSERT INTO overstep_float_tbl(f1) VALUES ('1.2345678901234e-20');
INSERT INTO overstep_float_tbl(f1) VALUES (1.2345678901234e+20);  
INSERT INTO overstep_float_tbl(f1) VALUES (1.2345678901234e-20);
SELECT f1, 
    case f1 
	WHEN cast('1.2345678901234e+20' as float4) THEN '+20FLOAT4' 
	when cast('1.2345678901234e-20' as float4) THEN '-20FLOAT4'
	ELSE 'others' END AS CASE_BINARY_DOUBLE 
	FROM overstep_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, 
    case f1 
	WHEN cast(1.2345678901234e+20 as float4) THEN '+20FLOAT4' 
	when cast(1.2345678901234e-20 as float4) THEN '-20FLOAT4'
	ELSE 'others' END AS CASE_BINARY_DOUBLE 
	FROM overstep_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

CREATE FOREIGN TABLE CASE_TABLE_39(COL_NAME varchar2(20),col_popu binary_double) SERVER mot_server ;
INSERT INTO CASE_TABLE_39 VALUES('num_1',999.1239567890123956);
SELECT COL_POPU,
            CASE COL_POPU
            WHEN cast(999.1239567890123956 as BINARY_DOUBLE) THEN 'num'
            ELSE 'others'
       END AS CASE_BINARY_DOUBLE
FROM CASE_TABLE_39 ORDER BY COL_POPU,CASE_BINARY_DOUBLE;

SELECT f1, 
    case f1 
	WHEN cast('1.2345678901234e+20' as float4) THEN '+20FLOAT4' 
	when cast('1.2345678901234e-20' as float4) THEN '-20FLOAT4'
	ELSE 'others' END AS CASE_BINARY_DOUBLE 
	FROM overstep_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, 
    case f1 
	WHEN cast(1.2345678901234e+20 as float4) THEN '+20FLOAT4' 
	when cast(1.2345678901234e-20 as float4) THEN '-20FLOAT4'
	ELSE 'others' END AS CASE_BINARY_DOUBLE 
	FROM overstep_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
SELECT COL_POPU,
            CASE COL_POPU
            WHEN cast(999.1239567890123956 as BINARY_DOUBLE) THEN 'num'
            ELSE 'others'
       END AS CASE_BINARY_DOUBLE 
from CASE_TABLE_39  ORDER BY COL_POPU,CASE_BINARY_DOUBLE;

DROP FOREIGN TABLE overstep_float_tbl;
DROP FOREIGN TABLE CASE_TABLE_39;

CREATE FOREIGN TABLE over_float_tbl (f1  float8) SERVER mot_server ;
INSERT INTO over_float_tbl(f1) VALUES (1.234e+20); 
INSERT INTO over_float_tbl(f1) VALUES ('1.234e+20'); 
INSERT INTO over_float_tbl(f1) VALUES (1.234e-20); 
INSERT INTO over_float_tbl(f1) VALUES ('1.234e-20'); 

SELECT f1, case f1 WHEN cast(1.234e+20 as float8) THEN '+short_num' when cast(1.234e-20 as float8) THEN '-short_num' 
             ELSE 'others'
        END AS CASE_BINARY_DOUBLE
FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
SELECT f1, case f1 WHEN cast(1.234e+20 as float8) THEN '+short_num' when cast(1.234e-20 as float8) THEN '-short_num' 
             ELSE 'others'
        END AS CASE_BINARY_DOUBLE
FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

delete from over_float_tbl;
insert into over_float_tbl(f1) values(999.1239567890123956e-20);
insert into over_float_tbl(f1) values('999.1239567890123956e-20');
insert into over_float_tbl(f1) values(999.1239567890123956e+20);
insert into over_float_tbl(f1) values('999.1239567890123956e+20');
SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float8) THEN '-long_num' 
                   when cast(999.1239567890123956e+20 as float8) then '+long_num'    
           ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float8) THEN '-long_num' 
                   when cast(999.1239567890123956e+20 as float8) then '+long_num'    
           ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
		  
delete from over_float_tbl;
insert into over_float_tbl values(9.99123956789012e-18);

SELECT f1, case f1  
                   when cast(9.99123956789012e-18 as float8) then '-18_num'
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float8) THEN '-20_num' 
                   
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, case f1  
                   when cast(9.99123956789012e-18 as float8) then '-18_num'
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float8) THEN '-20_num' 
                   
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
		  
DROP FOREIGN TABLE over_float_tbl;
CREATE FOREIGN TABLE over_float_tbl (f1  float4) SERVER mot_server ;
INSERT INTO over_float_tbl(f1) VALUES (1.234e+20); 
INSERT INTO over_float_tbl(f1) VALUES ('1.234e+20'); 
INSERT INTO over_float_tbl(f1) VALUES (1.234e-20); 
INSERT INTO over_float_tbl(f1) VALUES ('1.234e-20'); 
SELECT f1, case f1 WHEN cast(1.234e+20 as float4) THEN '+short_num' when cast(1.234e-20 as float8) THEN '-short_num' 
             ELSE 'others'
        END AS CASE_BINARY_DOUBLE
FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
SELECT f1, case f1 WHEN cast(1.234e+20 as float4) THEN '+short_num' when cast(1.234e-20 as float8) THEN '-short_num' 
             ELSE 'others'
        END AS CASE_BINARY_DOUBLE
FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
delete from over_float_tbl;
insert into over_float_tbl(f1) values(999.1239567890123956e-20);
insert into over_float_tbl(f1) values('999.1239567890123956e-20');
insert into over_float_tbl(f1) values(999.1239567890123956e+20);
insert into over_float_tbl(f1) values('999.1239567890123956e+20');
SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float4) THEN '-long_num' 
                   when cast(999.1239567890123956e+20 as float4) then '+long_num'    
           ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float4) THEN '-long_num' 
                   when cast(999.1239567890123956e+20 as float4) then '+long_num'    
           ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
		  
delete from over_float_tbl;
insert into over_float_tbl values(9.99123956789012e-18);
SELECT f1, case f1  
                   when cast(9.99123956789012e-18 as float4) then '-18_num'
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float4) THEN '-20_num' 
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
		 
SELECT f1, case f1  
                   when cast(9.99123956789012e-18 as float4) then '-18_num'
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;

SELECT f1, case f1 WHEN cast(999.1239567890123956e-20 as float4) THEN '-20_num' 
          ELSE 'others' END AS CASE_BINARY_DOUBLE FROM over_float_tbl ORDER BY f1,CASE_BINARY_DOUBLE;
DROP FOREIGN TABLE over_float_tbl;
