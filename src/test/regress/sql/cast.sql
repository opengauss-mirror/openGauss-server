-- Test for CAST function to support DEFAULT value ON CONVERSION ERROR.
-- and nls language.
--set to utc, makes the result indenpendent to local timezone.
-- to make all the output indenpdent to env.
SET LC_TIME = 'C';
SET TIMEZONE TO 'UTC';
SET DATESTYLE= 'ISO, YMD';

CREATE TABLE IF NOT EXISTS test_cast (a INT, b char(10));
INSERT INTO test_cast VALUES(1,'a');
INSERT INTO test_cast VALUES(2,'b');

-- CAST TEST 1
SELECT CAST('1' AS INT DEFAULT '2' ON CONVERSION ERROR);
SELECT CAST('1x' AS INT DEFAULT '2' ON CONVERSION ERROR);
SELECT CAST('1x' AS INT DEFAULT '2x' ON CONVERSION ERROR);
SELECT CAST('1' AS INT);
SELECT CAST('1x' AS INT);
SELECT * FROM test_cast WHERE a = CAST('1x' AS INT DEFAULT '2' ON CONVERSION ERROR);
SELECT * FROM test_cast WHERE a = CAST('1' AS INT);
SELECT 1 + CAST('1' AS INT);
SELECT 1 + CAST('1x' AS INT DEFAULT '2' ON CONVERSION ERROR);
SELECT CAST ( 1 + 1 AS INT );
SELECT CAST ( (1 + CAST('1x' AS INT DEFAULT '2' ON CONVERSION ERROR)) AS INT );

SELECT CAST('9875456' AS integer ,'9999999999S');
SELECT CAST('9875456' AS integer DEFAULT 999 ON CONVERSION ERROR  ,'9999999999');
SELECT CAST('9875456x' AS integer DEFAULT 999 ON CONVERSION ERROR  ,'9999999999');
SELECT CAST(999 AS integer DEFAULT 999 ON CONVERSION ERROR  ,'9999999999');
SELECT CAST('9875456x' AS integer DEFAULT '999' ON CONVERSION ERROR  ,'9999999999');
SELECT CAST('9875456x' AS integer DEFAULT '999x' ON CONVERSION ERROR  ,'9999999999');
SELECT CAST(999 AS integer );

SELECT CAST('January 15, 1989 ' AS DATE);  
SELECT CAST('2018-03-12 18:47:35' AS date,'YYYY-MM-dd hh24:mi:ss');

SELECT CAST('1999-12-01 11:00:00 -08:23' AS TIMESTAMP WITH TIME ZONE);
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE);
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM');
SELECT CAST('January 15, 1989, 11:00 A.M.'  AS DATE);
SELECT CAST('January 15, 1989 , 11:00 A.M.' AS DATE DEFAULT 'January 15, 1989' ON CONVERSION ERROR );
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1998-11-01 11:00:00 -8:00' ON CONVERSION ERROR);
SELECT CAST('1999-13-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1998-11-01 11:00:00 -8:00' ON CONVERSION ERROR);

SELECT CAST('1998-12-01 11:00:00 -08:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1999-12-01 11:00:00 -8:00' ON CONVERSION ERROR, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = american' );
SELECT CAST('1999-13-01 11:00:00 -08:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1998-11-01 11:00:00 -6:00' ON CONVERSION ERROR, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = american' );
SELECT CAST('1999-January-01 11:00:00 -05:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('1999-January-01 11:00:00 -05:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = english');
SELECT CAST('1999-January-01 11:00:00 -05:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = ca');
SELECT CAST('1999-12-01 11:00:00 -05:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', NLS_DATE_LANGUAGE = American);
SELECT CAST('2014-January-02 12:11:22' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('2014-January-02 12:11:22' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANG = American');
SELECT CAST('1999-12-01 11:00:00' AS date, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('1999-January-01 11:00:00' AS date, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('1999-12-01 11:00:00' AS date);
SELECT CAST(CAST('1999-12-01 11:00:00 -08:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1999-11-01 11:00:00 -8:00' ON CONVERSION ERROR, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = american' ) AS DATE);
SELECT CAST('1999-January-01 11:00:00 -05:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = American');
DROP TABLE test_cast;

-- CAST TEST 2
CREATE TABLE test_cast(col1 varchar(30), col2 char(40));

CREATE SEQUENCE date_strings_id_seq;
CREATE TABLE date_strings (id INTEGER NOT NULL DEFAULT nextval('date_strings_id_seq'),  date_array TEXT[]);
CREATE UNIQUE INDEX date_strings_pkey ON date_strings(id);
ALTER TABLE date_strings ADD CONSTRAINT date_strings_pkey PRIMARY KEY USING INDEX date_strings_pkey;

CREATE TABLE events ( year  TEXT, month TEXT, day   TEXT, time  TEXT);

INSERT INTO test_cast VALUES('1999-12-01 11:00:00', '2017-08-20 00:00:00');

SELECT CAST((SELECT col1 FROM test_cast) as date);
SELECT CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS');
SELECT SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS') FROM 3);
SELECT CAST(SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS') FROM 3) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS');
SELECT CAST(SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS') FROM 3) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS TZH:TZM');
SELECT CAST(SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS') FROM 3) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM');
SELECT CAST((SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = ENGLISH');
SELECT CAST(('19'||SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE= english');
INSERT INTO test_cast values('2000-January-01 11:00:00', '2027-september-10 01:00:00');
SELECT CAST(('19'||SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE= english');
DELETE FROM test_cast WHERE COL1 = '1999-12-01 11:00:00';
SELECT CAST((SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = ENGLISH');
SELECT CAST(('19'||SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE= english');
SELECT CAST(('19'||SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = CANADA') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE= american');
SELECT CAST(('19'||SUBSTRING(CAST((SELECT SUBSTRING(col1 FROM 3) FROM test_cast) AS TIMESTAMP WITH TIME ZONE, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = american') FROM 3)) AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE= canada');

-- CAST ARRAY 1
SELECT CAST(ARRAY['1999-01-01', '2023-10-23'] AS timestamp[]);
SELECT CAST(ARRAY['1999-01-01 12:00:00', '2023-10-23 15:00:00'] AS timestamp[]);
SELECT CAST(ARRAY['1999-January-01 11:00:00', '2023-January-23 11:00:00'] AS TIMESTAMP[], 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST((SELECT ARRAY[col1, col2] FROM test_cast) AS TIMESTAMP[], 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');

-- ARRAY CAST 2.
INSERT INTO date_strings (date_array) VALUES ('{"2023-january-01 10:00:00", "2023-february-01 12:00:00", "2023-03-01 14:00:00"}');
SELECT id, CAST(date_array AS TIMESTAMP[], 'YYYY-MM-DD HH24:MI:SS', 'NLS_DATE_LANGUAGE = American') AS timestamp_array FROM date_strings;
SELECT id, CAST(date_array AS TIMESTAMP[], 'YY-MM-DD HH24:MI:SS', 'NLS_DATE_LANGUAGE = American') AS timestamp_array FROM date_strings;
SELECT id, CAST(date_array AS TIMESTAMP[]) AS timestamp_array FROM date_strings;
SELECT id, CAST(date_array AS TIMESTAMP[], 'YYYY-MM-DD HH24:MI:SS') AS timestamp_array FROM date_strings;

-- DOMAIN TYPE CAST
CREATE DOMAIN event_timestamp AS TIMESTAMP CHECK (VALUE >= '2000-01-01 00:00:00');
SELECT CAST('2024-10-23 15:30:00' AS event_timestamp);
SELECT CAST('2024-10-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('24-10-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('2024-November-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS');
SELECT TO_CHAR(CAST('2024-10-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American'));
SELECT CAST(CAST('2024-10-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American') AS event_timestamp, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = english');
SELECT CAST(TO_CHAR(CAST('2024-10-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American')) AS event_timestamp, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = english');
SELECT CAST(TO_CHAR(CAST('2024-september-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American')) AS event_timestamp, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = english');


SELECT CAST('2024-November-23 15:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS');
SELECT CAST('2024-November-23 11:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American');
SELECT CAST(TO_CHAR(CAST('2024-september-23 12:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American')) AS event_timestamp, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = english');
SELECT CAST(TO_CHAR(CAST('2024-september-23 12:30:00' AS event_timestamp, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = canda')) AS event_timestamp, 'YY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = english');

INSERT INTO events (year, month, day, time) VALUES ('1999', 'january', '01', '11:00:00'),('2023', 'february', '23', '08:30:00'), ('20', 'september', '23', '08:30:00');

SELECT CAST(year || '-' || month || '-' || day || ' ' || time AS TIMESTAMP, 'YYYY-MM-DD HH:MI:SS', 'NLS_DATE_LANGUAGE = American') AS event_timestamp FROM  events;
SELECT CAST(year || '-' || month || '-' || day || ' ' || time AS TIMESTAMP, 'YYYY-MM-DD HH:MI:SS') AS event_timestamp FROM  events;
SELECT CAST(year || '-' || month || '-' || day || ' ' || time AS TIMESTAMP, 'YY-MM-DD HH:MI:SS',  'NLS_DATE_LANGUAGE = American') AS event_timestamp FROM  events;
SELECT CAST(year || '-' || month || '-' || day || ' ' || time AS TIMESTAMP) AS event_timestamp FROM events;

DROP TABLE events;
DROP TABLE test_cast;
DROP TABLE date_strings;

---another type
SELECT cast('1,234.56' as number default 999 on conversion  error , '99999.9') AS result;
SELECT cast('132654' as number default '123' on conversion error );
SELECT cast('132654' as number default 'a123' on conversion error );
SELECT cast(132,654.12 as number default 123 on conversion error ,'999,999,999.99' );
SELECT cast(132654.12 as number default 123 on conversion error ,'999,999,999.99' );
SELECT cast('12,234.56' as number default 999 on conversion error ,'999G999G999.99');
SELECT cast('1,234.56' as number default 999 on conversion  error , '999,999.99') AS result;
SELECT cast(' 2022-09-21 14:13:29' as timestamp default '01-09-02 14:10:10' on conversion error,'yyyy-mm-dd hh24:mi:ss')-30/60/24;
SELECT cast('22-Sep-21 12:13:29'as timestamp)-30/60/24;
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '1999-12-01 11:00:00 -8:00' ON CONVERSION ERROR);
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE  DEFAULT '2000-01-01 01:00:00 -8:00' ON CONVERSION ERROR, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('1299-12-12 12:30:00 -08:00' AS TIMESTAMP WITH TIME ZONE ,'YYYY-MM-DD HH:MI:SS TZH:TZM');
SELECT CAST('1299-12-12 12:30:00 -08:00' AS TIMESTAMP WITH TIME ZONE);
SELECT CAST(200 AS NUMBER DEFAULT 0 ON CONVERSION ERROR);

SELECT CAST(TO_DATE('22-Oct-1997', 'DD-Mon-YYYY') AS TIMESTAMP WITH LOCAL TIME ZONE) ;
SELECT product_id, CAST(ad_sourcetext AS VARCHAR2(30)) text  FROM print_media   ORDER BY product_id;
SELECT CAST(200  AS NUMBER DEFAULT 0 ON CONVERSION ERROR);
SELECT CAST('Jan 15, 1989, 11:00 A.M.' AS DATE 'Mon dd, YYYY, HH:MI A.M.');
SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.');
SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.');
SELECT CAST('1999-12-01 11:00:00 -8:00' AS TIMESTAMP WITH TIME ZONE DEFAULT '2000-01-01 01:00:00 -8:00' ON CONVERSION ERROR, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'NLS_DATE_LANGUAGE = American');
SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.');
SELECT CAST('N/A' AS NUMBER DEFAULT '0' ON CONVERSION ERROR);

select cast(1.35464648979849849498794546654687867987 as int) ;
select cast(1.354646489798498494987945466546823525243453 as varchar2(200)) ;
select cast(cast ('01-May-03 14:10:10.123000'  as timestamp ,'DD-Mon-RR HH24:MI:SS.FF','NLS_DATE_LANGUAGE = American') as varchar2(200),'DD-Mon-RR HH24:MI:SS.FF') ;
select cast(cast ('01-May-03 14:10:10.123000' as timestamp  ,'DD-Mon-RR HH24:MI:SS.FF','NLS_DATE_LANGUAGE = American') as varchar2(200));
select cast(cast ('29-Feb-29 14:10:10.123000'  as timestamp ,'DD-Mon-RR HH24:MI:SS.FF') as varchar2(200));
select cast(cast ('29-Feb-29 14:10:10.123000'  as timestamp ,'DD-Mon-RR HH24:MI:SS.FF','NLS_DATE_LANGUAGE = American') as varchar2(200));
select cast(1.35464648979849849498794546654682352524 as varchar2(2)) ;
select cast(1.35464648979849849498794546654682352524 as TINYINT);
select cast(1.35464648979849849498794546654682352524 as NUMERIC);
select cast(as VARCHAR2(20) 'as');
select cast('9.999999' as varchar2(20),'99999.9999999999999');

SELECT CAST('January 15, 1989, 11:00 A.M.' AS DATE DEFAULT NULL ON CONVERSION ERROR, 'Month dd, YYYY, HH:MI A.M.', 'NLS_DATE_LANGUAGE = American');
select cast('01-May-03 14:10:10.123000'  as varchar2(200) default 20 on conversion error) ;
select cast(1234::varchar(4) as varchar(4));
select cast(cast(1234 as varchar2(4)) as varchar2(4) );
select cast(1234::varchar(2) as varchar(4));
select cast(cast(1234 as varchar2(2)) as varchar2(4) );
select cast(cast(1234 as varchar2(4)) as varchar2(2) );
select cast(1234 as varchar2(4) );
select cast(1234::varchar(4) as varchar(2));
select cast(21 as varchar2(2));
select cast(1234 as varchar2(2)) ;
SELECT CAST('abc' AS NUMBER DEFAULT -1 ON CONVERSION ERROR) ;

EXPLAIN PLAN FOR SELECT CAST(CAST(1234 AS VARCHAR2(4)) AS VARCHAR2(2)) ;
select cast(212 as varchar2(2));
select cast(122 as int default 12 on conversion error );
SELECT CAST('1999-12-01 11:00:00 -08:23' AS TIMESTAMP WITH TIME ZONE, 'YYYY-MM-DD HH:MI:SS TZH:TZM', 'nls_date_language = American');

SELECT CAST('true' AS  BOOLEAN) ;
SELECT CAST('f' AS  BOOLEAN) ;
SELECT CAST('false' AS  BOOLEAN) ;
SELECT CAST('t' AS  BOOLEAN) ;
SELECT CAST('a' AS BOOLEAN) ;
SELECT CAST('' AS BOOLEAN) ;
SELECT CAST('1' AS BOOLEAN) ;
SELECT CAST('0' AS BOOLEAN) ;
SELECT CAST('-1' AS BOOLEAN) ;
SELECT CAST('-0' AS BOOLEAN) ;
SELECT CAST(null AS BOOLEAN) ;
SELECT CAST('1.1' AS BOOLEAN) ;
SELECT CAST(0 AS BOOLEAN) ;
SELECT CAST(1 AS BOOLEAN) ;
SELECT CAST(-1 AS BOOLEAN) ;
SELECT CAST(-0 AS BOOLEAN) ;
SELECT CAST(1.1 AS BOOLEAN) ;
SELECT CAST('yes' AS BOOLEAN) ;
SELECT CAST('no' AS BOOLEAN) ;
SELECT CAST('accept' AS BOOLEAN) ;
SELECT CAST('refuse' AS BOOLEAN) ;

select to_number('') ;
select cast('' as number) ;
select cast(null as number) ;
select cast( as number) ;
select cast( as number default 999 on conversion error) ;

select cast(1e38-1 as number default  999 on conversion error ) ;
select cast(1e39-1 as number default  999 on conversion error ) ;
select cast(1e41-1 as number default  999 on conversion error ) ;
select cast(0.9999999999999999999999999999999999999 as number default 999 on conversion error ) ;
select cast(0.99999999999999999999999999999999999999 as number default 999 on conversion error ) ;
select cast(0.99999999999999999999999999999999999999999 as number default 999 on conversion error ) ;
select cast(-1e39+1 as number default 999 on conversion error ) ;
select cast(-1e40+1 as number default 999 on conversion error ) ;
select cast(-1e40+1 as number default 999 on conversion error ) ;

select cast('1e38-1' as number default  999 on conversion error ) ;
select cast('1e39-1' as number default  999 on conversion error ) ;
select cast('1e41-1' as number default  999 on conversion error ) ;
select cast('0.9999999999999999999999999999999999999' as number default 999 on conversion error ) ;
select cast('0.99999999999999999999999999999999999999' as number default 999 on conversion error ) ;
select cast('0.99999999999999999999999999999999999999999' as number default 999 on conversion error ) ;
select cast('-1e39+1' as number default 999 on conversion error ) ;
select cast('-1e40+1' as number default 999 on conversion error ) ;
select cast('-1e40+1' as number default 999 on conversion error ) ;

--125
select cast(99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 as number default 999 on conversion error ) ;
--126
select cast(999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999  as number default 999 on conversion error ) ;
-- -125
select cast(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 as number default 999 on conversion error ) ;
-- -126
select cast(-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 as number default 999 on conversion error ) ;
-- 1000
select cast(-0.99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 as number default 999 on conversion error ) ;
--字符
--125
select cast('99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999' as number default 999 on conversion error ) ;
--126
select cast('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999'  as number default 999 on conversion error ) ;
-- -125
select cast('-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999' as number default 999 on conversion error ) ;
-- -126
select cast('-99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999' as number default 999 on conversion error ) ;
-- 1000
select cast('-0.99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999' as number default 999 on conversion error ) ;

--带有指定位数的值
SELECT cast('1234.56' as number default 999 on conversion error ,'99.99');
SELECT cast('1234.56' as number default 999 on conversion error ,'999999.99');
SELECT cast('1234.56' as number default 999 on conversion error ,'999999');

--带有前导零的值
select cast('234234.4350' as number default 999 on conversion error , '999999.0000000') ;
select cast('234234.4350' as number default 999 on conversion error , '999999.0000') ;
select cast('234234.4350' as number default 999 on conversion error , '999999.00') ;
--小数值
SELECT cast('1234.56' as number default 999 on conversion error ,'99999.999');
SELECT cast('1234.56' as number default 999 on conversion error ,'9999.99');
SELECT cast('1234.56' as number default 999 on conversion error ,'99.99');

--分组，（千）分隔符
SELECT cast('12,345,678.56' as number ,'999,999,999,999.99');

SELECT cast('12,345,678.56' as number default 999 on conversion error ,'999,999,999,999.99');

SELECT cast('12,345,678.56' as number default 999 on conversion error ,'99,999,999.99');

SELECT cast('1,234.56' as number default 999 on conversion error ,'9,999.99');

--尖括号内负值
select cast('<1234>' as number,'9999PR') ;

select cast('1234' as number,'9999PR') ;

--带负号的数值（使用区域设置）
SELECT cast('-$1,234.56' as number , 'SL999999999D99') ;

SELECT cast('$1,234.56-' as number , 'L999999999D99S');

--货币符号
SELECT cast('$1,234.56' as number, 'L999999999D99');

--小数点（使用区域设置）
SELECT cast('1234.56' as number default 999 on conversion error ,'99999D999');

SELECT cast('1234.56' as number default 999 on conversion error ,'9999D99');

SELECT cast('1234.56' as number default 99 on conversion error ,'99D99');

--分组分隔符
SELECT cast('12,345,678.56' as number default 999 on conversion error ,'999G999G999G999D99');

SELECT cast('12,345,678.56' as number default 999 on conversion error ,'99G999G999D99');

SELECT cast('1,234.56' as number default 999 on conversion error ,'999D99');

--移动指定位（小数）
SELECT cast('1413.213' as number , '9999V999') ;

SELECT cast('1413.213' as number , '999999V9') ;

SELECT cast('1413213' as number , '999V9999');

--default
select cast('a456' as number default 999 on conversion error ) ;

select cast('456+123' as number default 999 on conversion error) ;

select cast('456+123'+123 as number default 999 on conversion error) ;

select cast('132654' as number default '123' on conversion error ) ;

select cast('132654' as number default 'a123' on conversion error ) ;

select cast('iahdfoahsdf' as number DEFAULT '12414423' ON CONVERSION ERROR);

select cast('中文' as number DEFAULT 66 ON CONVERSION ERROR, '999D99');

select cast('中文' as number DEFAULT '12414423' ON CONVERSION ERROR, '999999999999999999999999');

select cast('中文' as number DEFAULT '1241423'+123 ON CONVERSION ERROR, '999999999999999999999999');

select cast( as number DEFAULT '1241423'+123 ON CONVERSION ERROR, '999999999999999999999999');

--边界
--精度边界
select cast(1e41-1 as number default 999 on conversion error ) ;

select cast('' as number default 999 on conversion error) ;

select cast(null as number default 999 on conversion error) ;

--数字格式模型
--输入格式
SELECT cast('1,234.56' as number default 999 on conversion  error , '99999.9') AS result ;

select cast('132654' as number default '123' on conversion error ) ;

select cast('132654' as number default 'a123' on conversion error ) ;

select cast('132,654.12' as number default 123 on conversion error ,'999,999,999.99' ) ;

select cast('132654.12' as number default 123 on conversion error ,'999,999,999.99' ) ;

SELECT cast('12,234.56' as number default 999 on conversion error ,'999G999G999.99');

--复杂度
SELECT cast('RMB1,234.56' as number default 999 on conversion error, 'L999999999D99', 'NLS_NUMERIC_CHARACTERS = ''.,''   NLS_CURRENCY = ''RMB''') AS converted_number;

SELECT cast('RMB1,234.56' as number, 'L999999999D99', 'NLS_NUMERIC_CHARACTERS = ''.,''   NLS_CURRENCY = ''RMB''') AS converted_number;

--隐式转换
select cast('456'+123 as number) ;

select cast('456'+123 as number default 999 on conversion error) ;

SELECT cast('1234.56'+1231323 as number default 999 on conversion error ,'9999999999D999') ;

SELECT cast('-$1,234.56'+6546464 as number, 'SL999999999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS converted_number;

SELECT cast('-$1,234.56'+6546464 as number default 999 on conversion error , 'SL999999999D99', 'NLS_NUMERIC_CHARACTERS = ''.,'' NLS_CURRENCY = ''$''') AS converted_number;

select cast(''+123 as number default 999 on conversion error ) ;

select cast('1q23' as number default '122535235235234524523453245325234'+3123 on conversion error );

SELECT cast ('01-01-2002 14:10:10.123000','DD-MM-RR HH24:MI:SS.FF');

-- 创建示例表
CREATE TABLE employee ( id NUMBER, name VARCHAR2(50), hire_date DATE ); 
INSERT INTO employee (id, name, hire_date) VALUES (1, 'John Doe', to_date('2023-09-24 08:22:0') - 10);
INSERT INTO employee (id, name, hire_date) VALUES (2, 'Jane Smith', to_date('2024-10-24 05:22:0') - 20);

-- 查询并格式化 hire_date
SELECT id, name, TO_CHAR(hire_date, 'YYYY-MM-DD HH24:MI:SS') AS default_format, TO_CHAR(hire_date, 'DD-MM-YYYY HH24:MI:SS') AS custom_format_1, TO_CHAR(hire_date, 'MM/DD/YYYY HH12:MI:SS AM') AS custom_format_2 FROM employee; 

--timestamp
--arg边界值检验
--day
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('31-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('-01-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('1-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('131-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('001-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--month
SELECT cast ('01-13-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01--13-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-001-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-111-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01--02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--年
--RR
SELECT cast ('01-01-2002 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');

SELECT cast ('01-01--2002 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01-9999 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01-999 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01-02002 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01-20002 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('01-01 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--YYYY

--year为YYYY
--year为正常四位数
SELECT cast ('01-01-2002 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');

--year为负数四位数越界
--无S指定负号
SELECT cast ('01-01--2002 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');
--有S指定负号
SELECT cast ('01-01--2002 14:10:10.123000'as timestamp,'DD-MM-SYYYY HH24:MI:SS.FF');
--year为越界四位数
SELECT cast ('01-01-0000 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');
--year为3位数位数
SELECT cast ('01-01-999 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');
--year为不越界5位数
SELECT cast ('01-01-02002 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');
--year为越界五位数
SELECT cast ('01-01-20002 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');
--year不输入
SELECT cast ('01-01- 14:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');

--hour
--hour两位数越界
SELECT cast ('01-01-02 25:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--hour两位数负数越界
SELECT cast ('01-01-02 -20:10:10.123000'as timestamp,'DD-MM-RR SHH24:MI:SS.FF');
--hour为一位数
SELECT cast ('01-01-02 1:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--hour为越界三位数
SELECT cast ('01-01-02 100:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--hour为不越界三位数
SELECT cast ('01-01-02 010:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--hour不输入
SELECT cast ('01-01-02 :10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--分钟
--两位数越界
SELECT cast ('01-01-02 10:70:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--负数分钟越界
SELECT cast ('01-01-02 10:-10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--一位数分钟
SELECT cast ('01-01-02 10:1:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--三位数分钟越界
SELECT cast ('01-01-02 10:100:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--三位数分钟不越界
SELECT cast ('01-01-02 10:010:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--分钟不输入
SELECT cast ('01-01-02 10::10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--秒
--正常两位数秒
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--越界两位数秒
SELECT cast ('01-01-02 10:10:70.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--一位数秒
SELECT cast ('01-01-02 10:10:7.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--越界负两位数秒
SELECT cast ('01-01-02 10:10:-60.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--不越界三位数秒
SELECT cast ('01-01-02 10:10:050.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--越界三位数秒
SELECT cast ('01-01-02 10:10:770.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--秒不输入
SELECT cast ('01-01-02 10:10:'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--毫秒
--三位数壕秒
SELECT cast ('01-01-02 10:10:40.123'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--毫秒位数越界
SELECT cast ('01-01-02 10:10:10.1239'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--毫秒处不输入
SELECT cast ('01-01-02 10:10:10'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--特殊日期检验
--闰年2月29检验
SELECT cast ('29-02-04 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--普通年2月29检验
SELECT cast ('29-02-03 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--输入值多输入一部分数据
--日期处随便多输入一条
SELECT cast ('21-23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--时间处随便多输入一条
SELECT cast ('23-09-02 14:10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt大小写检验
--年月日
--fmt中的DD参数
--fmt中的D为两位，输入为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt中的D有大写有小写为两位，输入值为两位
SELECT cast ('11-01-02 10:10:10.123000'as timestamp,'Dd-MM-RR HH24:MI:SS.FF');

--fmt中的D有大写有小写为两位，输入值为两位
SELECT cast ('11-01-02 10:10:10.123000'as timestamp,'dD-MM-RR HH24:MI:SS.FF');

--fmt中的D全小写为两位，输入值为两位
SELECT cast ('11-01-02 10:10:10.123000'as timestamp,'dd-MM-RR HH24:MI:SS.FF');

--fmt中的MM参数
--fmt中的M为两位，输入为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt中的M有大写有小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-Mm-RR HH24:MI:SS.FF');

--fmt中的M有大写有小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-mM-RR HH24:MI:SS.FF');

--fmt中的M全小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-mm-RR HH24:MI:SS.FF');

--opengauss
SELECT cast ('01-Jan-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Feb-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Mar-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Apr-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-May-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Jun-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Jul-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Aug-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Sep-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Oct-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Nov-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Dec-02 10:10:10.123000'as timestamp,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Jan-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Feb-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Mar-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Apr-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-May-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Jun-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Jul-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Aug-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Sep-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Oct-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Nov-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Dec-02 10:10:10.123000'as timestamp,'DD-MON-RR HH24:MI:SS.FF');

SELECT cast ('01-Jan-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Feb-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Mar-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Apr-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-May-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Jun-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Jul-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Aug-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Sep-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Oct-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Nov-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

SELECT cast ('01-Dec-02 10:10:10.123000'as timestamp,'DD-mon-RR HH24:MI:SS.FF');

--fmt中的RR参数
--fmt中的R为两位，输入为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt中的R有大写有小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-Rr HH24:MI:SS.FF');

--fmt中的R有大写有小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-rR HH24:MI:SS.FF');

--fmt中的R全小写为两位，输入值为两位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-rr HH24:MI:SS.FF');

--fmt中的YYYY参数
--fmt中的YYYY为四位，输入为四位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FF');

--fmt中的YYYY有大写有小写为四位，输入值为四位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-YyYy HH24:MI:SS.FF');

--fmt中的YYYY全小写为四位，输入值为四位
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-yyyy HH24:MI:SS.FF');

--分时秒
--fmt时分秒中的hour
--fmt中的hour不指定具体数值，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR HH:MI:SS.FF');

--fmt中的hour不指定具体数值，按1-24输入
SELECT cast ('01-01-02 14:10:10.123000'as timestamp,'DD-MM-RR HH:MI:SS.FF');

--fmt中的hour指定为12，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR HH12:MI:SS.FF');

--fmt中的hour指定为24，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt中的hour指定为24，按1-24输入
SELECT cast ('01-01-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt中的hour指定为13，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR HH13:MI:SS.FF');

--fmt中的hour不指定具体数值且为小写，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR hh:MI:SS.FF');

--fmt中的hour不指定具体数值且为前大后小，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR Hh:MI:SS.FF');

--fmt中的hour不指定具体数值且为前小后大，按1-12输入
SELECT cast ('01-01-02 11:10:10.123000'as timestamp,'DD-MM-RR hH:MI:SS.FF');

--fmt中的hour指定为24，不输入
SELECT cast ('01-01-02 :10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt时分秒中的MI
--正常MI
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--MI一大一小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:Mi:SS.FF');

--MI一小一大
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:mI:SS.FF');

--MI全小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:mi:SS.FF');

--fmt时分秒中的SS
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--SS一大一小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:Mi:Ss.FF');
--SS一小一大
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:mI:sS.FF');
--SS全小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:mi:ss.FF');

--fmt时分秒中的FF
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--FF一大一小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.Ff');

--FF一小一大
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.fF');

--FF全小
SELECT cast ('01-01-02 10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.ff');

--fmt长短检验
--三位数D一位数输入值
SELECT cast ('1-09-02 14:10:10.123000'as timestamp,'DDD-MM-RR HH24:MI:SS.FF');

--三位数D两位数输入值
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DDD-MM-RR HH24:MI:SS.FF');

--三位数D三位数输入值
SELECT cast ('014-09-02 14:10:10.123000'as timestamp,'DDD-MM-RR HH24:MI:SS.FF');

--一位数D一位数输入值
SELECT cast ('1-09-02 14:10:10.123000'as timestamp,'D-MM-RR HH24:MI:SS.FF');
--一位数D两位数输入值
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'D-MM-RR HH24:MI:SS.FF');
--一位数D三位数输入值
SELECT cast ('014-09-02 14:10:10.123000'as timestamp,'D-MM-RR HH24:MI:SS.FF');

--三位数M一位数输入值
SELECT cast ('01-9-02 14:10:10.123000'as timestamp,'DD-MMM-RR HH24:MI:SS.FF');
--三位数M两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MMM-RR HH24:MI:SS.FF');
--三位数M三位数输入值
SELECT cast ('01-009-02 14:10:10.123000'as timestamp,'DD-MMM-RR HH24:MI:SS.FF');

--一位数M一位数输入值
SELECT cast ('01-9-02 14:10:10.123000'as timestamp,'DD-M-RR HH24:MI:SS.FF');
--一位数M两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-M-RR HH24:MI:SS.FF');
--一位数M三位数输入值
SELECT cast ('01-012-02 14:10:10.123000'as timestamp,'DD-M-RR HH24:MI:SS.FF');

--三位数R一位数输入值
SELECT cast ('01-09-2 14:10:10.123000'as timestamp,'DD-MM-RRR HH24:MI:SS.FF');
--三位数R两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-RRR HH24:MI:SS.FF');
--三位数R四位数输入值
SELECT cast ('01-09-2002 14:10:10.123000'as timestamp,'DD-MM-RRR HH24:MI:SS.FF');

--一位数R一位数输入值
SELECT cast ('01-09-2 14:10:10.123000'as timestamp,'DD-MM-R HH24:MI:SS.FF');
--一位数R两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-R HH24:MI:SS.FF');

--三位数H一位数输入值
SELECT cast ('01-09-02 2:10:10.123000'as timestamp,'DD-MM-RR HHH24:MI:SS.FF');
--三位数H两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HHH24:MI:SS.FF');
--三位数H3位数输入值
SELECT cast ('01-09-02 014:10:10.123000'as timestamp,'DD-MM-RR HHH24:MI:SS.FF');

--一位数H一位数输入值
SELECT cast ('01-09-02 2:10:10.123000'as timestamp,'DD-MM-RR H24:MI:SS.FF');
--一位数H两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-RR H24:MI:SS.FF');

--三位数S一位数输入值
SELECT cast ('01-09-02 02:10:9.123000'as timestamp,'DD-MM-RR HH24:MI:SSS.FF');
--三位数S两位数输入值
SELECT cast ('01-09-02 14:10:56.123000'as timestamp,'DD-MM-RR HH24:MI:SSS.FF');
--三位数S3位数输入值
SELECT cast ('01-09-02 14:10:056.123000'as timestamp,'DD-MM-RR HH24:MI:SSS.FF');

--一位数S一位数输入值
SELECT cast ('01-09-02 02:10:9.123000'as timestamp,'DD-MM-RR HH24:MI:S.FF');
--一位数S两位数输入值
SELECT cast ('01-09-02 14:10:56.123000'as timestamp,'DD-MM-RR HH24:MI:S.FF');

--三位数F正常输入
SELECT cast ('01-09-02 14:10:56.123000'as timestamp,'DD-MM-YYYY HH24:MI:SS.FFF');

--一位数F正常输入
SELECT cast ('01-09-02 14:10:56.123000'as timestamp,'DD-MM-RR HH24:MI:SS.F');

--三位数Y一位数输入值
SELECT cast ('01-09-2 14:10:10.123000'as timestamp,'DD-MM-YYY HH24:MI:SS.FF');
--三位数Y两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-YYY HH24:MI:SS.FF');
--三位数Y四位数输入值
SELECT cast ('01-09-2002 14:10:10.123000'as timestamp,'DD-MM-YYY HH24:MI:SS.FF');
--五位数Y一位数输入值
SELECT cast ('01-09-2 14:10:10.123000'as timestamp,'DD-MM-YYYYY HH24:MI:SS.FF');
--五位数Y两位数输入值
SELECT cast ('01-09-02 14:10:10.123000'as timestamp,'DD-MM-YYYYY HH24:MI:SS.FF');
--五位数Y四位数输入值
SELECT cast ('01-09-2002 14:10:10.123000'as timestamp,'DD-MM-YYYYY HH24:MI:SS.FF');

--fmt符号检验
--日期符号的替换
--符号正常
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--将-号变成@符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD@MM@RR HH24:MI:SS.FF');
--将-号变成!符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD!MM!RR HH24:MI:SS.FF');
--将-号变成#符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD#MM#RR HH24:MI:SS.FF');
--将-号变成$符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD$MM$RR HH24:MI:SS.FF');
--将-号变成%符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD%MM%RR HH24:MI:SS.FF');
--将-号变成/符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD/MM/RR HH24:MI:SS.FF');
--将-号变成:符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD:MM:RR HH24:MI:SS.FF');
--将-号变成?符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD?MM?RR HH24:MI:SS.FF');
--将-号变成.符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD.MM.RR HH24:MI:SS.FF');
--将-号变成*符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD*MM*RR HH24:MI:SS.FF');
--将-号变成+符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD+MM+RR HH24:MI:SS.FF');
--将-号变成=符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD=MM=RR HH24:MI:SS.FF');

--时间符号的替换
--符号正常
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--将：号变成-符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24-MI-SS.FF');
--将：号变成!符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24!MI!SS.FF');
--将：号变成@符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24@MI@SS.FF');
--将：号变成#符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24#MI#SS.FF');
--将：号变成$符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24$MI$SS.FF');
--将：号变成%符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24%MI%SS.FF');
--将：号变成&符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24&MI&SS.FF');
--将：号变成*符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24*MI*SS.FF');
--将：号变成/符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24/MI/SS.FF');
--将：号变成=符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24=MI=SS.FF');
--将：号变成+符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24+MI+SS.FF');

--输入值的符号与fmt不一致
SELECT cast ('14!09*02#14$10^10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--不同符号的替换
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DDMM%RR HH24*MI/SS.FF');
--输入值与fmt不一致
SELECT cast ('14!09%02#14$10^10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');
--fmt中的字母写错
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'pl-MM-RR HH24:MI:SS.FF');
--输入值与fmt都打乱
SELECT cast ('14!09&02#14$10^10.123000'as timestamp,'DD#MM$RR^HH24*MI+SS.FF');

--不同符号的替换
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD@MM%RR HH24*MI/SS.FF');

--fmt中的字母写错
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'pl-MM-RR HH24:MI:SS.FF');

--符号长度与输入值不一致
--符号中有中文符号
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD？MM%RR HH24*MI/SS.FF');
--符号加长，与输入值不对应
SELECT cast ('14-09-02 14:10:10.123000'as timestamp,'DD??MM%RR HH24*MI/SS.FF');

--输入输出中皆无分隔符号
SELECT cast ('140902141010.123000'as timestamp,'DDMMRRHH24MISS.FF');

--fmt无分隔符号
SELECT cast ('14-09-02 14-10-10.123000'as timestamp,'DDMMRRHH24MISS.FF');

--输入值多输入一部分数据
--日期处随便多输入一条
SELECT cast ('21-23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--时间处随便多输入一条
SELECT cast ('23-09-02 14:10:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--fmt少写一部分，输入值不变
--正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS.FF');

--DD少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'MM-RR HH24:MI:SS.FF');

--DD少写，输入值也少写
SELECT cast ('09-02 14:10:10.123000'as timestamp,'MM-RR HH24:MI:SS.FF');

--MM少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-RR HH24:MI:SS.FF');

--MM少写，输入值也少写
SELECT cast ('23-02 14:10:10.123000'as timestamp'DD-RR HH24:MI:SS.FF');

--RR少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM HH24:MI:SS.FF');
--RR少写，输入值也少写
SELECT cast ('23-09 14:10:10.123000'as timestamp,'DD-MM HH24:MI:SS.FF');

--YYYY少写，输入值正常
SELECT cast ('23-09-2002 14:10:10.123000'as timestamp,'DD-MM HH24:MI:SS.FF');

--YYYY少写，输入值也少写
SELECT cast ('23-09-2002 14:10:10.123000'as timestamp,'DD-MM HH24:MI:SS.FF');

--HH少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR MI:SS.FF');

--HH少写，输入值少写
SELECT cast ('23-09-02 14:10:10'as timestamp,'DD-MM-RR MI:SS.FF');

--MI少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:SS.FF');

--MI少写，输入值少写
SELECT cast ('23-09-02 14:10.123000'as timestamp,'DD-MM-RR HH24:SS.FF');

--SS少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:FF');
--SS少写，输入值少写
SELECT cast ('23-09-02 14:10:.123000'as timestamp,'DD-MM-RR HH24:MI:FF');

--FF少写，输入值正常
SELECT cast ('23-09-02 14:10:10.123000'as timestamp,'DD-MM-RR HH24:MI:SS');
--FF少写，输入值少写
SELECT cast ('23-09-02 14:10:10'as timestamp,'DD-MM-RR HH24:MI:SS');

--fmt和arg参数同时少写
SELECT cast ('05-02 14:10' as timestamp,'MM-RR HH24:MI');

SELECT cast ('140902141010.123000'as timestamp,'DDMMRRHH24MISS.FF');

--fmt多写一部分，输入值与之对应
--多写一段DD
SELECT cast ('14-23-09-02 14:10:10.123000'as timestamp,'DD-DD-MM-RR HH24:MI:SS.FF');

--default部分
--正常
SELECT cast ('10-09-02 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--输入值部分异常，fmt正常，default正常

--越界异常，default正确
--day正数越界
SELECT cast ('31-09-02 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day负数数越界
SELECT cast ('-01-09-02 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day不输入
SELECT cast ('09-02 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--MM越界
SELECT cast ('02-13-02 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--YYYY越界
SELECT cast ('02-13-9999 14:10:10.123000'as timestamp DEFAULT '11-09-2003 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-YYYY HH24:MI:SS.FF');
--特殊日期
--闰年2月29检验
SELECT cast ('29-02-04 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--普通年2月29检验
SELECT cast ('29-02-03 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--符号错误
SELECT cast ('29？02-03 14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

SELECT cast ('29-02-03？14:10:10.123000'as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--空值检验null
SELECT cast (null as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--空字符串''
SELECT cast (''  as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--什么都不输入
SELECT cast (  as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--fmt输入异常，default正常，输入值中正常
--DD多输一段，输入值也多输一段
SELECT cast ('21-31-09-02 14:10:10.123000'  as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-DD-MM-RR HH24:MI:SS.FF');
--DD多输一段，输入值正常
SELECT cast ('31-09-02 14:10:10.123000' as timestamp  DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD-DD-MM-RR HH24:MI:SS.FF');

--fmt中的字母错误
SELECT cast ('09-09-02 14:10:10.123000' as timestamp  DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'pl-MM-RR HH24:MI:SS.FF');
--fmt中的符号错误
SELECT cast ('09-09-02 14:10:10.123000'  as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'DD--MM-RR HH24:MI:SS.FF');
--fmt为空
SELECT cast ('09-09-02 14:10:10.123000'  as timestamp DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR);
--fmt为null
SELECT cast ('09-09-02 14:10:10.123000' as timestamp  DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,null);
--fmt为''
SELECT cast ('09-09-02 14:10:10.123000' as timestamp  DEFAULT '11-09-11 14:10:10.123000' ON CONVERSION ERROR,'');

--fmt正常，输入值异常，default正常
SELECT cast ('.123000'  as timestamp DEFAULT '14-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--fmt正常，输入值正常，default越界
SELECT cast ('09-09-02 14:10:10.123000' as timestamp  DEFAULT '29-02-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--fmt正常，输入值正常，default正常
SELECT cast ('09-09-02 14:10:10.123000'  as timestamp DEFAULT '28-02-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--fmt正常，输入值异常，default异常
--fmt中的格式正确，边界检验
--day正数越界
SELECT cast ('.123000' as timestamp  DEFAULT '31-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day负数越界
SELECT cast ('.123000'  as timestamp DEFAULT '-01-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day为单数
SELECT cast ('.123000'  as timestamp DEFAULT '1-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day为越界三位数
SELECT cast ('.123000'  as timestamp DEFAULT '131-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day为正常三位数
SELECT cast ('.123000'  as timestamp DEFAULT '001-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--day不输入
SELECT cast ('.123000' as timestamp  DEFAULT '-09-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF')l;

--month正数越界
SELECT cast ('.123000' as timestamp  DEFAULT '01-13-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--month负数越界
SELECT cast ('.123000'  as timestamp DEFAULT '01--13-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--month为01
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--month为正常三位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-001-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--month为越界三位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-111-02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--month不输入
SELECT cast ('.123000'  as timestamp DEFAULT '01--02 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--year为RR
--year为正常四位数
SELECT cast ('.123000' as timestamp  DEFAULT '11-09-2002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为负数四位数越界
SELECT cast ('.123000'  as timestamp DEFAULT '01-01--2002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为越界四位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-9999 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为三位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-999 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为不越界5位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为越界5位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-20002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year不输入
SELECT cast ('.123000' as timestamp  DEFAULT '01-01 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--year为YYYY
--year为正常四位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-2002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为负数四位数越界
--无S指定负号
SELECT cast ('.123000' as timestamp  DEFAULT '01-01--2002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--有S指定负号
SELECT cast ('dasd' as timestamp default'01-01--2002 14:10:10.123000'on conversion error,'DD-MM-SYYYY HH24:MI:SS.FF');
--year为越界四位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-9999 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为3位数位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-999 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为不越界5位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year为越界五位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-20002 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--year不输入
SELECT cast ('.123000' as timestamp  DEFAULT '01-01 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--hour两位数越界
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 25:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--hour两位数负数越界
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02 -20:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF') ;
--hour为一位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02 1:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF') ;
--hour为越界三位数
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02 100:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--hour为不越界三位数
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 010:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--hour不输入
SELECT cast ('.123000' as timestamp DEFAULT '01-01-02 :10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--两位数越界
SELECT cast ('dasd' as timestamp default'01-01-02 10:70:10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--负数分钟越界
SELECT cast ('dasd' as timestamp default'01-01-02 10:-10:10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--一位数分钟
SELECT cast ('dasd' as timestamp default'01-01-02 10:1:10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--三位数分钟越界
SELECT cast ('dasd' as timestamp default'01-01-02 10:100:10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--三位数分钟不越界
SELECT cast ('dasd' as timestamp default'01-01-02 10:010:10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--分钟不输入
SELECT cast ('dasd' as timestamp default'01-01-02 10::10.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');

--正常两位数秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--越界两位数秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:70.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--一位数秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:7.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--越界负两位数秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:-60.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--不越界三位数秒
SELECT cast ('dasd' as timestamp default'01-01-02 10:10:050.123000'on conversion error,'DD-MM-RR HH24:MI:SS.FF');
--越界三位数秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:770.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--秒不输入
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--三位数壕秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:40.123' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--毫秒位数越界
SELECT cast ('.123000'  as timestamp DEFAULT '01-01-02 10:10:10.123987892374' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--毫秒位数为0
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02 10:10:10' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--少输入年月日
SELECT cast ('.123000'  as timestamp DEFAULT '10:10:10.123974' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--少输入时分秒
SELECT cast ('.123000' as timestamp  DEFAULT '01-01-02' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--default空值检验
--为null
SELECT cast ('.123000' as timestamp  DEFAULT null ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--为''
SELECT cast ('.123000' as timestamp  DEFAULT '' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--什么都不输
SELECT cast ('.123000'  as timestamp DEFAULT ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--特殊日期检验
--闰年2月29检验
SELECT cast ('4:10:10.123000' as timestamp  DEFAULT '29-02-04 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');
--普通年2月29检验
SELECT cast ('2914:10:10.123000' as timestamp  DEFAULT '29-02-03 14:10:10.123000' ON CONVERSION ERROR,'DD-MM-RR HH24:MI:SS.FF');

--sqlplan检验
SELECT cast ('01-Jan-03 14:10:10.123000'as timestamp DEFAULT '11-Jan-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Feb-03 14:10:10.123000' as timestamp DEFAULT '11-Feb-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Mar-03 14:10:10.123000' as timestamp DEFAULT '11-Mar-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Apr-03 14:10:10.123000' as timestamp DEFAULT '11-Apr-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-May-03 14:10:10.123000' as timestamp DEFAULT '11-May-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Jun-03 14:10:10.123000' as timestamp DEFAULT '11-Jun-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Jul-03 14:10:10.123000' as timestamp DEFAULT '11-Jul-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Aug-03 14:10:10.123000' as timestamp DEFAULT '11-Aug-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Sep-03 14:10:10.123000' as timestamp DEFAULT '11-Sep-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Oct-03 14:10:10.123000' as timestamp DEFAULT '11-Oct-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Nov-03 14:10:10.123000' as timestamp DEFAULT '11-Nov-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');
SELECT cast ('01-Dec-03 14:10:10.123000' as timestamp DEFAULT '11-Dec-11 14:10:10.123000' ON CONVERSION ERROR,'DD-Mon-RR HH24:MI:SS.FF');

SELECT cast('1' as numeric default '123' on conversion error,'nls_date_language = American');
SELECT cast('1s' as numeric default '123' on conversion error,'9999');
SELECT cast('1ea' as numeric default '123' on conversion error);
SELECT cast('1' as number, 'nls_date_language = American');
