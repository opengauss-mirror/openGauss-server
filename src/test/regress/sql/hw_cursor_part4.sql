--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
CREATE schema hw_cursor_part4;
set current_schema = hw_cursor_part4;
set behavior_compat_options = 'skip_insert_gs_source';
CREATE TABLE TBL(V1 INTEGER);
INSERT INTO TBL VALUES(1);
INSERT INTO TBL VALUES(2);
CREATE OR REPLACE PROCEDURE SP_TEST 
AS
    CURSOR CUR IS
        SELECT * FROM TBL;
BEGIN 
    --EXPLICIT CURSOR ATTRIBUTES INITIAL STATUS
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN INITIAL STATUS BEFORE OPEN : NULL';
    END IF;
    OPEN CUR;
    IF CUR%FOUND THEN 
        raise notice '%','CUR%FOUND INITIAL STATUS : TRUE';
    ELSIF NOT CUR%FOUND THEN 
        raise notice '%','CUR%FOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','CUR%FOUND INITIAL STATUS : NULL';
    END IF;
    
    IF CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : TRUE';
    ELSIF NOT CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','CUR%NOTFOUND INITIAL STATUS : NULL';
    END IF;
    
    raise notice 'CUR%%ROWCOUNT INITIAL STATUS :%',NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
    
    CLOSE CUR;
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN STATUS AFTER CLOSE : NULL';
    END IF;
    
    --IMPLICIT CURSOR ATTRIBUTES INITIAL STATUS 
    IF SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN INITIAL STATUS : TRUE';
    ELSIF NOT SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','SQL%ISOPEN INITIAL STATUS : NULL';
    END IF;

    IF SQL%FOUND THEN 
        raise notice '%','SQL%FOUND INITIAL STATUS : TRUE';
    ELSIF NOT SQL%FOUND THEN 
        raise notice '%','SQL%FOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','SQL%FOUND INITIAL STATUS : NULL';
    END IF;
    
    IF SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND INITIAL STATUS : TRUE';
    ELSIF NOT SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND INITIAL STATUS : FALSE';
    ELSE
        raise notice '%','SQL%NOTFOUND INITIAL STATUS : NULL';
    END IF;
    
    raise notice 'SQL%%ROWCOUNT INITIAL STATUS : %',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');
END;
/
CALL SP_TEST();
DROP TABLE TBL;
DROP PROCEDURE SP_TEST;

-- GAUSSDBV100R001C00-754 
CREATE TABLE TBL_H248LNK_INFO(ULBSGMODULENO INTEGER);
INSERT INTO TBL_H248LNK_INFO VALUES(123);
INSERT INTO TBL_H248LNK_INFO VALUES(456);
INSERT INTO TBL_H248LNK_INFO VALUES(789);
CREATE TABLE TBL (I_MODULENO INTEGER);

CREATE OR REPLACE PROCEDURE TEST_CURSOR_4 
AS
        TYPE CUR_TYPE IS REF CURSOR;
        CUR CUR_TYPE;
        PSV_SQL VARCHAR2(1000);
        PI_MODULENO INTEGER;
        TBL_STR VARCHAR2(1000) := 'TBL';
BEGIN
        OPEN CUR FOR SELECT DISTINCT ULBSGMODULENO FROM TBL_H248LNK_INFO;
        LOOP
            FETCH CUR INTO PI_MODULENO;
            EXIT WHEN CUR%NOTFOUND;            
            PSV_SQL := 'BEGIN INSERT INTO TBL (I_MODULENO) VALUES('||PI_MODULENO||');END;';
            EXECUTE IMMEDIATE PSV_SQL;

            -- check cursor attris status
            IF CUR%ISOPEN THEN 
                raise notice '%','CUR%ISOPEN : TRUE';
            ELSIF NOT CUR%ISOPEN THEN 
                raise notice '%','CUR%ISOPEN : FALSE';
            ELSE
                raise notice '%','CUR%ISOPEN : NULL';
            END IF;
            IF CUR%FOUND THEN 
                raise notice '%','CUR%FOUND : TRUE';
            ELSIF NOT CUR%FOUND THEN 
                raise notice '%','CUR%FOUND : FALSE';
            ELSE
                raise notice '%','CUR%FOUND : NULL';
            END IF;            
            IF CUR%NOTFOUND THEN 
                raise notice '%','CUR%NOTFOUND : TRUE';
            ELSIF NOT CUR%NOTFOUND THEN 
                raise notice '%','CUR%NOTFOUND : FALSE';
            ELSE
                raise notice '%','CUR%NOTFOUND : NULL';
            END IF;            
            raise notice 'CUR%%ROWCOUNT :%',NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
            IF SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : TRUE';
            ELSIF NOT SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : FALSE';
            ELSE
                raise notice '%','SQL%ISOPEN : NULL';
            END IF;
            IF SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : TRUE';
            ELSIF NOT SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : FALSE';
            ELSE
                raise notice '%','SQL%FOUND : NULL';
            END IF;            
            IF SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : TRUE';
            ELSIF NOT SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : FALSE';
            ELSE
                raise notice '%','SQL%NOTFOUND : NULL';
            END IF;            
            raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');

        END LOOP;
        
    -- check cursor attris status
    IF CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN : TRUE';
    ELSIF NOT CUR%ISOPEN THEN 
        raise notice '%','CUR%ISOPEN : FALSE';
    ELSE
        raise notice '%','CUR%ISOPEN : NULL';
    END IF;
    IF CUR%FOUND THEN 
        raise notice '%','CUR%FOUND : TRUE';
    ELSIF NOT CUR%FOUND THEN 
        raise notice '%','CUR%FOUND : FALSE';
    ELSE
        raise notice '%','CUR%FOUND : NULL';
    END IF;            
    IF CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND : TRUE';
    ELSIF NOT CUR%NOTFOUND THEN 
        raise notice '%','CUR%NOTFOUND : FALSE';
    ELSE
        raise notice '%','CUR%NOTFOUND : NULL';
    END IF;            
    raise notice 'CUR%%ROWCOUNT :%',NVL(TO_CHAR(CUR%ROWCOUNT),'NULL');
    IF SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN : TRUE';
    ELSIF NOT SQL%ISOPEN THEN 
        raise notice '%','SQL%ISOPEN : FALSE';
    ELSE
        raise notice '%','SQL%ISOPEN : NULL';
    END IF;
    IF SQL%FOUND THEN 
        raise notice '%','SQL%FOUND : TRUE';
    ELSIF NOT SQL%FOUND THEN 
        raise notice '%','SQL%FOUND : FALSE';
    ELSE
        raise notice '%','SQL%FOUND : NULL';
    END IF;            
    IF SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND : TRUE';
    ELSIF NOT SQL%NOTFOUND THEN 
        raise notice '%','SQL%NOTFOUND : FALSE';
    ELSE
        raise notice '%','SQL%NOTFOUND : NULL';
    END IF;            
    raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');
END;
/
CALL TEST_CURSOR_4();
DROP PROCEDURE TEST_CURSOR_4;
DROP TABLE TBL_H248LNK_INFO;
DROP TABLE TBL;

CREATE TABLE TBL_RCWSCFG (
    IWSNO INTEGER,
    USCDBMID SMALLINT,
    USWSBOARDMID SMALLINT,
    UCWSTYPE8100 SMALLINT,
    UCWSTYPE6600 SMALLINT,
    UCLOGINTYPE SMALLINT,
    UCTTSCAPABILITY SMALLINT,
    UCASRCAPABILITY SMALLINT,
    UCRESCAPABILITY CHARACTER VARYING(8)
);
INSERT INTO TBL_RCWSCFG VALUES (0, 184, 472, 0, 1, 0, NULL, NULL, '11011000');

CREATE TABLE TBL_TEMP_MODULE_312 (
    I_MODULENO INTEGER
);
CREATE OR REPLACE PROCEDURE TEST_TEMP
AS
BEGIN
        raise notice '%','TEST_TEMP';
END;
/
CREATE OR REPLACE PROCEDURE TEST_CRS_RPT_EMPTYSOR(FLAG INTEGER)
AS
    TYPE T_PSTMT_CRS_RPT_EMPTY IS REF CURSOR;
    CRS_RPT_EMPTY T_PSTMT_CRS_RPT_EMPTY;
	PI_MODULENO INTEGER;
	PSV_MODULETBLNAME VARCHAR2(128) := 'TBL_TEMP_MODULE_312';
	PSV_SQL  VARCHAR2(128);
	V_TEMP INTEGER := 0;
	PI_NN INTEGER := NULL;
BEGIN
	OPEN CRS_RPT_EMPTY FOR SELECT DISTINCT USCDBMID FROM TBL_RCWSCFG WHERE IWSNO >=0 AND IWSNO <= 0;
	LOOP
		FETCH CRS_RPT_EMPTY INTO PI_MODULENO;
		EXIT WHEN CRS_RPT_EMPTY%NOTFOUND;
		IF (FLAG = 0) THEN 
			-- INSERT INTO TBL_TEMP_MODULE_312, INSERT TRIGGER FUNCTION CALLED
			PSV_SQL := 'BEGIN INSERT INTO '||PSV_MODULETBLNAME||' (I_MODULENO) VALUES('||PI_MODULENO||');END;';
			EXECUTE IMMEDIATE PSV_SQL;
		ELSE
			TEST_TEMP();
		END IF;
	END LOOP;
	-- check cursor attris status
	IF CRS_RPT_EMPTY%ISOPEN THEN 
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%ISOPEN THEN 
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%ISOPEN : NULL';
	END IF;
	IF CRS_RPT_EMPTY%FOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%FOUND : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%FOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%FOUND : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%FOUND : NULL';
	END IF;            
	IF CRS_RPT_EMPTY%NOTFOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : TRUE';
	ELSIF NOT CRS_RPT_EMPTY%NOTFOUND THEN 
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : FALSE';
	ELSE
                raise notice '%','CRS_RPT_EMPTY%NOTFOUND : NULL';
	END IF;            
        raise notice 'CRS_RPT_EMPTY%%ROWCOUNT :%',NVL(TO_CHAR(CRS_RPT_EMPTY%ROWCOUNT),'NULL');
	IF SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : TRUE';
	ELSIF NOT SQL%ISOPEN THEN 
                raise notice '%','SQL%ISOPEN : FALSE';
	ELSE
                raise notice '%','SQL%ISOPEN : NULL';
	END IF;
	IF SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : TRUE';
	ELSIF NOT SQL%FOUND THEN 
                raise notice '%','SQL%FOUND : FALSE';
	ELSE
                raise notice '%','SQL%FOUND : NULL';
	END IF;            
	IF SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : TRUE';
	ELSIF NOT SQL%NOTFOUND THEN 
                raise notice '%','SQL%NOTFOUND : FALSE';
	ELSE
                raise notice '%','SQL%NOTFOUND : NULL';
	END IF;            
        raise notice 'SQL%%ROWCOUNT :%',NVL(TO_CHAR(SQL%ROWCOUNT),'NULL');
END;
/
CALL TEST_CRS_RPT_EMPTYSOR(0);
CALL TEST_CRS_RPT_EMPTYSOR(1);

DROP schema hw_cursor_part4 CASCADE;

