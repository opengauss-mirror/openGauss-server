-- FOR PL/pgSQL RAISE_APPLICATION_ERROR scenarios --

-- check compatibility --
show sql_compatibility; -- expect ORA --

-- create new schema --
drop schema if exists plpgsql_custom_exception;
create schema plpgsql_custom_exception;
set current_schema = plpgsql_custom_exception;

-- test within the range --
CREATE OR REPLACE PROCEDURE account_status (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN                   -- explicitly raise exception
    RAISE_APPLICATION_ERROR(-20000, 'Account past due.'); -- within the range -20999 ~ -20000
  END IF;
END;
/
 
DECLARE
  past_due  EXCEPTION;                       -- declare exception
  PRAGMA EXCEPTION_INIT (past_due, -20000);  -- assign error code to exception
BEGIN
  account_status (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));   -- invoke procedure

EXCEPTION
	WHEN past_due THEN
		RAISE INFO 'past_due exception: %', SQLERRM;
END;
/

-- test out of range --
CREATE OR REPLACE PROCEDURE account_status (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN                   -- explicitly raise exception
    RAISE_APPLICATION_ERROR(-30000, 'Account past due.'); -- out of range -20999 ~ -20000
  END IF;
END;
/

DECLARE
  past_due  EXCEPTION;                       -- declare exception
  PRAGMA EXCEPTION_INIT (past_due, -20000);  -- assign error code to exception
BEGIN
  account_status (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));   -- invoke procedure

EXCEPTION
	WHEN past_due THEN
		RAISE INFO 'past_due exception: %', SQLERRM;
	WHEN OTHERS THEN
		RAISE INFO 'others exception: %', SQLERRM;
END;
/

-- test parameter code is null --
CREATE OR REPLACE PROCEDURE account_status (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN                   -- explicitly raise exception
    RAISE_APPLICATION_ERROR(NULL, 'Account past due.'); -- out of range -20999 ~ -20000
  END IF;
END;
/

DECLARE
  past_due  EXCEPTION;                       -- declare exception
  PRAGMA EXCEPTION_INIT (past_due, -20000);  -- assign error code to exception
BEGIN
  account_status (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));   -- invoke procedure

EXCEPTION
	WHEN past_due THEN
		RAISE INFO 'past_due exception: %', SQLERRM;
	WHEN OTHERS THEN
		RAISE INFO 'others exception: %', SQLERRM;
END;
/

-- test parameter message is null --
CREATE OR REPLACE PROCEDURE account_status (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN                   -- explicitly raise exception
    RAISE_APPLICATION_ERROR(-20000, NULL); -- out of range -20999 ~ -20000
  END IF;
END;
/

DECLARE
  past_due  EXCEPTION;                       -- declare exception
  PRAGMA EXCEPTION_INIT (past_due, -20000);  -- assign error code to exception
BEGIN
  account_status (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));   -- invoke procedure

EXCEPTION
	WHEN past_due THEN
		RAISE INFO 'past_due exception: %', SQLERRM;
	WHEN OTHERS THEN
		RAISE INFO 'others exception: %', SQLERRM;
END;
/

-- test parameter keep_errors is null --
CREATE OR REPLACE PROCEDURE account_status (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN                   -- explicitly raise exception
    RAISE_APPLICATION_ERROR(-20000, 'Account past due.', NULL); -- out of range -20999 ~ -20000
  END IF;
END;
/

DECLARE
  past_due  EXCEPTION;                       -- declare exception
  PRAGMA EXCEPTION_INIT (past_due, -20000);  -- assign error code to exception
BEGIN
  account_status (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));   -- invoke procedure

EXCEPTION
	WHEN past_due THEN
		RAISE INFO 'past_due exception: %', SQLERRM;
	WHEN OTHERS THEN
		RAISE INFO 'others exception: %', SQLERRM;
END;
/

----------------------
-- test keep errors --
----------------------
CREATE TABLE t1 (id INT PRIMARY KEY,name text);
INSERT INTO t1 VALUES (1,'ABC');

-- scenarios 1
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 2
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 3
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 4
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 5
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/
 
DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 6
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 7
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE);
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 8
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.'); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 9
-- account_status1(keep_errors=false)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE);
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.'); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 10
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.'); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 11
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.'); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 12
-- account_status1(keep_errors=true)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.'); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 13
-- account_status1(insert error)
-- account_status2(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 14
-- account_status1(insert error)
-- account_status2(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status2(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status2 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 15
-- account_status1(insert error)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 16
-- account_status1(insert error)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',FALSE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 17
-- account_status1(insert error)
-- account_status2(keep_errors=false)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.'); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 18
-- account_status1(insert error)
-- account_status2(keep_errors=true)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20001, 'Account past due 2.',TRUE); 
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',FALSE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 19
-- account_status1(keep_errors=true)
-- account_status2(insert error)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today);
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/

DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 20
-- account_status1(keep_errors=false)
-- account_status2(insert error)
-- account_status3(keep_errors=true)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today);
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.',TRUE); 
END;
/
 
DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 21
-- account_status1(keep_errors=false)
-- account_status2(insert error)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.');
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today);
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.'); 
END;
/
 
DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- scenarios 22
-- account_status1(keep_errors=true)
-- account_status2(insert error)
-- account_status3(keep_errors=false)
CREATE OR REPLACE PROCEDURE account_status1 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  IF due_date < today THEN
    RAISE_APPLICATION_ERROR(-20000, 'Account past due 1.', TRUE);
  END IF;
END;
/

CREATE OR REPLACE PROCEDURE account_status2 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status1 (due_date,today);
EXCEPTION
	WHEN OTHERS THEN
		INSERT INTO t1 VALUES (1,'ABC');
END;
/

CREATE OR REPLACE PROCEDURE account_status3 (
  due_date DATE,
  today    DATE
)
IS
BEGIN
  account_status2 (due_date,today); 
EXCEPTION
	WHEN OTHERS THEN
		RAISE_APPLICATION_ERROR(-20002, 'Account past due 3.'); 
END;
/
 
DECLARE
BEGIN
  account_status3(TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),
                  TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
END;
/

DECLARE
BEGIN
  account_status3 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'), 
				           TO_DATE('09-JUL-2010', 'DD-MON-YYYY'));
EXCEPTION
	WHEN OTHERS THEN
		RAISE INFO 'CATCH EXCEPTION: %', SQLERRM;
END;
/

-- test package and package proc has same exception
-- only declare in package proc
CREATE OR REPLACE PACKAGE pkg_09 AS
	PROCEDURE proc2_09(due_date DATE,today DATE);
END pkg_09;
/
CREATE OR REPLACE PACKAGE BODY pkg_09 AS
    PROCEDURE proc2_09(due_date DATE,today DATE)
        IS
        e1 EXCEPTION;
        PRAGMA EXCEPTION_INIT (e1, -20121);
        BEGIN
            IF due_date < today THEN
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
            END IF;
            EXCEPTION WHEN E1 THEN
                raise info 'catch e1';
        END;
    END pkg_09;
/
BEGIN
  pkg_09.proc2_09 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
end;
/
drop package pkg_09;

-- only declare in package
CREATE OR REPLACE PACKAGE pkg_09 AS
	e1 EXCEPTION;
	PROCEDURE proc2_09(due_date DATE,today DATE);
END pkg_09;
/
CREATE OR REPLACE PACKAGE BODY pkg_09 AS
    PROCEDURE proc2_09(due_date DATE,today DATE)
        IS
        PRAGMA EXCEPTION_INIT (e1, -20121);
        BEGIN
            IF due_date < today THEN
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
            END IF;
            EXCEPTION WHEN E1 THEN
                raise info 'catch e1';
        END;
    END pkg_09;
/
BEGIN
  pkg_09.proc2_09 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
end;
/
drop package pkg_09;

--declare in package an package proc
CREATE OR REPLACE PACKAGE pkg_09 AS
	e1 EXCEPTION;
	PROCEDURE proc2_09(due_date DATE,today DATE);
END pkg_09;
/
CREATE OR REPLACE PACKAGE BODY pkg_09 AS
    PROCEDURE proc2_09(due_date DATE,today DATE)
        IS
        e1 EXCEPTION;
        PRAGMA EXCEPTION_INIT (e1, -20121);
        BEGIN
            IF due_date < today THEN
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
                RAISE_APPLICATION_ERROR(-20121, 'Account past due.');
            END IF;
            EXCEPTION WHEN E1 THEN
                raise info 'catch e1';
        END;
    END pkg_09;
/
BEGIN
  pkg_09.proc2_09 (TO_DATE('01-JUL-2010', 'DD-MON-YYYY'),TO_DATE('09-JUL-2010', 'DD-MON-YYYY')); 
end;
/
drop package pkg_09;

-- clean up --
drop schema if exists plpgsql_custom_exception cascade;
