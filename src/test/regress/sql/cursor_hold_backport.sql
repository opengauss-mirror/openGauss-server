-- PR 9664 / DTS13: holdable cursor state and volatile expressions.
CREATE SCHEMA cursor_hold_backport;
SET current_schema = cursor_hold_backport;

-- DTS13: commit in a cursor loop with a non-scroll cursor
SET client_min_messages = warning;
DROP TABLE IF EXISTS dts13_test, dts13_test_re;
DROP PROCEDURE IF EXISTS dts13_test_function();

CREATE TABLE dts13_test(b int, a int);
INSERT INTO dts13_test VALUES(generate_series(1, 200000), generate_series(1, 200000));
CREATE TABLE dts13_test_re(a int);

CREATE OR REPLACE PROCEDURE cursor_hold_backport.dts13_test_function()
SECURITY DEFINER
AS
result int;
vn_count int := 0;
vn_count2 int := 0;
DECLARE
BEGIN
    TRUNCATE TABLE dts13_test_re;
    FOR rec IN (SELECT sum(a) FROM dts13_test GROUP BY a)
    LOOP
        vn_count := vn_count + 1;
        INSERT INTO dts13_test_re VALUES(rec.sum);
        IF vn_count >= 1000 THEN
            COMMIT;
            vn_count := 0;
        END IF;
    END LOOP;
END;
/

SET work_mem = 64;
CALL cursor_hold_backport.dts13_test_function();

SELECT count(*) AS row_count, count(DISTINCT a) AS distinct_count FROM dts13_test_re;

SET work_mem = default;
SET client_min_messages = default;

DROP PROCEDURE dts13_test_function();
DROP TABLE dts13_test, dts13_test_re;

-- DTS13: preserve the current position of a non-scroll holdable cursor
START TRANSACTION;
DECLARE dts13_hold_cursor NO SCROLL CURSOR WITH HOLD FOR
    SELECT generate_series(1, 3);
\t on
FETCH 1 FROM dts13_hold_cursor;
COMMIT;
FETCH ALL FROM dts13_hold_cursor;
\t off
FETCH ABSOLUTE 1 FROM dts13_hold_cursor;
CLOSE dts13_hold_cursor;

-- A default cursor with volatile output must not re-execute consumed rows.
CREATE SEQUENCE cursor_volatile_seq;
BEGIN;
DECLARE volatile_hold_cursor CURSOR WITH HOLD FOR
    SELECT nextval('cursor_volatile_seq') AS v FROM generate_series(1, 3);
FETCH 1 FROM volatile_hold_cursor;
COMMIT;
FETCH ALL FROM volatile_hold_cursor;
CLOSE volatile_hold_cursor;
SELECT last_value AS volatile_calls FROM cursor_volatile_seq;
DROP SEQUENCE cursor_volatile_seq;
RESET current_schema;
DROP SCHEMA cursor_hold_backport;
