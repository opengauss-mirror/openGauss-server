DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t(a INT, b INT);
INSERT INTO t VALUES(1,1), (2,3);
CREATE TABLE t1(a INT, b INT);
INSERT INTO t1 VALUES(1,1), (2,2);
CREATE TABLE t2(a INT, b INT);
INSERT INTO t2 VALUES(1,1), (2,2);
CREATE TABLE t3(a INT, b INT, c INT);
INSERT INTO t3 VALUES(1,1,1), (2,2,2);

show d_format_behavior_compat_options;
set d_format_behavior_compat_options = 'enable_sbr_identifier, default_collation, disable_target_alias';
-- alias not use
SELECT a = 1;
SELECT [a] = 1;
SELECT "a" = 1;
SELECT [a b] = 1;
SELECT "a b" = 1;
SELECT a = 'hello';
SELECT a = (SELECT 1);
SELECT a = b FROM t;
SELECT x = a + b FROM t;
SELECT cnt = count(*) FROM t;
SELECT a1 = a, b1 = b FROM t;
SELECT a1 = a, b AS b1 FROM t;
SELECT a = t.b FROM t;
SELECT sum_col = sum(t.b) FROM t;
SELECT "a" = b FROM t;
SELECT [a] = b FROM t;
SELECT "AliasName" = b + 1 FROM t;
SELECT a1 = b FROM t ORDER BY a1;
SELECT a = b, a = c FROM t3;
SELECT [SELECT] = b FROM t;
SELECT "SELECT" = b FROM t;
SELECT a=a+1, "B"=b, c, c as d FROM t3;

reset d_format_behavior_compat_options;
show d_format_behavior_compat_options;
-- alias use
SELECT a = 1;
SELECT [a] = 1;
SELECT "a" = 1;
SELECT [a b] = 1;
SELECT "a b" = 1;
SELECT a = 'hello';
SELECT a = (SELECT 1);
SELECT a = b FROM t;
SELECT x = a + b FROM t;
SELECT cnt = count(*) FROM t;
SELECT a1 = a, b1 = b FROM t;
SELECT a1 = a, b AS b1 FROM t;
SELECT a = t.b FROM t;
SELECT sum_col = sum(t.b) FROM t;
SELECT "a" = b FROM t;
SELECT [a] = b FROM t;
SELECT "AliasName" = b + 1 FROM t;
SELECT a1 = b FROM t ORDER BY a1;
SELECT a = b, a = c FROM t3;
SELECT [SELECT] = b FROM t;
SELECT "SELECT" = b FROM t;
SELECT a=a+1, "B"=b, c, c as d FROM t3;

-- not alias
SELECT GETDATE() = GETDATE();
SELECT (SELECT 1) = 1;
SELECT 1 + 2 = 3;
SELECT CAST(1 AS INT) = 1;
SELECT (a + 1) = b FROM t;
SELECT t.a = b FROM t;

SELECT * FROM t WHERE a = b;
SELECT * FROM t1 JOIN t2 ON t1.a = t2.b;
SELECT count(*) FROM t HAVING count(*) = 1;
SELECT CASE WHEN a = b THEN 1 END AS a FROM t;

CREATE OR REPLACE PROCEDURE check_number(num IN INTEGER)
AS
BEGIN
    IF num = 0 THEN
        RAISE NOTICE 'num = 0';
    ELSIF num = 1 THEN
        RAISE NOTICE 'num = 1';
    ELSE 
        RAISE NOTICE 'num != 0 and num != 1';
    END IF;
END;
/

CALL check_number(0);
CALL check_number(1);
CALL check_number(2);
