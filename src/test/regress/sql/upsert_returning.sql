DROP SCHEMA test_upsert_returning CASCADE;
CREATE SCHEMA test_upsert_returning;
SET CURRENT_SCHEMA TO test_upsert_returning;


CREATE TEMP TABLE foo (f1 int unique, f2 text, f3 int default 42);
CREATE TEMP TABLE foo_2 (a int, b int);
INSERT INTO foo_2 select generate_series(1, 5), generate_series(1, 2);

INSERT INTO foo (f1, f2, f3)
    VALUES (1, 'test', DEFAULT), (2, 'More', 11), (3, upper('more'), 7+9) ON DUPLICATE KEY UPDATE f3 = f3+1
    RETURNING *, f1+f3 AS sum;

SELECT * FROM foo ORDER BY f1;

with t as
(
    INSERT INTO foo SELECT f1+1, f2, f3+2 FROM foo order by f3 ON DUPLICATE KEY UPDATE f3 = f3+10 RETURNING foo.*, '1'
)
select * from t;


-- error
INSERT INTO foo SELECT f1+1, f2, f3+2 FROM foo order by f3 ON DUPLICATE KEY UPDATE f1 = f1+1 RETURNING f3, f2, f1, least(f1,f3);

INSERT INTO foo SELECT f1+1, f2, f3+2 FROM foo order by f3 ON DUPLICATE KEY UPDATE f3 = f3+1 RETURNING f3, f2, f1, least(f1,f3);

-- update nothing
INSERT INTO foo SELECT f1+1, f2, f3+2 FROM foo order by f3 ON DUPLICATE KEY UPDATE f3 = f3 RETURNING f3, f2, f1, least(f1,f3);

SELECT * FROM foo ORDER BY f1;

-- Subplans and initplans in the RETURNING list

INSERT INTO foo SELECT f1+1, f2, f3+99 FROM foo order by 1, 2, 3 ON DUPLICATE KEY UPDATE f3 =f3+10 RETURNING *, f1 - 3 IN
(SELECT b FROM foo_2) AS subplan, EXISTS(SELECT * FROM foo_2) AS initplan;

SELECT * FROM foo order by 1,2,3;

DROP SCHEMA test_upsert_returning CASCADE;