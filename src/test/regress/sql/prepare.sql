-- Regression tests for prepareable statements. We query the content
-- of the pg_prepared_statements view as prepared statements are
-- created and removed.

SELECT name, statement, parameter_types FROM pg_prepared_statements;

PREPARE q1 AS SELECT 1 AS a;
EXECUTE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- should fail
PREPARE q1 AS SELECT 2;

-- should succeed
DEALLOCATE q1;
PREPARE q1 AS SELECT 2;
EXECUTE q1;

PREPARE q2 AS SELECT 2 AS b;
SELECT name, statement, parameter_types FROM pg_prepared_statements ORDER BY name;

-- sql92 syntax
DEALLOCATE PREPARE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

DEALLOCATE PREPARE q2;
-- the view should return the empty set again
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- parameterized queries
PREPARE q2(text) AS
	SELECT datname, datistemplate, datallowconn
	FROM pg_database WHERE datname = $1;

EXECUTE q2('postgres');

PREPARE q3(text, int, float, boolean, smallint) AS
	SELECT * FROM tenk1 WHERE string4 = $1 AND (four = $2 OR
	ten = $3::bigint OR true = $4 OR odd = $5::int)
	ORDER BY unique1;

EXECUTE q3('AAAAxx', 5::smallint, 10.5::float, false, 4::bigint);

-- too few params
EXECUTE q3('bool');

-- too many params
EXECUTE q3('bytea', 5::smallint, 10.5::float, false, 4::bigint, true);

-- wrong param types
EXECUTE q3(5::smallint, 10.5::float, false, 4::bigint, 'bytea');

-- invalid type
PREPARE q4(nonexistenttype) AS SELECT $1;


-- unknown or unspecified parameter types: should succeed
PREPARE q6 AS
    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2;
PREPARE q7(unknown) AS
    SELECT * FROM road WHERE thepath = $1;

SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

-- test DEALLOCATE ALL;
DEALLOCATE ALL;
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

CREATE TABLE create_columnar_table_012
(
 
    c_smallint smallint null,
    c_date text
)
PARTITION BY RANGE(c_smallint)
(
    PARTITION create_columnar_table_partition_p1 VALUES LESS THAN(1),
    PARTITION create_columnar_table_partition_p2 VALUES LESS THAN(3),
    PARTITION create_columnar_table_partition_p3 VALUES LESS THAN(7),
    PARTITION create_columnar_table_partition_p4 VALUES LESS THAN(2341),
    PARTITION create_columnar_table_partition_p5 VALUES LESS THAN(11121),
    PARTITION create_columnar_table_partition_p6 VALUES LESS THAN(22222)
);

PREPARE fooplan (int) AS SELECT count(1) FROM create_columnar_table_012 WHERE c_smallint <= $1;

EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);

SET plan_cache_mode = force_generic_plan;
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);
SET plan_cache_mode = force_custom_plan;
EXPLAIN (costs OFF, VERBOSE OFF) EXECUTE fooplan(3);

DROP TABLE create_columnar_table_012;
DEALLOCATE PREPARE fooplan;
