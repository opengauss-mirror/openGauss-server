CREATE TABLE test1 (a int, b text);

CREATE PROCEDURE transaction_test1()
AS
BEGIN
    FOR i IN 0..9 LOOP
        INSERT INTO test1 (a) VALUES (i);
        IF i % 2 = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
END;
/

CALL transaction_test1();

SELECT * FROM test1;


TRUNCATE test1;

DO
LANGUAGE plpgsql
$$
BEGIN
    FOR i IN 0..9 LOOP
        INSERT INTO test1 (a) VALUES (i);
        IF i % 2 = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
END
$$;

SELECT * FROM test1;


-- transaction commands not allowed when called in transaction block
START TRANSACTION;
CALL transaction_test1();
COMMIT;

START TRANSACTION;
DO LANGUAGE plpgsql $$ BEGIN COMMIT; END $$;
COMMIT;


TRUNCATE test1;

-- not allowed in a function
CREATE FUNCTION transaction_test2() RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    FOR i IN 0..9 LOOP
        INSERT INTO test1 (a) VALUES (i);
        IF i % 2 = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
    RETURN 1;
END
$$;

SELECT transaction_test2();

SELECT * FROM test1;


-- also not allowed if procedure is called from a function
CREATE FUNCTION transaction_test3() RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    CALL transaction_test1();
    RETURN 1;
END;
$$;

SELECT transaction_test3();

SELECT * FROM test1;


-- DO block inside function
CREATE FUNCTION transaction_test4() RETURNS int
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE 'DO LANGUAGE plpgsql $x$ BEGIN COMMIT; END $x$';
    RETURN 1;
END;
$$;

SELECT transaction_test4();


-- proconfig settings currently disallow transaction statements
CREATE PROCEDURE transaction_test5()
SET work_mem = 555
AS
BEGIN
    COMMIT;
END;
/

CALL transaction_test5();


-- commit inside cursor loop
CREATE TABLE test2 (x int);
INSERT INTO test2 VALUES (0), (1), (2), (3), (4);

TRUNCATE test1;

DO LANGUAGE plpgsql $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM test2 ORDER BY x LOOP
        INSERT INTO test1 (a) VALUES (r.x);
        COMMIT;
    END LOOP;
END;
$$;

SELECT * FROM test1;

-- check that this doesn't leak a holdable portal
SELECT * FROM pg_cursors;


-- error in cursor loop with commit
TRUNCATE test1;

DO LANGUAGE plpgsql $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM test2 ORDER BY x LOOP
        INSERT INTO test1 (a) VALUES (12/(r.x-2));
        COMMIT;
    END LOOP;
END;
$$;

SELECT * FROM test1;

SELECT * FROM pg_cursors;


-- rollback inside cursor loop
TRUNCATE test1;

DO LANGUAGE plpgsql $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM test2 ORDER BY x LOOP
        INSERT INTO test1 (a) VALUES (r.x);
        ROLLBACK;
    END LOOP;
END;
$$;

SELECT * FROM test1;

SELECT * FROM pg_cursors;


-- first commit then rollback inside cursor loop
TRUNCATE test1;

DO LANGUAGE plpgsql $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT * FROM test2 ORDER BY x LOOP
        INSERT INTO test1 (a) VALUES (r.x);
        IF r.x % 2 = 0 THEN
            COMMIT;
        ELSE
            ROLLBACK;
        END IF;
    END LOOP;
END;
$$;

SELECT * FROM test1;

SELECT * FROM pg_cursors;


-- rollback inside cursor loop
TRUNCATE test1;

DO LANGUAGE plpgsql $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN UPDATE test2 SET x = x * 2 RETURNING x LOOP
        INSERT INTO test1 (a) VALUES (r.x);
        ROLLBACK;
    END LOOP;
END;
$$;

SELECT * FROM test1;
SELECT * FROM test2;

SELECT * FROM pg_cursors;


-- commit inside block with exception handler
TRUNCATE test1;

DO LANGUAGE plpgsql $$
BEGIN
    BEGIN
        INSERT INTO test1 (a) VALUES (1);
        COMMIT;
        INSERT INTO test1 (a) VALUES (1/0);
        COMMIT;
    EXCEPTION
        WHEN division_by_zero THEN
            RAISE NOTICE 'caught division_by_zero';
    END;
END;
$$;

SELECT * FROM test1;


-- rollback inside block with exception handler
TRUNCATE test1;

DO LANGUAGE plpgsql $$
BEGIN
    BEGIN
        INSERT INTO test1 (a) VALUES (1);
        ROLLBACK;
        INSERT INTO test1 (a) VALUES (1/0);
        ROLLBACK;
    EXCEPTION
        WHEN division_by_zero THEN
            RAISE NOTICE 'caught division_by_zero';
    END;
END;
$$;

SELECT * FROM test1;


-- COMMIT failures
DO LANGUAGE plpgsql $$
BEGIN
    CREATE TABLE test3 (y int UNIQUE DEFERRABLE INITIALLY DEFERRED);
    COMMIT;
    INSERT INTO test3 (y) VALUES (1);
    COMMIT;
    INSERT INTO test3 (y) VALUES (1);
    INSERT INTO test3 (y) VALUES (2);
    COMMIT;
    INSERT INTO test3 (y) VALUES (3);  -- won't get here
END;
$$;

SELECT * FROM test3;


DROP TABLE test1;
DROP TABLE test2;
DROP TABLE test3;

--
create table test1(id int, name varchar(20));
insert into test1 values(1,'bbb');

CREATE OR REPLACE PROCEDURE PROC_OUT_PARAM_001(P1 OUT INT)
AS
BEGIN
select id into P1 from test1 where name = 'bbb';
insert into test1 values(P1,'ddd');
COMMIT;
insert into test1 values(P1,'eee');
ROLLBACK;
END;
/

DECLARE
V_P1 INT;
BEGIN
PROC_OUT_PARAM_001(V_P1);
END;
/

select * from test1;

start transaction;
select * from test1 for share;
select count(*) > 0 from dbe_perf.global_locks where transactionid is not null;
commit;

DROP TABLE test1;