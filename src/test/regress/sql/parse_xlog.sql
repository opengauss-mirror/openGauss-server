-- gs_xlogdump_lsn
START TRANSACTION;

CREATE OR REPLACE FUNCTION gs_xlogdump_lsn()
RETURNS table (output text)
LANGUAGE plpgsql
AS
$$
DECLARE
    param1  text;
    param2  text;
BEGIN
    SELECT pg_current_xlog_location() into param1;
    CHECKPOINT;
    SELECT pg_current_xlog_location() into param2;
    return query SELECT gs_xlogdump_lsn(''|| param1 || '', ''|| param2 || '');
END;
$$
;

SELECT gs_xlogdump_lsn();
COMMIT;

-- gs_xlogdump_xid
SELECT gs_xlogdump_xid('200');


-- gs_xlogdump_tablepath
START TRANSACTION;

CREATE OR REPLACE FUNCTION gs_xlogdump_tablepath()
RETURNS table (output text)
LANGUAGE plpgsql
AS
$$
DECLARE
    param1  text;
    param2  text;
BEGIN
    DROP TABLE IF EXISTS heap_t;
    CREATE TABLE heap_t (i INT);
    INSERT INTO heap_t SELECT * FROM generate_series(1,10);
    CHECKPOINT;
    SELECT pg_relation_filepath('heap_t') into param1;
    return query SELECT gs_xlogdump_tablepath(''|| param1 || '', 0, 'heap');
END;
$$
;

SELECT gs_xlogdump_tablepath();
DROP TABLE heap_t;
COMMIT;

-- gs_xlogdump_parsepage_tablepath
START TRANSACTION;

CREATE OR REPLACE FUNCTION gs_xlogdump_parsepage_tablepath()
RETURNS table (output text)
LANGUAGE plpgsql
AS
$$
DECLARE
    param1  text;
    param2  text;
BEGIN
    DROP TABLE IF EXISTS heap_t1;
    CREATE TABLE heap_t1 (i INT);
    INSERT INTO heap_t1 SELECT * FROM generate_series(1,10);
    CHECKPOINT;
    SELECT pg_relation_filepath('heap_t1') into param1;
    return query SELECT gs_xlogdump_parsepage_tablepath(''|| param1 || '', 0, 'heap', false);
END;
$$
;

SELECT gs_xlogdump_parsepage_tablepath();
DROP TABLE heap_t1;
COMMIT;
