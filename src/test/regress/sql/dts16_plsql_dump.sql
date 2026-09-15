CREATE SCHEMA dts16_plsql_dump;
SET CURRENT_SCHEMA = 'dts16_plsql_dump';

CREATE TABLE tb (c1 int, c2 varchar);
INSERT INTO tb SELECT generate_series(1, 10), generate_series(1, 10) || 'a';

CREATE OR REPLACE FUNCTION add(a integer, b integer) RETURNS integer
LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER AS $$
# option dump
BEGIN
    DECLARE
        CURSOR cur IS SELECT * FROM tb;
    BEGIN
        FOR i IN cur LOOP
            a := a + i.c1;
            b := b + 1;
        END LOOP;
    END;
    RETURN a + b;
END$$;

DROP TABLE tb;
DROP FUNCTION add(integer, integer);
RESET CURRENT_SCHEMA;
DROP SCHEMA dts16_plsql_dump CASCADE;
