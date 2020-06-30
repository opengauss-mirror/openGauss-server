--
--FOR BLACKLIST FEATURE: REFERENCES、INHERITS、TRIGGER、SEQUENCE is not supported.
--

-- Test basic TRUNCATE functionality.
CREATE TABLE truncate_a (col1 integer primary key);
INSERT INTO truncate_a VALUES (1);
INSERT INTO truncate_a VALUES (2);
SELECT * FROM truncate_a ORDER BY 1;
-- Roll truncate back
START TRANSACTION;
TRUNCATE truncate_a;
ROLLBACK;
SELECT * FROM truncate_a ORDER BY 1;
-- Commit the truncate this time
START TRANSACTION;
TRUNCATE truncate_a;
COMMIT;
SELECT * FROM truncate_a ORDER BY 1;


-- Test ON TRUNCATE triggers

CREATE TABLE trunc_trigger_test (f1 int, f2 text, f3 text);
CREATE TABLE trunc_trigger_log (tgop text, tglevel text, tgwhen text,
        tgargv text, tgtable name, rowcount bigint);

CREATE FUNCTION trunctrigger() RETURNS trigger as $$
declare c bigint;
begin
    execute 'select count(*) from ' || quote_ident(tg_table_name) into c;
    insert into trunc_trigger_log values
      (TG_OP, TG_LEVEL, TG_WHEN, TG_ARGV[0], tg_table_name, c);
    return null;
end;
$$ LANGUAGE plpgsql;

-- basic before trigger
INSERT INTO trunc_trigger_test VALUES(1, 'foo', 'bar'), (2, 'baz', 'quux');

CREATE TRIGGER t
BEFORE TRUNCATE ON trunc_trigger_test
FOR EACH STATEMENT
EXECUTE PROCEDURE trunctrigger('before trigger truncate');

SELECT count(*) as "Row count in test table" FROM trunc_trigger_test;
SELECT * FROM trunc_trigger_log;
TRUNCATE trunc_trigger_test;
SELECT count(*) as "Row count in test table" FROM trunc_trigger_test;
SELECT * FROM trunc_trigger_log;

DROP TRIGGER t ON trunc_trigger_test;

truncate trunc_trigger_log;

-- same test with an after trigger
INSERT INTO trunc_trigger_test VALUES(1, 'foo', 'bar'), (2, 'baz', 'quux');

CREATE TRIGGER tt
AFTER TRUNCATE ON trunc_trigger_test
FOR EACH STATEMENT
EXECUTE PROCEDURE trunctrigger('after trigger truncate');

SELECT count(*) as "Row count in test table" FROM trunc_trigger_test;
SELECT * FROM trunc_trigger_log;
TRUNCATE trunc_trigger_test;
SELECT count(*) as "Row count in test table" FROM trunc_trigger_test;
SELECT * FROM trunc_trigger_log;

DROP TABLE trunc_trigger_test;
DROP TABLE trunc_trigger_log;

DROP FUNCTION trunctrigger();


DROP TABLE truncate_a;
