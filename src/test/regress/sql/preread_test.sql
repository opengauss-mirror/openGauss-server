-- test database create

CREATE TABLE test (id int, field1 char(400),field2 char(400));

DO $$
BEGIN
    FOR i IN 0..5000 LOOP
        INSERT INTO test (id, field1, field2) VALUES 
        (
            i, 
            i::text || LPAD('1', 200 - length(i::text), '1'), 
            i::text || LPAD('2', 200 - length(i::text), '2')
        );
    END LOOP;
END;
$$;


-- heap seqscan test;
SET heap_bulk_read_size TO '64kB';

SELECT COUNT(*) FROM test;

SELECT COUNT(*) FROM test WHERE test.id >2400;

SELECT test.id, test.field1, test.field2 FROM test
        WHERE test.id <20 ORDER BY field1 using <;


select * from test, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE test.id = v.i;

select * from test,
  (values ((select i from
    (values(''), ('pijinngjijifezxcvzxv'),((select 'jafipejifjeaio'))) as foo(i)
    order by i asc limit 1))) bar (i)
  where test.field1 < bar.i LIMIT 5;

SELECT t.id, t.field1, t.field2 FROM test* t ORDER BY id using >, field1 LIMIT 10;

SELECT t.id, t.field1, t.field2 FROM test* t LIMIT 10;

SELECT * FROM test ORDER BY id LIMIT 10;

SELECT * FROM test ORDER BY id DESC LIMIT 10;

SELECT * FROM test ORDER BY id NULLS FIRST LIMIT 10;

DROP TABLE test;
SET heap_bulk_read_size TO '0kB';


--LAZY_VACUUM test

CREATE TABLE vactst (i INT);
INSERT INTO vactst VALUES (1);
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);

ALTER SYSTEM SET vacuum_bulk_read_size='64kB';

SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
SELECT * FROM vactst;
VACUUM vactst;

UPDATE vactst SET i = i + 1;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst SELECT * FROM vactst;
INSERT INTO vactst VALUES (0);
SELECT count(*) FROM vactst;
DELETE FROM vactst WHERE i != 0;
VACUUM vactst;

DELETE FROM vactst;
SELECT * FROM vactst;

DROP TABLE vactst;

ALTER SYSTEM SET vacuum_bulk_read_size='0kB';
