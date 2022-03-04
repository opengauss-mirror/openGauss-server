CREATE SCHEMA large_sequence;
SET CURRENT_SCHEMA = large_sequence;

-- test psql support
CREATE SEQUENCE S1;
CREATE LARGE SEQUENCE S2;

\d

\ds

\d S1

\d S2

COMMENT ON LARGE SEQUENCE S2 IS 'FOO';
COMMENT ON LARGE SEQUENCE S2 IS NULL;

-- temp sequences not support
CREATE TEMP LARGE SEQUENCE myseq2;
CREATE TEMP LARGE SEQUENCE myseq3;

-- default no cache, no start, no cycle
CREATE LARGE SEQUENCE S
INCREMENT 17014118346046923173168730371588410572
MINVALUE 17014118346046923173168730371588410573
MAXVALUE 170141183460469231731687303715884105721;

-- basic api
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');

SELECT * FROM setval('s', 17014118346046923173168730371588410573);

SELECT * FROM nextval('s');

SELECT * FROM lastval();

SELECT * FROM currval('s');

SELECT * FROM setval('s', 17014118346046923173168730371588410573, FALSE);

SELECT * FROM setval('s'::text, 17014118346046923173168730371588410573);

SELECT * FROM setval('s'::text, 17014118346046923173168730371588410573, FALSE);

SELECT * FROM nextval('s'::text);

SELECT * FROM currval('s'::text);

SELECT * FROM setval('s'::regclass, 17014118346046923173168730371588410573);

SELECT * FROM setval('s'::regclass, 17014118346046923173168730371588410573, FALSE);

SELECT * FROM nextval('s'::regclass);

SELECT * FROM currval('s'::regclass);

-- needs drop large sequence
DROP SEQUENCE S;
DROP LARGE SEQUENCE S;

-- cycle
CREATE LARGE SEQUENCE S
INCREMENT 17014118346046923173168730371588410572
MINVALUE 17014118346046923173168730371588410573
MAXVALUE 51042355038140769519506191114765231717
CYCLE;

SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');

DROP LARGE SEQUENCE S;

-- cache
CREATE LARGE SEQUENCE S
INCREMENT 17014118346046923173168730371588410572
MINVALUE 17014118346046923173168730371588410573
MAXVALUE 170141183460469231731687303715884105721
CACHE 5;

SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');
SELECT * FROM nextval('s');

DROP LARGE SEQUENCE S;

-- start with
CREATE LARGE SEQUENCE S
INCREMENT -17014118346046923173168730371588410572
MINVALUE 17014118346046923173168730371588410573
MAXVALUE 170141183460469231731687303715884105721
START WITH 170141183460469231731687303715884105720
CACHE 5;

DROP LARGE SEQUENCE S;

CREATE LARGE SEQUENCE S
INCREMENT 100000000000000000000000000000000000
MINVALUE 100000000000000000000000000000000000
MAXVALUE 100000000000000000000000000000000000000;

-- can create sequence with default nextval(<large sequence>)
CREATE TABLE TAB_SEQ(c1 numeric, c2 numeric default nextval('S'), c3 serial);
INSERT INTO TAB_SEQ VALUES(0);
INSERT INTO TAB_SEQ VALUES(0);
INSERT INTO TAB_SEQ VALUES(0);
INSERT INTO TAB_SEQ VALUES(0);
INSERT INTO TAB_SEQ VALUES(0);
INSERT INTO TAB_SEQ VALUES(0);

SELECT * FROM TAB_SEQ ORDER BY c3;

\d TAB_SEQ
-- cannot drop
DROP LARGE SEQUENCE S;

DROP LARGE SEQUENCE S CASCADE;

-- default value is dropped accordingly
\d TAB_SEQ

-- alter sequence
CREATE LARGE SEQUENCE foo;

-- rename not supported
ALTER LARGE SEQUENCE foo RENAME TO bar;

SELECT * FROM foo;

-- alter maxvalue - ok
ALTER LARGE SEQUENCE foo MAXVALUE 1000;

-- alter owner role -ok
CREATE ROLE role_foo PASSWORD '!@#123qwe';
ALTER LARGE SEQUENCE foo OWNER TO role_foo;

-- alter owner column - fail if owners are different
CREATE TABLE tab_foo (a bigint);
ALTER LARGE SEQUENCE foo OWNED BY tab_foo.a;
DROP LARGE SEQUENCE IF EXISTS foo;
CREATE LARGE SEQUENCE foo;
ALTER LARGE SEQUENCE IF EXISTS foo OWNED BY tab_foo.a;

-- owner column set OK
DROP TABLE tab_foo;

-- alter if exitsts works
ALTER LARGE SEQUENCE IF EXISTS foo MAXVALUE 100;
CREATE LARGE SEQUENCE foo INCREMENT 10 CYCLE;
ALTER LARGE SEQUENCE IF EXISTS foo MAXVALUE 30;

SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');

ALTER LARGE SEQUENCE IF EXISTS foo NO MAXVALUE;
SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');
SELECT * FROM nextval('foo');

ALTER LARGE SEQUENCE IF EXISTS foo NOMAXVALUE;

-- alter other attributes are not supported
ALTER LARGE SEQUENCE IF EXISTS foo MINVALUE 1;
ALTER LARGE SEQUENCE IF EXISTS foo NO CYCLE;
ALTER LARGE SEQUENCE IF EXISTS foo START 1;
ALTER LARGE SEQUENCE IF EXISTS foo CACHE 100;

-- test for largeserial
CREATE TABLE serialTest (f1 text, f2 largeserial);
INSERT INTO serialTest VALUES ('foo');
INSERT INTO serialTest VALUES ('bar');
INSERT INTO serialTest VALUES ('force', 17014118346046923173168730371588410573);
INSERT INTO serialTest VALUES ('wrong', NULL);
SELECT * FROM serialTest;

CREATE TABLE serialTest2 (f1 text, f2 serial, f3 smallserial, f4 serial2,
  f5 bigserial, f6 serial8, f7 largeserial, f8 serial16);
INSERT INTO serialTest2 (f1)
  VALUES ('test_defaults');
INSERT INTO serialTest2 (f1, f2, f3, f4, f5, f6, f7, f8)
  VALUES ('test_max_vals', 2147483647, 32767, 32767, 9223372036854775807,
          9223372036854775807, 170141183460469231731687303715884105727, 170141183460469231731687303715884105727),
         ('test_min_vals', -2147483648, -32768, -32768, -9223372036854775808,
          -9223372036854775808, -170141183460469231731687303715884105728, -170141183460469231731687303715884105728);

INSERT INTO serialTest2 (f1, f7)
  VALUES ('bogus', -170141183460469231731687303715884105729);

INSERT INTO serialTest2 (f1, f8)
  VALUES ('bogus', -170141183460469231731687303715884105729);

INSERT INTO serialTest2 (f1, f7)
  VALUES ('bogus', 170141183460469231731687303715884105728);

INSERT INTO serialTest2 (f1, f8)
  VALUES ('bogus', 170141183460469231731687303715884105728);

SELECT * FROM serialTest2 ORDER BY f2 ASC;

SELECT nextval('serialTest2_f2_seq');
SELECT nextval('serialTest2_f3_seq');
SELECT nextval('serialTest2_f4_seq');
SELECT nextval('serialTest2_f5_seq');
SELECT nextval('serialTest2_f6_seq');
SELECT nextval('serialTest2_f7_seq');
SELECT nextval('serialTest2_f8_seq');

-- Create table like
CREATE TABLE cat (like serialTest2);
INSERT INTO cat (f1)
  VALUES ('eins');
INSERT INTO cat (f1)
  VALUES ('zwei');
INSERT INTO cat (f1)
  VALUES ('drei');
INSERT INTO cat (f1)
  VALUES ('funf');
SELECT * FROM cat;

-- renaming serial sequences
ALTER TABLE serialtest_f7_seq RENAME TO serialtest_f7_foo;
INSERT INTO serialTest VALUES ('more');
SELECT * FROM serialTest;

-- Check dependencies of serial and ordinary sequences
CREATE LARGE SEQUENCE myseq2;
CREATE LARGE SEQUENCE myseq3;
-- Cannot have type cast in nextval's argument
CREATE TABLE t1 (
  f1 serial,
  f2 numeric DEFAULT nextval('myseq2'),
  f3 numeric DEFAULT nextval('myseq3'::text)
);

CREATE TABLE t1 (
  f1 largeserial,
  f2 numeric DEFAULT nextval('myseq2'),
  f3 numeric DEFAULT nextval('myseq3')
);

-- Both drops should fail
DROP LARGE SEQUENCE t1_f1_seq;
DROP LARGE SEQUENCE myseq2;
DROP TABLE t1;
-- Fails because no longer existent:
DROP SEQUENCE t1_f1_seq;
DROP LARGE SEQUENCE myseq2;

-- Information schema do not support large sequence for now
SELECT * FROM information_schema.sequences WHERE sequence_name in ('myseq3');

-- Privilige check
CREATE LARGE SEQUENCE priv_seq;
CREATE ROLE zeus PASSWORD '123qwe!@#';

GRANT SELECT ON priv_seq TO zeus;
GRANT ALL ON SCHEMA large_sequence TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
SELECT * FROM priv_seq;
\d priv_seq
SELECT nextval('priv_seq');
ALTER LARGE SEQUENCE priv_seq MAXVALUE 100;
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
GRANT UPDATE ON priv_seq TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
SELECT nextval('priv_seq');
ALTER LARGE SEQUENCE priv_seq MAXVALUE 100;
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
GRANT USAGE ON priv_seq TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
ALTER LARGE SEQUENCE priv_seq MAXVALUE 100;
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
GRANT ALTER ON priv_seq TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
ALTER LARGE SEQUENCE priv_seq MAXVALUE 100;
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
GRANT COMMENT ON priv_seq TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
GRANT DROP ON priv_seq TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
CREATE SCHEMA seq_priv_schema;
CREATE LARGE SEQUENCE seq_priv_schema.priv_seq;
GRANT ALL ON SCHEMA seq_priv_schema TO zeus;
GRANT ALL ON ALL SEQUENCES IN SCHEMA seq_priv_schema TO zeus;
SET ROLE zeus PASSWORD '123qwe!@#';
SET current_schema = seq_priv_schema;
SELECT * FROM priv_seq;
\d priv_seq
SELECT nextval('priv_seq');
ALTER LARGE SEQUENCE priv_seq MAXVALUE 100;
COMMENT ON LARGE SEQUENCE priv_seq IS 'FOO';
DROP LARGE SEQUENCE priv_seq;

RESET ROLE;
DROP SCHEMA seq_priv_schema CASCADE;
REVOKE ALL ON SCHEMA large_sequence FROM zeus;
DROP ROLE zeus;

SET current_schema = large_sequence;
create table ctest (a int) with (orientation=column);
create user ctest_user password 'huawei@123';
alter table ctest owner to "ctest_user";
drop user ctest_user cascade;

DROP ROLE role_foo;
DROP SCHEMA large_sequence CASCADE;