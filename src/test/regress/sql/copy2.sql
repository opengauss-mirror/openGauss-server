-- Enforce use of COMMIT instead of 2PC for temporary objects

CREATE TABLE x (
	a int,
	b int,
	c text not null default 'stuff',
	d text,
	e text
);

CREATE FUNCTION fn_x_before () RETURNS TRIGGER AS '
  BEGIN
		NEW.e := ''before trigger fired''::text;
		return NEW;
	END;
' LANGUAGE plpgsql;

CREATE FUNCTION fn_x_after () RETURNS TRIGGER AS '
  BEGIN
		UPDATE x set e=''after trigger fired'' where c=''stuff'';
		return NULL;
	END;
' LANGUAGE plpgsql;

CREATE TRIGGER trg_x_after AFTER INSERT ON x
FOR EACH ROW EXECUTE PROCEDURE fn_x_after();

CREATE TRIGGER trg_x_before BEFORE INSERT ON x
FOR EACH ROW EXECUTE PROCEDURE fn_x_before();

COPY x (a, b, c, d, e) from stdin;
9999	\N	\\N	\NN	\N
10000	21	31	41	51
\.

COPY x (b, d) from stdin;
1	test_1
\.

COPY x (b, d) from stdin;
2	test_2
3	test_3
4	test_4
5	test_5
\.

COPY x (a, b, c, d, e) from stdin;
10001	22	32	42	52
10002	23	33	43	53
10003	24	34	44	54
10004	25	35	45	55
10005	26	36	46	56
\.

-- non-existent column in column list: should fail
COPY x (xyz) from stdin;

-- too many columns in column list: should fail
COPY x (a, b, c, d, e, d, c) from stdin;

-- missing data: should fail
COPY x from stdin;

\.
COPY x from stdin;
2000	230	23	23
\.
COPY x from stdin;
2001	231	\N	\N
\.

-- extra data: should fail
COPY x from stdin;
2002	232	40	50	60	70	80
\.

-- various COPY options: delimiters, oids, NULL string, encoding
COPY x (b, c, d, e) from stdin with oids delimiter ',' null 'x';
500000,x,45,80,90
500001,x,\x,\\x,\\\x
500002,x,\,,\\\,,\\
\.

COPY x from stdin WITH DELIMITER AS ';' NULL AS '';
3000;;c;;
\.

COPY x from stdin WITH DELIMITER AS ':' NULL AS E'\\X' ENCODING 'sql_ascii';
4000:\X:C:\X:\X
4001:1:empty::
4002:2:null:\X:\X
4003:3:Backslash:\\:\\
4004:4:BackslashX:\\X:\\X
4005:5:N:\N:\N
4006:6:BackslashN:\\N:\\N
4007:7:XX:\XX:\XX
4008:8:Delimiter:\::\:
\.

-- check results of copy in
SELECT * FROM x ORDER BY a, b;

-- COPY w/ oids on a table w/o oids should fail
CREATE TABLE no_oids (
	a	int,
	b	int
) WITHOUT OIDS;

INSERT INTO no_oids (a, b) VALUES (5, 10);
INSERT INTO no_oids (a, b) VALUES (20, 30);

-- should fail
COPY no_oids FROM stdin WITH OIDS;
COPY no_oids TO stdout WITH OIDS;

-- check copy out
COPY (select * from x order by 1,2,3,4,5) TO stdout;
COPY (select c,e from x where c != 'N' order by 1,2) TO stdout;
COPY (select b,e from x order by 1,2) TO stdout WITH NULL 'I''m null';

CREATE TEMP TABLE y (
	col1 text,
	col2 text
);

INSERT INTO y VALUES ('Jackson, Sam', E'\\h');
INSERT INTO y VALUES ('It is "perfect".',E'\t');
INSERT INTO y VALUES ('', NULL);

COPY y TO stdout WITH CSV;
COPY y TO stdout WITH CSV QUOTE '''' DELIMITER '|';
COPY y TO stdout WITH CSV FORCE QUOTE col2 ESCAPE E'\\' ENCODING 'sql_ascii';
COPY y TO stdout WITH CSV FORCE QUOTE *;

-- Repeat above tests with new 9.0 option syntax

COPY y TO stdout (FORMAT CSV);
COPY y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\');
COPY y TO stdout (FORMAT CSV, FORCE_QUOTE *);

\copy y TO stdout (FORMAT CSV)
\copy y TO stdout (FORMAT CSV, QUOTE '''', DELIMITER '|')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE (col2), ESCAPE E'\\')
\copy y TO stdout (FORMAT CSV, FORCE_QUOTE *)

--test that we read consecutive LFs properly

CREATE TEMP TABLE testnl (a int, b text, c int);

COPY testnl FROM stdin CSV;
1,"a field with two LFs

inside",2
\.

-- test end of copy marker
CREATE TABLE testeoc (a text);

COPY testeoc FROM stdin CSV;
a\.
\.b
c\.d
"\."
\.

COPY (select * from testeoc order by a using ~<~) TO stdout CSV;

-- test handling of nonstandard null marker that violates escaping rules

CREATE TEMP TABLE testnull(a int, b text);
INSERT INTO testnull VALUES (1, E'\\0'), (NULL, NULL);

COPY (select * from testnull order by 1,2) TO stdout WITH NULL AS E'\\0';

COPY testnull FROM stdin WITH NULL AS E'\\0';
42	\\0
\0	\0
\.

SELECT * FROM testnull ORDER BY 1,2;

START TRANSACTION;
CREATE TABLE vistest (LIKE testeoc);
COPY vistest FROM stdin CSV;
a0
b
\.
COMMIT;
SELECT * FROM vistest order by 1;

START TRANSACTION;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
x
y
\.
SELECT * FROM vistest order by 1;
COMMIT;
TRUNCATE vistest;
COPY vistest FROM stdin CSV FREEZE;
p
g
\.

CREATE FUNCTION truncate_in_subxact() RETURNS VOID AS
$$
BEGIN
	TRUNCATE vistest;
--EXCEPTION
--  WHEN OTHERS THEN
--	INSERT INTO vistest VALUES ('subxact failure');
END;
$$ language plpgsql;
START TRANSACTION;
INSERT INTO vistest VALUES ('z');
SELECT truncate_in_subxact();
COPY vistest FROM stdin CSV FREEZE;
d4
e
\.
SELECT * FROM vistest order by 1;
COMMIT;
SELECT * FROM vistest order by 1;
DROP TABLE vistest;
DROP FUNCTION truncate_in_subxact();
DROP TABLE x;
DROP TABLE y;
DROP FUNCTION fn_x_before();
DROP FUNCTION fn_x_after();


CREATE TABLE measurement (
city_id int not null,
logdate int not null,
peaktemp int,
unitsales int
);

CREATE TABLE measurement_movement (
city_id int not null,
logdate int not null,
peaktemp int,
unitsales int
);

CREATE OR REPLACE FUNCTION measurement_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
INSERT INTO measurement_movement VALUES (NEW.*);
RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_measurement_trigger
BEFORE INSERT ON measurement
FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();

COPY measurement from stdin;
10001	22	32	42
10002	23	33	43
10003	24	34	44
10004	25	35	45
10005	26	36	46
\.

select * from measurement_movement order by city_id;
select * from measurement order by city_id;

drop trigger insert_measurement_trigger on measurement;
drop function measurement_insert_trigger;
drop table measurement;
drop table measurement_movement;
