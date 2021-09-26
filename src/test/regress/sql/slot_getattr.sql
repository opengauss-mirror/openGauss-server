set enable_opfusion = on;


\i slot_getsomeattrs.sql;

SELECT DISTINCT typtype, typinput
FROM pg_type AS p1
WHERE p1.typtype not in ('b', 'p')
ORDER BY 1;


--test COMMENT ON view's column
create table slot_getattr_normal_view_column_t(id1 int,id2 int);
create or replace view slot_getattr_normal_view_column_v as select * from slot_getattr_normal_view_column_t;
create temp table slot_getattr_comment_view_column_t(id1 int,id2 int);
create or replace temp view slot_getattr_comment_view_column_v as select * from slot_getattr_comment_view_column_t;
comment on column slot_getattr_normal_view_column_t.id1 is 'this is normal table';
comment on column slot_getattr_normal_view_column_v.id1 is 'this is normal view';
comment on column slot_getattr_comment_view_column_t.id1 is 'this is temp table';
comment on column slot_getattr_comment_view_column_v.id1 is 'this is temp view';
\d+ slot_getattr_normal_view_column_t
\d+ slot_getattr_normal_view_column_v
drop view slot_getattr_normal_view_column_v cascade;
drop table slot_getattr_normal_view_column_t cascade;
drop view slot_getattr_comment_view_column_v cascade;
drop table slot_getattr_comment_view_column_t cascade;

CREATE TABLE slot_getattr_s (rf_a SERIAL PRIMARY KEY,
	b INT);

CREATE TABLE slot_getattr (a SERIAL PRIMARY KEY,
	b INT,
	c TEXT,
	d TEXT
	);

CREATE INDEX slot_getattr_b ON slot_getattr (b);
CREATE INDEX slot_getattr_c ON slot_getattr (c);
CREATE INDEX slot_getattr_c_b ON slot_getattr (c,b);
CREATE INDEX slot_getattr_b_c ON slot_getattr (b,c);

INSERT INTO slot_getattr_s (b) VALUES (0);
INSERT INTO slot_getattr_s (b) SELECT b FROM slot_getattr_s;
INSERT INTO slot_getattr_s (b) SELECT b FROM slot_getattr_s;
INSERT INTO slot_getattr_s (b) SELECT b FROM slot_getattr_s;
INSERT INTO slot_getattr_s (b) SELECT b FROM slot_getattr_s;
INSERT INTO slot_getattr_s (b) SELECT b FROM slot_getattr_s;
drop table slot_getattr_s cascade;

-- CREATE TABLE clstr_tst_inh () INHERITS (slot_getattr);

INSERT INTO slot_getattr (b, c) VALUES (11, 'once');
INSERT INTO slot_getattr (b, c) VALUES (10, 'diez');
INSERT INTO slot_getattr (b, c) VALUES (31, 'treinta y uno');
INSERT INTO slot_getattr (b, c) VALUES (22, 'veintidos');
INSERT INTO slot_getattr (b, c) VALUES (3, 'tres');
INSERT INTO slot_getattr (b, c) VALUES (20, 'veinte');
INSERT INTO slot_getattr (b, c) VALUES (23, 'veintitres');
INSERT INTO slot_getattr (b, c) VALUES (21, 'veintiuno');
INSERT INTO slot_getattr (b, c) VALUES (4, 'cuatro');
INSERT INTO slot_getattr (b, c) VALUES (14, 'catorce');
INSERT INTO slot_getattr (b, c) VALUES (2, 'dos');
INSERT INTO slot_getattr (b, c) VALUES (18, 'dieciocho');
INSERT INTO slot_getattr (b, c) VALUES (27, 'veintisiete');
INSERT INTO slot_getattr (b, c) VALUES (25, 'veinticinco');
INSERT INTO slot_getattr (b, c) VALUES (13, 'trece');
INSERT INTO slot_getattr (b, c) VALUES (28, 'veintiocho');
INSERT INTO slot_getattr (b, c) VALUES (32, 'treinta y dos');
INSERT INTO slot_getattr (b, c) VALUES (5, 'cinco');
INSERT INTO slot_getattr (b, c) VALUES (29, 'veintinueve');
INSERT INTO slot_getattr (b, c) VALUES (1, 'uno');
INSERT INTO slot_getattr (b, c) VALUES (24, 'veinticuatro');
INSERT INTO slot_getattr (b, c) VALUES (30, 'treinta');
INSERT INTO slot_getattr (b, c) VALUES (12, 'doce');
INSERT INTO slot_getattr (b, c) VALUES (17, 'diecisiete');
INSERT INTO slot_getattr (b, c) VALUES (9, 'nueve');
INSERT INTO slot_getattr (b, c) VALUES (19, 'diecinueve');
INSERT INTO slot_getattr (b, c) VALUES (26, 'veintiseis');
INSERT INTO slot_getattr (b, c) VALUES (15, 'quince');
INSERT INTO slot_getattr (b, c) VALUES (7, 'siete');
INSERT INTO slot_getattr (b, c) VALUES (16, 'dieciseis');
INSERT INTO slot_getattr (b, c) VALUES (8, 'ocho');
-- This entry is needed to test that TOASTED values are copied correctly.
INSERT INTO slot_getattr (b, c, d) VALUES (6, 'seis', repeat('xyzzy', 100000));

CLUSTER slot_getattr_c ON slot_getattr;


-- Verify that foreign key link still works
INSERT INTO slot_getattr (b, c) VALUES (1111, 'this should fail');



-- Try changing indisclustered
ALTER TABLE slot_getattr CLUSTER ON slot_getattr_b_c;

-- Try turning off all clustering
ALTER TABLE slot_getattr SET WITHOUT CLUSTER;
drop table slot_getattr cascade;

-- Verify that clustering all tables does in fact cluster the right ones
CREATE USER clstr_user PASSWORD 'gauss@123';
CREATE TABLE slot_getattr_1 (a INT PRIMARY KEY);
CREATE TABLE slot_getattr_2 (a INT PRIMARY KEY);
CREATE TABLE slot_getattr_3 (a INT PRIMARY KEY);
ALTER TABLE slot_getattr_1 OWNER TO clstr_user;
ALTER TABLE slot_getattr_3 OWNER TO clstr_user;
GRANT SELECT ON slot_getattr_2 TO clstr_user;
INSERT INTO slot_getattr_1 VALUES (2);
INSERT INTO slot_getattr_1 VALUES (1);
INSERT INTO slot_getattr_2 VALUES (2);
INSERT INTO slot_getattr_2 VALUES (1);
INSERT INTO slot_getattr_3 VALUES (2);
INSERT INTO slot_getattr_3 VALUES (1);

-- "CLUSTER <tablename>" on a table that hasn't been clustered

CLUSTER slot_getattr_1_pkey ON slot_getattr_1;
CLUSTER slot_getattr_2 USING slot_getattr_2_pkey;
SELECT * FROM slot_getattr_1 UNION ALL
  SELECT * FROM slot_getattr_2 UNION ALL
  SELECT * FROM slot_getattr_3
  ORDER BY 1;

drop table slot_getattr_1;
drop table slot_getattr_2;
drop table slot_getattr_3;
drop user clstr_user cascade;



-- Test DROP OWNED
CREATE USER regression_user0 PASSWORD 'gauss@123';
CREATE USER regression_user1 PASSWORD 'gauss@123';
CREATE USER regression_user2 PASSWORD 'gauss@123';
SET SESSION AUTHORIZATION regression_user0 PASSWORD 'gauss@123';
-- permission denied
DROP OWNED BY regression_user1 CASCADE;
DROP OWNED BY regression_user0, regression_user2 CASCADE;
REASSIGN OWNED BY regression_user0 TO regression_user1;
REASSIGN OWNED BY regression_user1 TO regression_user0;
CREATE TABLE deptest1 (f1 int unique);
GRANT USAGE ON schema regression_user0 TO regression_user1;
GRANT ALL ON deptest1 TO regression_user1 WITH GRANT OPTION;

SET SESSION AUTHORIZATION regression_user1 PASSWORD 'gauss@123';
CREATE TABLE deptest (a int primary key, b text);
GRANT ALL ON regression_user0.deptest1 TO regression_user2;
RESET SESSION AUTHORIZATION;
\z regression_user0.deptest1

DROP USER regression_user0 CASCADE;
DROP USER regression_user1 CASCADE;
DROP USER regression_user2 CASCADE;



CREATE TABLE slot_getattr_4 (
    col1 INT,
    col2 INT,
    col3 INT DEFAULT 1,
--    col4 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    col5 BIGSERIAL
)  ;

--- distribute key are not allowed to update
INSERT INTO slot_getattr_4 VALUES (1, 2) ON DUPLICATE KEY UPDATE col1 = 3;

--- should always insert
INSERT INTO slot_getattr_4 VALUES (1, 2) ON DUPLICATE KEY UPDATE col2 = 3;
INSERT INTO slot_getattr_4 VALUES (1, 2) ON DUPLICATE KEY UPDATE slot_getattr_4.col2 = 4;

--- appoint column list in insert clause, should always insert
INSERT INTO slot_getattr_4(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE col2 = 5;
INSERT INTO slot_getattr_4(col1, col3) VALUES (1, 3) ON DUPLICATE KEY UPDATE slot_getattr_4.col2 = 6;

--- multiple rows, should always insert
INSERT INTO slot_getattr_4 VALUES (2, 1), (2, 1) ON DUPLICATE KEY UPDATE col2 = 7, col3 = 7;

(SELECT col1, col1 + col2 FROM slot_getattr_4 WHERE col2 IS NOT NULL
    UNION
    SELECT 1, 2)
    INTERSECT
    SELECT col1, col3 FROM slot_getattr_4;
drop table slot_getattr_4 cascade;



create table slot_getattr_5(a int, b int);
insert into slot_getattr_5 select generate_series(1,5), generate_series(1,5);
analyze slot_getattr_5;
with x as
(select a, b from slot_getattr_5)
select 1 from slot_getattr_5 s 
where s.a not in 
(select a from x)
or exists
(select 1 from x where x.a=s.a)
or s.a >     
(select avg(a) from x where x.b=s.b);
drop table slot_getattr_5 cascade;

set enable_opfusion = off;