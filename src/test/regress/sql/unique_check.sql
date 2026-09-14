DROP DATABASE IF EXISTS unique_check_b;
DROP USER IF EXISTS unique_check_user;

-- A-format databases always enforce primary-key and non-primary-key unique indexes.
CREATE SCHEMA unique_check;
SET current_schema TO 'unique_check';

CREATE TABLE warehouse_a (
    w_id int PRIMARY KEY,
    w_code int UNIQUE
);
INSERT INTO warehouse_a VALUES (1, 10);
SET unique_checks = off;
INSERT INTO warehouse_a VALUES (1, 20);
INSERT INTO warehouse_a VALUES (2, 10);
RESET unique_checks;

DROP SCHEMA unique_check CASCADE;
RESET current_schema;

-- B-format databases only allow unique_checks to control non-primary-key unique indexes.
CREATE DATABASE unique_check_b DBCOMPATIBILITY 'B';
\c unique_check_b

CREATE SCHEMA unique_check;
SET current_schema TO 'unique_check';

CREATE TABLE warehouse_b (
    w_id int PRIMARY KEY,
    w_code int UNIQUE
);
INSERT INTO warehouse_b VALUES (1, 10), (2, 20);

CREATE TABLE warehouse_b_col (
    w_id int PRIMARY KEY,
    w_code int UNIQUE
) WITH (orientation = column, deltarow_threshold = 0);
INSERT INTO warehouse_b_col VALUES (1, 10), (2, 20);

CREATE TABLE warehouse_b_idx (w_id int, w_code int);
CREATE UNIQUE INDEX warehouse_b_idx_code_idx ON warehouse_b_idx(w_code);
INSERT INTO warehouse_b_idx VALUES (1, 10);

CREATE TABLE warehouse_b_deferred (
    w_id int,
    w_code int UNIQUE DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO warehouse_b_deferred VALUES (1, 10);

-- With unique_checks enabled, both primary-key and non-primary-key unique indexes are enforced.
INSERT INTO warehouse_b VALUES (1, 30);
INSERT INTO warehouse_b VALUES (3, 10);
INSERT INTO warehouse_b_col VALUES (1, 30);
INSERT INTO warehouse_b_col VALUES (3, 10);
INSERT INTO warehouse_b_idx VALUES (2, 10);

SET unique_checks = off;

-- Primary keys are still enforced, while non-primary-key unique indexes can be bypassed.
INSERT INTO warehouse_b VALUES (1, 40);
INSERT INTO warehouse_b VALUES (3, 10);
UPDATE warehouse_b SET w_code = 10 WHERE w_id = 2;
UPDATE warehouse_b SET w_id = 1 WHERE w_id = 2;
INSERT INTO warehouse_b_idx VALUES (2, 10);

-- Column-store inserts and updates follow the same rules.
INSERT INTO warehouse_b_col VALUES (1, 40);
INSERT INTO warehouse_b_col VALUES (3, 10);
UPDATE warehouse_b_col SET w_code = 10 WHERE w_id = 2;
UPDATE warehouse_b_col SET w_id = 1 WHERE w_id = 2;

-- COPY follows the same rules as ordinary DML.
COPY warehouse_b FROM STDIN;
4	10
\.
COPY warehouse_b FROM STDIN;
1	50
\.

-- INSERT ... SELECT fusion uses the same uniqueness-check decision.
CREATE TABLE warehouse_source (w_id int, w_code int);
INSERT INTO warehouse_source VALUES (5, 10);
SET enable_opfusion = on;
INSERT INTO warehouse_b SELECT * FROM warehouse_source;
TRUNCATE warehouse_source;
INSERT INTO warehouse_source VALUES (1, 60);
INSERT INTO warehouse_b SELECT * FROM warehouse_source;
RESET enable_opfusion;

-- A non-owner cannot bypass a non-primary-key unique index.
CREATE USER unique_check_user PASSWORD 'Unique@123';
GRANT USAGE ON SCHEMA unique_check TO unique_check_user;
GRANT INSERT ON warehouse_b TO unique_check_user;
GRANT INSERT ON warehouse_b_col TO unique_check_user;
GRANT INSERT ON warehouse_b_idx TO unique_check_user;
GRANT INSERT ON warehouse_b_deferred TO unique_check_user;
SET SESSION AUTHORIZATION unique_check_user PASSWORD 'Unique@123';
SET unique_checks = off;
INSERT INTO unique_check.warehouse_b VALUES (6, 10);
INSERT INTO unique_check.warehouse_b_col VALUES (4, 10);
INSERT INTO unique_check.warehouse_b_idx VALUES (3, 10);

-- A non-owner keeps the original deferred-check behavior when unique_checks is off.
BEGIN;
INSERT INTO unique_check.warehouse_b_deferred VALUES (2, 10);
ROLLBACK;
RESET SESSION AUTHORIZATION;

SELECT * FROM warehouse_b ORDER BY w_id, w_code;
SELECT * FROM warehouse_b_col ORDER BY w_id, w_code;
SELECT * FROM warehouse_b_idx ORDER BY w_id, w_code;

RESET unique_checks;
DROP SCHEMA unique_check CASCADE;
RESET current_schema;

\c regression
DROP DATABASE unique_check_b;
DROP USER unique_check_user;
