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

-- Promoting an existing unique index must validate data written with checks disabled.
CREATE TABLE warehouse_b_promote (k int NOT NULL);
CREATE UNIQUE INDEX warehouse_b_promote_idx ON warehouse_b_promote(k);
SET unique_checks = off;
INSERT INTO warehouse_b_promote VALUES (1), (1);
RESET unique_checks;
ALTER TABLE warehouse_b_promote ADD CONSTRAINT warehouse_b_promote_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_idx;
-- Failed promotion must preserve the original name, data and index flags.
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_idx'::regclass;
SELECT k, count(*) FROM warehouse_b_promote GROUP BY k ORDER BY k;
SELECT count(*) FROM pg_constraint
    WHERE conrelid = 'warehouse_b_promote'::regclass AND contype = 'p';
-- Validation must also run while unique_checks is still disabled.
SET unique_checks = off;
ALTER TABLE warehouse_b_promote ADD CONSTRAINT warehouse_b_promote_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_idx;
DELETE FROM warehouse_b_promote WHERE k = 1;
INSERT INTO warehouse_b_promote VALUES (1), (2);
-- A successful rebuild and promotion must remain transactional.
BEGIN;
ALTER TABLE warehouse_b_promote ADD CONSTRAINT warehouse_b_promote_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_idx;
ROLLBACK;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_idx'::regclass;
SELECT k, count(*) FROM warehouse_b_promote GROUP BY k ORDER BY k;
ALTER TABLE warehouse_b_promote ADD CONSTRAINT warehouse_b_promote_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_idx;
INSERT INTO warehouse_b_promote VALUES (1);
RESET unique_checks;

-- A successful rebuild must still roll back if the key contains NULL.
CREATE TABLE warehouse_b_promote_null (k int);
CREATE UNIQUE INDEX warehouse_b_promote_null_idx ON warehouse_b_promote_null(k);
INSERT INTO warehouse_b_promote_null VALUES (NULL), (1);
ALTER TABLE warehouse_b_promote_null ADD CONSTRAINT warehouse_b_promote_null_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_null_idx;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_null_idx'::regclass;
SELECT k FROM warehouse_b_promote_null ORDER BY k NULLS FIRST;

-- Ustore indexes must be checked before promotion as well.
CREATE TABLE warehouse_b_promote_ustore (k int NOT NULL) WITH (storage_type = ustore);
CREATE UNIQUE INDEX warehouse_b_promote_ustore_idx ON warehouse_b_promote_ustore(k);
SET unique_checks = off;
INSERT INTO warehouse_b_promote_ustore VALUES (1), (1);
RESET unique_checks;
ALTER TABLE warehouse_b_promote_ustore ADD CONSTRAINT warehouse_b_promote_ustore_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_ustore_idx;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_ustore_idx'::regclass;
SELECT k, count(*) FROM warehouse_b_promote_ustore GROUP BY k ORDER BY k;
DELETE FROM warehouse_b_promote_ustore WHERE k = 1;
INSERT INTO warehouse_b_promote_ustore VALUES (1), (2);
SET unique_checks = off;
ALTER TABLE warehouse_b_promote_ustore ADD CONSTRAINT warehouse_b_promote_ustore_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_ustore_idx;
INSERT INTO warehouse_b_promote_ustore VALUES (1);
RESET unique_checks;

-- A global index must validate duplicates across partitions before promotion.
CREATE TABLE warehouse_b_promote_gpi (k int NOT NULL, p int)
PARTITION BY RANGE (p) (
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (MAXVALUE)
);
CREATE UNIQUE INDEX warehouse_b_promote_gpi_idx ON warehouse_b_promote_gpi(k) GLOBAL;
SET unique_checks = off;
INSERT INTO warehouse_b_promote_gpi VALUES (1, 1), (1, 11);
RESET unique_checks;
ALTER TABLE warehouse_b_promote_gpi ADD CONSTRAINT warehouse_b_promote_gpi_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_gpi_idx;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_gpi_idx'::regclass;
SELECT k, count(*) FROM warehouse_b_promote_gpi GROUP BY k ORDER BY k;
DELETE FROM warehouse_b_promote_gpi WHERE k = 1;
INSERT INTO warehouse_b_promote_gpi VALUES (1, 1), (2, 11);
SET unique_checks = off;
ALTER TABLE warehouse_b_promote_gpi ADD CONSTRAINT warehouse_b_promote_gpi_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_gpi_idx;
INSERT INTO warehouse_b_promote_gpi VALUES (1, 11);
RESET unique_checks;

-- A GTT without session-local storage must allow promotion after reconnecting.
CREATE GLOBAL TEMPORARY TABLE warehouse_b_promote_gtt_empty (k int NOT NULL)
    ON COMMIT PRESERVE ROWS;
CREATE UNIQUE INDEX warehouse_b_promote_gtt_empty_idx
    ON warehouse_b_promote_gtt_empty(k);
\c unique_check_b
SET current_schema TO 'unique_check';
-- Keep this GTT unattached until the promotion.
ALTER TABLE warehouse_b_promote_gtt_empty
    ADD CONSTRAINT warehouse_b_promote_gtt_empty_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_gtt_empty_idx;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_gtt_empty_pkey'::regclass;
SET unique_checks = off;
INSERT INTO warehouse_b_promote_gtt_empty VALUES (1);
INSERT INTO warehouse_b_promote_gtt_empty VALUES (1);
SELECT k, count(*) FROM warehouse_b_promote_gtt_empty GROUP BY k ORDER BY k;
RESET unique_checks;
DROP TABLE warehouse_b_promote_gtt_empty;

-- An attached GTT must still reject dirty keys even with unique_checks disabled.
CREATE GLOBAL TEMPORARY TABLE warehouse_b_promote_gtt_attached (k int NOT NULL)
    ON COMMIT PRESERVE ROWS;
CREATE UNIQUE INDEX warehouse_b_promote_gtt_attached_idx
    ON warehouse_b_promote_gtt_attached(k);
SET unique_checks = off;
INSERT INTO warehouse_b_promote_gtt_attached VALUES (1), (1);
ALTER TABLE warehouse_b_promote_gtt_attached
    ADD CONSTRAINT warehouse_b_promote_gtt_attached_pkey
    PRIMARY KEY USING INDEX warehouse_b_promote_gtt_attached_idx;
SELECT indisprimary, indisvalid FROM pg_index
    WHERE indexrelid = 'warehouse_b_promote_gtt_attached_idx'::regclass;
SELECT k, count(*) FROM warehouse_b_promote_gtt_attached GROUP BY k ORDER BY k;
SELECT count(*) FROM pg_constraint
    WHERE conrelid = 'warehouse_b_promote_gtt_attached'::regclass AND contype = 'p';
RESET unique_checks;
DROP TABLE warehouse_b_promote_gtt_attached;

RESET unique_checks;
DROP SCHEMA unique_check CASCADE;
RESET current_schema;

\c regression
DROP DATABASE unique_check_b;
DROP USER unique_check_user;
