CREATE USER regress_view_user1 PASSWORD 'Gauss@123';
CREATE USER regress_view_user2 PASSWORD 'Gauss@123';

-- nested-view permissions check
CREATE TABLE base_tbl(a int, b text, c float);
INSERT INTO base_tbl VALUES (1, 'Row 1', 1.0);

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
CREATE VIEW rw_view1 AS SELECT * FROM base_tbl;
GRANT ALL ON SCHEMA regress_view_user1 TO regress_view_user2;
SELECT * FROM rw_view1;  -- not allowed
SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
CREATE VIEW rw_view2 AS SELECT * FROM regress_view_user1.rw_view1;
SELECT * FROM rw_view2;  -- not allowed
SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

RESET SESSION AUTHORIZATION;
GRANT SELECT ON base_tbl TO regress_view_user1;

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
SELECT * FROM rw_view1;
SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- unlike pgsql, we do not support updating views

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
SELECT * FROM rw_view2;  -- not allowed
SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- not allowed

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
GRANT SELECT ON rw_view1 TO regress_view_user2;

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
SELECT * FROM rw_view2;
SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- unlike pgsql, we do not support updating views

RESET SESSION AUTHORIZATION;
GRANT UPDATE ON base_tbl TO regress_view_user1;

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
SELECT * FROM rw_view1;
SELECT * FROM rw_view1 FOR UPDATE;
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- unlike pgsql, we do not support updating views

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
SELECT * FROM rw_view2;
SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- unlike pgsql, we do not support updating views

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
GRANT UPDATE ON rw_view1 TO regress_view_user2;

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
SELECT * FROM rw_view2;
SELECT * FROM rw_view2 FOR UPDATE;
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- unlike pgsql, we do not support updating views

RESET SESSION AUTHORIZATION;
REVOKE UPDATE ON base_tbl FROM regress_view_user1;

SET SESSION AUTHORIZATION regress_view_user1 PASSWORD 'Gauss@123';
SELECT * FROM rw_view1;
SELECT * FROM rw_view1 FOR UPDATE;  -- not allowed
UPDATE rw_view1 SET b = 'foo' WHERE a = 1;  -- unlike pgsql, we do not support updating views

SET SESSION AUTHORIZATION regress_view_user2 PASSWORD 'Gauss@123';
SELECT * FROM rw_view2;
SELECT * FROM rw_view2 FOR UPDATE;  -- not allowed
UPDATE rw_view2 SET b = 'bar' WHERE a = 1;  -- unlike pgsql, we do not support updating views

RESET SESSION AUTHORIZATION;

DROP TABLE base_tbl CASCADE;

DROP USER regress_view_user1;
DROP USER regress_view_user2;