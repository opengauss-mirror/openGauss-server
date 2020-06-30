--
-- XC_CONSTRAINTS
--
-- Checks for constraint shippability in Postgres-XC for tables with different
-- distribution strategies

-- Create some tables for all the tests
CREATE TABLE xc_cons_rep (c1 int, c2 text, c3 text);
CREATE TABLE xc_cons_rr (c1 int, c2 text, c3 text);
CREATE TABLE xc_cons_hash (c1 text, c2 text, c3 text);

-- UNIQUE/PRIMARY KEY constraint
-- Replicated table
CREATE UNIQUE INDEX xc_cons_rep_unique1 ON xc_cons_rep(c1); -- OK
CREATE UNIQUE INDEX xc_cons_rep_unique2 ON xc_cons_rep(c1,c2); -- OK
CREATE UNIQUE INDEX xc_cons_rep_unique3 ON xc_cons_rep((c2 || c3)); -- OK
-- Roundrobin table
CREATE UNIQUE INDEX xc_cons_rr_unique1 ON xc_cons_rr(c1); -- error, not shippable
CREATE UNIQUE INDEX xc_cons_rr_unique2 ON xc_cons_rr(c1,c2); -- error, not shippable
CREATE UNIQUE INDEX xc_cons_rr_unique3 ON xc_cons_rr((c2 || c3)); -- error, not shippable
-- Distributed table
-- OK, is distribution column
CREATE UNIQUE INDEX xc_cons_hash_unique1 ON xc_cons_hash(c1);
-- OK, contains distribution column
CREATE UNIQUE INDEX xc_cons_hash_unique2 ON xc_cons_hash(c1,c2);
-- error, expression contains only distribution column
CREATE UNIQUE INDEX xc_cons_hash_unique3 ON xc_cons_hash((c1 || c1));
-- error, expression contains other columns than distribution one
CREATE UNIQUE INDEX xc_cons_hash_unique3 ON xc_cons_hash((c1 || c2));
-- Clean up
DROP TABLE xc_cons_rep,xc_cons_rr,xc_cons_hash;
-- EXCLUDE
CREATE TABLE xc_cons_hash (a int, c circle, EXCLUDE USING gist (c WITH &&)); -- error
CREATE TABLE xc_cons_rr (c circle, EXCLUDE USING gist (c WITH &&)); -- error
CREATE TABLE xc_cons_rep (c circle, EXCLUDE USING gist (c WITH &&)); -- OK
DROP TABLE xc_cons_rep;

-- FOREIGN KEY creation
CREATE SCHEMA xc_constraints_tests;
SET search_path = xc_constraints_tests;
-- Some parent tables
CREATE TABLE xc_parent_rep (a int PRIMARY KEY); -- OK
CREATE TABLE xc_parent_hash (a int PRIMARY KEY); -- OK
CREATE TABLE xc_parent_modulo (a int PRIMARY KEY); -- OK
-- Test on roundrobin does not make sense as it cannot have a primary key
CREATE TABLE xc_parent_rr (a int PRIMARY KEY); -- error
-- Test creation of child tables referencing to parents
-- To replicated parent
CREATE TABLE xc_child_rep_to_rep (b int, FOREIGN KEY (b) REFERENCES xc_parent_rep(a)); -- OK
CREATE TABLE xc_child_rr_to_rep (b int, FOREIGN KEY (b) REFERENCES xc_parent_rep(a)); -- OK
CREATE TABLE xc_child_modulo_to_rep (b int, FOREIGN KEY (b) REFERENCES xc_parent_rep(a)); -- OK
CREATE TABLE xc_child_hash_to_rep (b int, FOREIGN KEY (b) REFERENCES xc_parent_rep(a)); -- OK
-- To hash parent
CREATE TABLE xc_child_rep_to_hash (b int, FOREIGN KEY (b) REFERENCES xc_parent_hash(a)); -- error
CREATE TABLE xc_child_rr_to_hash (b int, FOREIGN KEY (b) REFERENCES xc_parent_hash(a)); -- error
CREATE TABLE xc_child_modulo_to_hash (b int, FOREIGN KEY (b) REFERENCES xc_parent_hash(a)); -- error
CREATE TABLE xc_child_hash_to_hash (b int, FOREIGN KEY (b) REFERENCES xc_parent_hash(a)); -- OK
-- To modulo parent
CREATE TABLE xc_child_rep_to_modulo (b int, FOREIGN KEY (b) REFERENCES xc_parent_modulo(a)); -- error
CREATE TABLE xc_child_rr_to_modulo (b int, FOREIGN KEY (b) REFERENCES xc_parent_modulo(a)); -- error
CREATE TABLE xc_child_modulo_to_modulo (b int, FOREIGN KEY (b) REFERENCES xc_parent_modulo(a)); -- OK
CREATE TABLE xc_child_hash_to_modulo (b int, FOREIGN KEY (b) REFERENCES xc_parent_modulo(a)); -- error
-- Clean up
DROP SCHEMA xc_constraints_tests CASCADE;
