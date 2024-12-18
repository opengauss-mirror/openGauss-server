-- Test keywords used in statements, refers to `kwlist.h`

CREATE SCHEMA keywords;
SET CURRENT_SCHEMA TO keywords;

-- Unreserved keywords
CREATE TABLE following (final int);
DROP TABLE following;
CREATE TYPE following;
DROP TYPE following;

-- Col name keywords
CREATE TABLE float (forward int);
DROP TABLE float;
CREATE TYPE grouping; -- Will fail
DROP TYPE grouping; -- Will fail

-- Type name keywords
CREATE TABLE full (freeze int); -- Will fail
DROP TABLE full; -- Will fail
CREATE TYPE full;
DROP TYPE full;

SET CURRENT_SCHEMA TO DEFAULT;
DROP SCHEMA keywords;
