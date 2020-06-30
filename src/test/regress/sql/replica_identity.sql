CREATE TABLE test_replica_identity (
       id serial primary key,
       keya text not null,
       keyb text not null,
       nonkey text
);

CREATE TABLE test_replica_identity_othertable (id serial primary key);

CREATE INDEX test_replica_identity_keyab ON test_replica_identity (keya, keyb);
CREATE INDEX test_replica_identity_hash ON test_replica_identity USING hash (nonkey);

-- default is 'd'/DEFAULT for user created tables
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;
-- but 'none' for system tables
SELECT relreplident FROM pg_class WHERE oid = 'pg_class'::regclass;
SELECT relreplident FROM pg_class WHERE oid = 'pg_constraint'::regclass;

----
-- Make sure we detect inelegible indexes
----

-- fail, not unique
ALTER TABLE test_replica_identity REPLICA IDENTITY USING INDEX test_replica_identity_keyab;
-- fail, not a candidate key, nullable column
-- fail, hash indexes cannot do uniqueness
ALTER TABLE test_replica_identity REPLICA IDENTITY USING INDEX test_replica_identity_hash;
-- fail, not our index
ALTER TABLE test_replica_identity REPLICA IDENTITY USING INDEX test_replica_identity_othertable_pkey;

SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;

----
-- Make sure index cases succeeed
----

-- succeed, primary key
ALTER TABLE test_replica_identity REPLICA IDENTITY USING INDEX test_replica_identity_pkey;
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;
\d test_replica_identity

SELECT count(*) FROM pg_index WHERE indrelid = 'test_replica_identity'::regclass AND indisreplident;

----
-- Make sure non index cases work
----
ALTER TABLE test_replica_identity REPLICA IDENTITY DEFAULT;
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;
SELECT count(*) FROM pg_index WHERE indrelid = 'test_replica_identity'::regclass AND indisreplident;

ALTER TABLE test_replica_identity REPLICA IDENTITY FULL;
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;
\d test_replica_identity
ALTER TABLE test_replica_identity REPLICA IDENTITY NOTHING;
SELECT relreplident FROM pg_class WHERE oid = 'test_replica_identity'::regclass;

DROP TABLE test_replica_identity;
DROP TABLE test_replica_identity_othertable;
