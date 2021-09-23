SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 9027, 9108, 4392, 4393;
CREATE TABLE pg_catalog.pg_hashbucket_tmp_9027
(
   bucketid OID NOCOMPRESS NOT NULL,
   bucketcnt INT4 NOCOMPRESS NOT NULL,
   bucketmapsize INT4 NOCOMPRESS NOT NULL,
   bucketref INT4 NOCOMPRESS NOT NULL,
   bucketvector oidvector_extend NOCOMPRESS
)WITH OIDS;
GRANT SELECT ON pg_catalog.pg_hashbucket_tmp_9027 TO public;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3492;
CREATE UNIQUE INDEX pg_hashbucket_tmp_9027_oid_index ON pg_catalog.pg_hashbucket_tmp_9027 USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3493;
CREATE INDEX pg_hashbucket_tmp_9027_bid_index ON pg_catalog.pg_hashbucket_tmp_9027 USING BTREE(bucketid OID_OPS, bucketcnt INT4_OPS, bucketmapsize INT4_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

ALTER TABLE IF EXISTS pg_catalog.pg_hashbucket rename to pg_hashbucket_tmp_9026;
ALTER INDEX IF EXISTS pg_hashbucket_oid_index rename to pg_hashbucket_tmp_9026_oid_index;
ALTER INDEX IF EXISTS pg_hashbucket_bid_index rename to pg_hashbucket_tmp_9026_bid_index;

