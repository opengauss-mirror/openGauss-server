CREATE OR REPLACE FUNCTION drop_pg_hashbucket()
RETURNS void
AS $$
DECLARE
    qry text;
    cnt integer;
    dropqry text;
BEGIN
    qry := 'select count(*) from pg_class where relname=''pg_hashbucket_tmp_9026''';
    execute qry into cnt;
    dropqry := 'DROP INDEX IF EXISTS pg_toast.pg_toast_9027_index;DROP TYPE IF EXISTS pg_toast.pg_toast_9027;DROP TABLE IF EXISTS pg_toast.pg_toast_9027;DROP INDEX IF EXISTS pg_catalog.pg_hashbucket_oid_index;DROP INDEX IF EXISTS pg_catalog.pg_hashbucket_bid_index;DROP TYPE IF EXISTS pg_catalog.pg_hashbucket;DROP TABLE IF EXISTS pg_catalog.pg_hashbucket;DROP INDEX IF EXISTS pg_catalog.pg_hashbucket_tmp_9027_oid_index;DROP INDEX IF EXISTS pg_catalog.pg_hashbucket_tmp_9027_bid_index;DROP TYPE IF EXISTS pg_catalog.pg_hashbucket_tmp_9027;DROP TABLE IF EXISTS pg_catalog.pg_hashbucket_tmp_9027;ALTER TABLE pg_catalog.pg_hashbucket_tmp_9026 rename to pg_hashbucket;ALTER INDEX pg_hashbucket_tmp_9026_oid_index rename to pg_hashbucket_oid_index;ALTER INDEX pg_hashbucket_tmp_9026_bid_index rename to pg_hashbucket_bid_index;';
    IF cnt = 1 THEN
        execute dropqry;
    END IF;
END; $$
LANGUAGE plpgsql NOT FENCED;
select drop_pg_hashbucket();

