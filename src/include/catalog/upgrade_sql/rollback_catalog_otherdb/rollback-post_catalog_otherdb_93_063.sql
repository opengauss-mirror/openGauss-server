DO $$  BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8304) THEN
    DROP OPERATOR CLASS IF EXISTS pg_catalog.int4_ops USING bloom cascade;
    DROP OPERATOR CLASS IF EXISTS pg_catalog.text_ops USING bloom cascade;
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.int4_ops USING bloom cascade;
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.text_ops USING bloom cascade;
END IF;

IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 8340) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 8340;
END IF;
IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 8341) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 8341;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 8340) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 8340;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 8341) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 8341;
END IF;
END $$;

DO $$  BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8304) THEN
    DELETE FROM pg_catalog.pg_am WHERE oid = 8304;
END IF;

DROP FUNCTION IF EXISTS pg_catalog.blbuild(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blbuildempty(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blinsert(internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blbeginscan(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blrescan(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blendscan(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blbulkdelete(internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blvacuumcleanup(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bloptions(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.blgetbitmap(internal, internal) CASCADE;
END $$;