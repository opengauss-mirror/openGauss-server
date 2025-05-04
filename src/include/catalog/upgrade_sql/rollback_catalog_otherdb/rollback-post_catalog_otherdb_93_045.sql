DO $$  BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8302) THEN
    DROP OPERATOR CLASS IF EXISTS pg_catalog.bm25_textarr_ops USING bm25 cascade;
    DROP OPERATOR CLASS IF EXISTS pg_catalog.bm25_text_ops USING bm25 cascade;
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.bm25_textarr_ops USING bm25 cascade;
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.bm25_text_ops USING bm25 cascade;
END IF;

IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 6509) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 6509;
END IF;
IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 6510) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 6510;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 6509) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 6509;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 6510) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 6510;
END IF;

DROP FUNCTION IF EXISTS pg_catalog.bm25_scores_textarr(_text, _text) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25_scores_text(text, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<&> (_text, _text);
DROP OPERATOR IF EXISTS pg_catalog.<&> (text, text);
END $$ ;

DO $$  BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8302) THEN
    DELETE FROM pg_catalog.pg_am WHERE oid = 8302;
END IF;
DROP FUNCTION IF EXISTS pg_catalog.bm25build(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25buildempty(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25insert(internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25beginscan(internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25gettuple(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25rescan(internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25endscan(internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25bulkdelete(internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25vacuumcleanup(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25costestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25options(internal, internal) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.bm25delete(internal, internal, internal, internal, internal) CASCADE;
END $$ ;
