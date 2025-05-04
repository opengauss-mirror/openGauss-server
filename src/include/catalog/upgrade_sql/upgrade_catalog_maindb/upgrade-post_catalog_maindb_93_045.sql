DROP FUNCTION IF EXISTS pg_catalog.bm25build(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8456;
CREATE OR REPLACE FUNCTION pg_catalog.bm25build(internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25build';

DROP FUNCTION IF EXISTS pg_catalog.bm25buildempty(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8457;
CREATE OR REPLACE FUNCTION pg_catalog.bm25buildempty(internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25buildempty';

DROP FUNCTION IF EXISTS pg_catalog.bm25insert(internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8451;
CREATE OR REPLACE FUNCTION pg_catalog.bm25insert(internal, internal, internal, internal, internal, internal)
    RETURNS boolean
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25insert';

DROP FUNCTION IF EXISTS pg_catalog.bm25beginscan(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8452;
CREATE OR REPLACE FUNCTION pg_catalog.bm25beginscan(internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25beginscan';

DROP FUNCTION IF EXISTS pg_catalog.bm25rescan(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8454;
CREATE OR REPLACE FUNCTION pg_catalog.bm25rescan(internal, internal, internal, internal, internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25rescan';

DROP FUNCTION IF EXISTS pg_catalog.bm25gettuple(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8453;
CREATE OR REPLACE FUNCTION pg_catalog.bm25gettuple(internal, internal)
    RETURNS boolean
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25gettuple';

DROP FUNCTION IF EXISTS pg_catalog.bm25endscan(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8455;
CREATE OR REPLACE FUNCTION pg_catalog.bm25endscan(internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25endscan';

DROP FUNCTION IF EXISTS pg_catalog.bm25bulkdelete(internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8480;
CREATE OR REPLACE FUNCTION pg_catalog.bm25bulkdelete(internal, internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25bulkdelete';

DROP FUNCTION IF EXISTS pg_catalog.bm25vacuumcleanup(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8481;
CREATE OR REPLACE FUNCTION pg_catalog.bm25vacuumcleanup(internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25vacuumcleanup';

DROP FUNCTION IF EXISTS pg_catalog.bm25costestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8482;
CREATE OR REPLACE FUNCTION pg_catalog.bm25costestimate(internal, internal, internal, internal, internal, internal, internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25costestimate';

DROP FUNCTION IF EXISTS pg_catalog.bm25options(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8483;
CREATE OR REPLACE FUNCTION pg_catalog.bm25options(internal, internal)
    RETURNS bytea
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25options';

DROP FUNCTION IF EXISTS pg_catalog.bm25delete(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8484;
CREATE OR REPLACE FUNCTION pg_catalog.bm25delete(internal, internal, internal, internal, internal)
    RETURNS boolean
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25delete';

DROP FUNCTION if EXISTS pg_catalog.bm25_scores_textarr(_text, _text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8487;
CREATE OR REPLACE FUNCTION pg_catalog.bm25_scores_textarr(_text, _text)
 RETURNS double precision
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25_scores_textarr';
comment on function PG_CATALOG.bm25_scores_textarr(a _text, b _text) is 'implementation of <&> operator';

DROP FUNCTION if EXISTS pg_catalog.bm25_scores_text(text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 8489;
CREATE OR REPLACE FUNCTION pg_catalog.bm25_scores_text(text, text)
 RETURNS double precision
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'bm25_scores_text';
comment on function PG_CATALOG.bm25_scores_text(a text, b text) is 'implementation of <&> operator';

DO $$ BEGIN
IF NOT EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8302) THEN
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8302;
INSERT INTO pg_catalog.pg_am VALUES('bm25', 0, 5, false, true, false, false, false, true, false, false, false, false, false,
                                    0, 8451, 8452, 8453, 0, 8454, 8455, 0, 0, 0, 8456, 8457, 8480, 8481, 0, 8482, 8483, 0, 8484);
END IF;
END $$;

DROP OPERATOR IF EXISTS pg_catalog.<&> (_text, _text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6207;
CREATE OPERATOR pg_catalog.<&> (
    leftarg = _text, rightarg = _text, procedure = bm25_scores_textarr,
    commutator = operator(pg_catalog.<&>)
);
COMMENT ON OPERATOR pg_catalog.<&>(_text, _text) IS 'bm25 scores textarr';

DROP OPERATOR IF EXISTS pg_catalog.<&> (text, text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 6208;
CREATE OPERATOR pg_catalog.<&> (
    leftarg = text, rightarg = text, procedure = bm25_scores_text,
    commutator = operator(pg_catalog.<&>)
);
COMMENT ON OPERATOR pg_catalog.<&>(text, text) IS 'bm25 scores text';

DO $$ BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8302) THEN
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.bm25_text_ops USING bm25 CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 9738;
CREATE OPERATOR FAMILY pg_catalog.bm25_text_ops USING bm25;
    DROP OPERATOR FAMILY IF EXISTS pg_catalog.bm25_textarr_ops USING bm25 CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 9737;
CREATE OPERATOR FAMILY pg_catalog.bm25_textarr_ops USING bm25;
END IF;

IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 9737) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 9737;
END IF;
IF EXISTS(select * from pg_catalog.pg_amop where amopfamily = 9738) THEN
    DELETE pg_catalog.pg_amop where amopfamily = 9738;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 9737) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 9737;
END IF;
IF EXISTS(select * from pg_catalog.pg_amproc where amprocfamily = 9738) THEN
    DELETE pg_catalog.pg_amproc where amprocfamily = 9738;
END IF;
END $$;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;
DO $$ BEGIN
IF EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8302) THEN
    DROP OPERATOR CLASS IF EXISTS pg_catalog.bm25_textarr_ops USING bm25 CASCADE;
    CREATE OPERATOR CLASS pg_catalog.bm25_textarr_ops
        DEFAULT FOR TYPE _text USING bm25 family pg_catalog.bm25_textarr_ops AS
        OPERATOR 1 <&> (_text, _text) FOR ORDER BY float_ops,
        FUNCTION 1 bm25_scores_textarr(_text, _text);
    DROP OPERATOR CLASS IF EXISTS pg_catalog.bm25_text_ops USING bm25 CASCADE;
    CREATE OPERATOR CLASS pg_catalog.bm25_text_ops
        DEFAULT FOR TYPE text USING bm25 family pg_catalog.bm25_text_ops AS
        OPERATOR 1 <&> (text, text) FOR ORDER BY float_ops,
        FUNCTION 1 bm25_scores_text(text, text);
END IF;
END $$;