DROP FUNCTION IF EXISTS pg_catalog.blbuild(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4480;
CREATE OR REPLACE FUNCTION pg_catalog.blbuild(internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blbuild';

DROP FUNCTION IF EXISTS pg_catalog.blbuildempty(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4481;
CREATE OR REPLACE FUNCTION pg_catalog.blbuildempty(internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blbuildempty';

DROP FUNCTION IF EXISTS pg_catalog.blinsert(internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4482;
CREATE OR REPLACE FUNCTION pg_catalog.blinsert(internal, internal, internal, internal, internal, internal)
    RETURNS boolean
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blinsert';

DROP FUNCTION IF EXISTS pg_catalog.blbeginscan(internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4488;
CREATE OR REPLACE FUNCTION pg_catalog.blbeginscan(internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blbeginscan';

DROP FUNCTION IF EXISTS pg_catalog.blrescan(internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4489;
CREATE OR REPLACE FUNCTION pg_catalog.blrescan(internal, internal, internal, internal, internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blrescan';

DROP FUNCTION IF EXISTS pg_catalog.blendscan(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4490;
CREATE OR REPLACE FUNCTION pg_catalog.blendscan(internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blendscan';

DROP FUNCTION IF EXISTS pg_catalog.blbulkdelete(internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4483;
CREATE OR REPLACE FUNCTION pg_catalog.blbulkdelete(internal, internal, internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blbulkdelete';

DROP FUNCTION IF EXISTS pg_catalog.blvacuumcleanup(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4484;
CREATE OR REPLACE FUNCTION pg_catalog.blvacuumcleanup(internal, internal)
    RETURNS internal
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blvacuumcleanup';

DROP FUNCTION IF EXISTS pg_catalog.blcostestimate(internal, internal, internal, internal, internal, internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4485;
CREATE OR REPLACE FUNCTION pg_catalog.blcostestimate(internal, internal, internal, internal, internal, internal, internal)
    RETURNS void
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blcostestimate';

DROP FUNCTION IF EXISTS pg_catalog.bloptions(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4486;
CREATE OR REPLACE FUNCTION pg_catalog.bloptions(internal, internal)
    RETURNS internal
    LANGUAGE internal
    STABLE STRICT NOT FENCED NOT SHIPPABLE
AS 'bloptions';

DROP FUNCTION IF EXISTS pg_catalog.blgetbitmap(internal, internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 4492;
CREATE OR REPLACE FUNCTION pg_catalog.blgetbitmap(internal, internal)
    RETURNS int8
    LANGUAGE internal
    STRICT NOT FENCED NOT SHIPPABLE
AS 'blgetbitmap';

DO $$ BEGIN
IF NOT EXISTS(SELECT * FROM pg_catalog.pg_am WHERE oid = 8304) THEN
    SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8304;
INSERT INTO pg_catalog.pg_am VALUES('bloom', 1, 1, false, false, false, false, true, true, false, false, false, false, false,
                                    0, 4482, 4488, 0, 4492, 4489, 4490, 0, 0, 0, 4480, 4481, 4483, 4484, 0, 4485, 4486, 0, 0);
END IF;
END $$;

DROP OPERATOR FAMILY IF EXISTS pg_catalog.int4_ops USING bloom CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8340;
CREATE OPERATOR FAMILY pg_catalog.int4_ops USING bloom;

DROP OPERATOR FAMILY IF EXISTS pg_catalog.text_ops USING bloom CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8341;
CREATE OPERATOR FAMILY pg_catalog.text_ops USING bloom;

DROP OPERATOR CLASS IF EXISTS pg_catalog.int4_ops USING bloom CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8940;
CREATE OPERATOR CLASS pg_catalog.int4_ops
    DEFAULT FOR TYPE int4 USING bloom family pg_catalog.int4_ops AS
    OPERATOR    1    =(int4, int4),
    FUNCTION    1    hashint4(int4);

DROP OPERATOR CLASS IF EXISTS pg_catalog.text_ops USING bloom CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 8941;
CREATE OPERATOR CLASS pg_catalog.text_ops
    DEFAULT FOR TYPE text USING bloom family pg_catalog.text_ops AS
    OPERATOR    1    =(text, text),
    FUNCTION    1    hashtext(text);