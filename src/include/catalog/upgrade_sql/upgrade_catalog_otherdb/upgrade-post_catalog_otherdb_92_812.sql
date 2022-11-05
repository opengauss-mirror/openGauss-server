--------------------------------------------------------------
-- add pg_set table
--------------------------------------------------------------
DROP TYPE IF EXISTS pg_catalog.pg_set;
DROP TABLE IF EXISTS pg_catalog.pg_set;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 3516, 9377, 0, 0;
CREATE TABLE pg_catalog.pg_set
(
  settypid oid NOT NULL,
  setnum tinyint NOT NULL,
  setsortorder tinyint NOT NULL,
  setlabel text NOCOMPRESS
) WITH OIDS TABLESPACE pg_default;

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3517;
CREATE UNIQUE INDEX pg_set_oid_index ON pg_catalog.pg_set USING BTREE(oid OID_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3518;
CREATE UNIQUE INDEX pg_set_typid_label_index ON pg_catalog.pg_set USING BTREE(settypid OID_OPS, setlabel TEXT_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 3519;
CREATE UNIQUE INDEX pg_set_typid_order_index ON pg_catalog.pg_set USING BTREE(settypid OID_OPS, setsortorder INT1_OPS);

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_CATALOG, false, true, 0, 0, 0, 0;

GRANT SELECT ON TABLE pg_catalog.pg_set TO PUBLIC;

--------------------------------------------------------------
-- add new data type : anyset
--------------------------------------------------------------

DROP TYPE IF EXISTS pg_catalog.anyset;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 3272, 0, s;
CREATE TYPE pg_catalog.anyset;

DROP FUNCTION IF EXISTS pg_catalog.anyset_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3295;
CREATE FUNCTION pg_catalog.anyset_in (
cstring
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'anyset_in';

DROP FUNCTION IF EXISTS pg_catalog.anyset_out(anyset) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3296;
CREATE FUNCTION pg_catalog.anyset_out (
anyset
) RETURNS cstring LANGUAGE INTERNAL STABLE STRICT as 'anyset_out';

CREATE TYPE pg_catalog.anyset (
  INPUT=anyset_in,
  OUTPUT=anyset_out,
  STORAGE=plain,
  CATEGORY='H',
  PREFERRED=true
  );
COMMENT ON TYPE pg_catalog.anyset IS 'set type';

--------------------------------------------------------------
-- compare & coerce & other functions
--------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.setlt(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6533; 
CREATE FUNCTION pg_catalog.setlt ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setlt';

DROP FUNCTION IF EXISTS pg_catalog.setint2lt(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6518; 
CREATE FUNCTION pg_catalog.setint2lt ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2lt';

DROP FUNCTION IF EXISTS pg_catalog.setint4lt(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6524; 
CREATE FUNCTION pg_catalog.setint4lt ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4lt';

DROP FUNCTION IF EXISTS pg_catalog.setint8lt(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6530; 
CREATE FUNCTION pg_catalog.setint8lt ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8lt';

DROP FUNCTION IF EXISTS pg_catalog.settextlt(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6568; 
CREATE FUNCTION pg_catalog.settextlt ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settextlt';

DROP FUNCTION IF EXISTS pg_catalog.int2setlt(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6495; 
CREATE FUNCTION pg_catalog.int2setlt ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2setlt';

DROP FUNCTION IF EXISTS pg_catalog.int4setlt(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6501; 
CREATE FUNCTION pg_catalog.int4setlt ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4setlt';

DROP FUNCTION IF EXISTS pg_catalog.int8setlt(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6507; 
CREATE FUNCTION pg_catalog.int8setlt ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8setlt';

DROP FUNCTION IF EXISTS pg_catalog.textsetlt(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6574; 
CREATE FUNCTION pg_catalog.textsetlt ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textsetlt';

DROP FUNCTION IF EXISTS pg_catalog.setle(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6532; 
CREATE FUNCTION pg_catalog.setle ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setle';

DROP FUNCTION IF EXISTS pg_catalog.setint2le(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6517; 
CREATE FUNCTION pg_catalog.setint2le ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2le';

DROP FUNCTION IF EXISTS pg_catalog.setint4le(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6523; 
CREATE FUNCTION pg_catalog.setint4le ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4le';

DROP FUNCTION IF EXISTS pg_catalog.setint8le(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6529; 
CREATE FUNCTION pg_catalog.setint8le ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8le';

DROP FUNCTION IF EXISTS pg_catalog.settextle(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6567; 
CREATE FUNCTION pg_catalog.settextle ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settextle';

DROP FUNCTION IF EXISTS pg_catalog.int2setle(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6494; 
CREATE FUNCTION pg_catalog.int2setle ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2setle';

DROP FUNCTION IF EXISTS pg_catalog.int4setle(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6500; 
CREATE FUNCTION pg_catalog.int4setle ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4setle';

DROP FUNCTION IF EXISTS pg_catalog.int8setle(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6506; 
CREATE FUNCTION pg_catalog.int8setle ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8setle';

DROP FUNCTION IF EXISTS pg_catalog.textsetle(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6573; 
CREATE FUNCTION pg_catalog.textsetle ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textsetle';

DROP FUNCTION IF EXISTS pg_catalog.setne(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6534; 
CREATE FUNCTION pg_catalog.setne ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setne';

DROP FUNCTION IF EXISTS pg_catalog.setint2ne(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6519; 
CREATE FUNCTION pg_catalog.setint2ne ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2ne';

DROP FUNCTION IF EXISTS pg_catalog.setint4ne(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6525; 
CREATE FUNCTION pg_catalog.setint4ne ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4ne';

DROP FUNCTION IF EXISTS pg_catalog.setint8ne(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6531; 
CREATE FUNCTION pg_catalog.setint8ne ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8ne';

DROP FUNCTION IF EXISTS pg_catalog.settextne(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6569; 
CREATE FUNCTION pg_catalog.settextne ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settextne';

DROP FUNCTION IF EXISTS pg_catalog.int2setne(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6496; 
CREATE FUNCTION pg_catalog.int2setne ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2setne';

DROP FUNCTION IF EXISTS pg_catalog.int4setne(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6502; 
CREATE FUNCTION pg_catalog.int4setne ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4setne';

DROP FUNCTION IF EXISTS pg_catalog.int8setne(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6508; 
CREATE FUNCTION pg_catalog.int8setne ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8setne';

DROP FUNCTION IF EXISTS pg_catalog.textsetne(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6575; 
CREATE FUNCTION pg_catalog.textsetne ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textsetne';

DROP FUNCTION IF EXISTS pg_catalog.seteq(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6511; 
CREATE FUNCTION pg_catalog.seteq ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'seteq';

DROP FUNCTION IF EXISTS pg_catalog.setint2eq(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6514; 
CREATE FUNCTION pg_catalog.setint2eq ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2eq';

DROP FUNCTION IF EXISTS pg_catalog.setint4eq(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6520; 
CREATE FUNCTION pg_catalog.setint4eq ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4eq';

DROP FUNCTION IF EXISTS pg_catalog.setint8eq(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6526; 
CREATE FUNCTION pg_catalog.setint8eq ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8eq';

DROP FUNCTION IF EXISTS pg_catalog.settexteq(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6564; 
CREATE FUNCTION pg_catalog.settexteq ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settexteq';

DROP FUNCTION IF EXISTS pg_catalog.int2seteq(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6491; 
CREATE FUNCTION pg_catalog.int2seteq ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2seteq';

DROP FUNCTION IF EXISTS pg_catalog.int4seteq(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6497; 
CREATE FUNCTION pg_catalog.int4seteq ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4seteq';

DROP FUNCTION IF EXISTS pg_catalog.int8seteq(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6503; 
CREATE FUNCTION pg_catalog.int8seteq ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8seteq';

DROP FUNCTION IF EXISTS pg_catalog.textseteq(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6570; 
CREATE FUNCTION pg_catalog.textseteq ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textseteq';

DROP FUNCTION IF EXISTS pg_catalog.setgt(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6513; 
CREATE FUNCTION pg_catalog.setgt ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setgt';

DROP FUNCTION IF EXISTS pg_catalog.setint2gt(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6516; 
CREATE FUNCTION pg_catalog.setint2gt ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2gt';

DROP FUNCTION IF EXISTS pg_catalog.setint4gt(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6522; 
CREATE FUNCTION pg_catalog.setint4gt ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4gt';

DROP FUNCTION IF EXISTS pg_catalog.setint8gt(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6528; 
CREATE FUNCTION pg_catalog.setint8gt ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8gt';

DROP FUNCTION IF EXISTS pg_catalog.settextgt(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6566; 
CREATE FUNCTION pg_catalog.settextgt ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settextgt';

DROP FUNCTION IF EXISTS pg_catalog.int2setgt(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6493; 
CREATE FUNCTION pg_catalog.int2setgt ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2setgt';

DROP FUNCTION IF EXISTS pg_catalog.int4setgt(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6499; 
CREATE FUNCTION pg_catalog.int4setgt ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4setgt';

DROP FUNCTION IF EXISTS pg_catalog.int8setgt(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6505; 
CREATE FUNCTION pg_catalog.int8setgt ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8setgt';

DROP FUNCTION IF EXISTS pg_catalog.textsetgt(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6572; 
CREATE FUNCTION pg_catalog.textsetgt ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textsetgt';

DROP FUNCTION IF EXISTS pg_catalog.setge(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6512; 
CREATE FUNCTION pg_catalog.setge ( 
anyset, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setge';

DROP FUNCTION IF EXISTS pg_catalog.setint2ge(anyset, int2) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6515; 
CREATE FUNCTION pg_catalog.setint2ge ( 
anyset, int2 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint2ge';

DROP FUNCTION IF EXISTS pg_catalog.setint4ge(anyset, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6521; 
CREATE FUNCTION pg_catalog.setint4ge ( 
anyset, int4 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint4ge';

DROP FUNCTION IF EXISTS pg_catalog.setint8ge(anyset, int8) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6527; 
CREATE FUNCTION pg_catalog.setint8ge ( 
anyset, int8 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'setint8ge';

DROP FUNCTION IF EXISTS pg_catalog.settextge(anyset, text) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6565; 
CREATE FUNCTION pg_catalog.settextge ( 
anyset, text 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'settextge';

DROP FUNCTION IF EXISTS pg_catalog.int2setge(int2, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6492; 
CREATE FUNCTION pg_catalog.int2setge ( 
int2, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int2setge';

DROP FUNCTION IF EXISTS pg_catalog.int4setge(int4, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6498; 
CREATE FUNCTION pg_catalog.int4setge ( 
int4, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int4setge';

DROP FUNCTION IF EXISTS pg_catalog.int8setge(int8, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6504; 
CREATE FUNCTION pg_catalog.int8setge ( 
int8, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE LEAKPROOF STRICT as 'int8setge';

DROP FUNCTION IF EXISTS pg_catalog.btint2setcmp(int2, anyset) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6548; 
CREATE FUNCTION pg_catalog.btint2setcmp ( 
int2, anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btint2setcmp';

DROP FUNCTION IF EXISTS pg_catalog.btint4setcmp(int4, anyset) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6543; 
CREATE FUNCTION pg_catalog.btint4setcmp ( 
int4, anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btint4setcmp';

DROP FUNCTION IF EXISTS pg_catalog.btint8setcmp(int8, anyset) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6547; 
CREATE FUNCTION pg_catalog.btint8setcmp ( 
int8, anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btint8setcmp';

DROP FUNCTION IF EXISTS pg_catalog.btsetcmp(anyset, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6544; 
CREATE FUNCTION pg_catalog.btsetcmp ( 
anyset, anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btsetcmp';

DROP FUNCTION IF EXISTS pg_catalog.btsetint2cmp(anyset, int2) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6549; 
CREATE FUNCTION pg_catalog.btsetint2cmp ( 
anyset, int2 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btsetint2cmp';

DROP FUNCTION IF EXISTS pg_catalog.btsetint4cmp(anyset, int4) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6545; 
CREATE FUNCTION pg_catalog.btsetint4cmp ( 
anyset, int4 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btsetint4cmp';

DROP FUNCTION IF EXISTS pg_catalog.btsetint8cmp(anyset, int8) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6546; 
CREATE FUNCTION pg_catalog.btsetint8cmp ( 
anyset, int8 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'btsetint8cmp';

DROP FUNCTION IF EXISTS pg_catalog.hashsetint(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3294; 
CREATE FUNCTION pg_catalog.hashsetint ( 
anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'hashsetint';

DROP FUNCTION IF EXISTS pg_catalog.hashsettext(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3297; 
CREATE FUNCTION pg_catalog.hashsettext ( 
anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'hashsettext';

DROP FUNCTION IF EXISTS pg_catalog.btsetsortsupport(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6538; 
CREATE FUNCTION pg_catalog.btsetsortsupport ( 
internal 
) RETURNS void LANGUAGE INTERNAL IMMUTABLE STRICT as 'btsetsortsupport';

DROP FUNCTION IF EXISTS pg_catalog.find_in_set(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6537; 
CREATE FUNCTION pg_catalog.find_in_set ( 
text, anyset 
) RETURNS int2 LANGUAGE INTERNAL IMMUTABLE STRICT as 'findinset';

DROP FUNCTION IF EXISTS pg_catalog.float4(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3305; 
CREATE FUNCTION pg_catalog.float4 ( 
anyset 
) RETURNS float4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'float4';

DROP FUNCTION IF EXISTS pg_catalog.float8(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3304; 
CREATE FUNCTION pg_catalog.float8 ( 
anyset 
) RETURNS float8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'float8';

DROP FUNCTION IF EXISTS pg_catalog.int2(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3300; 
CREATE FUNCTION pg_catalog.int2 ( 
anyset 
) RETURNS int2 LANGUAGE INTERNAL IMMUTABLE STRICT as 'int2';

DROP FUNCTION IF EXISTS pg_catalog.int4(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3301; 
CREATE FUNCTION pg_catalog.int4 ( 
anyset 
) RETURNS int4 LANGUAGE INTERNAL IMMUTABLE STRICT as 'int4';

DROP FUNCTION IF EXISTS pg_catalog.int8(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3302; 
CREATE FUNCTION pg_catalog.int8 ( 
anyset 
) RETURNS int8 LANGUAGE INTERNAL IMMUTABLE STRICT as 'int8';

DROP FUNCTION IF EXISTS pg_catalog.set(int2, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3307; 
CREATE FUNCTION pg_catalog.set ( 
int2, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'i2toset';

DROP FUNCTION IF EXISTS pg_catalog.set(int4, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3310; 
CREATE FUNCTION pg_catalog.set ( 
int4, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'i4toset';

DROP FUNCTION IF EXISTS pg_catalog.set(int8, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3309; 
CREATE FUNCTION pg_catalog.set ( 
int8, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'i8toset';

DROP FUNCTION IF EXISTS pg_catalog.set(float8, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3311; 
CREATE FUNCTION pg_catalog.set ( 
float8, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'dtoset';

DROP FUNCTION IF EXISTS pg_catalog.set(float4, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3312; 
CREATE FUNCTION pg_catalog.set ( 
float4, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'ftoset';

DROP FUNCTION IF EXISTS pg_catalog.set(numeric, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3313; 
CREATE FUNCTION pg_catalog.set ( 
numeric, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'numbertoset';

DROP FUNCTION IF EXISTS pg_catalog.set(bpchar, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3314; 
CREATE FUNCTION pg_catalog.set ( 
bpchar, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'bpchartoset';

DROP FUNCTION IF EXISTS pg_catalog.set(varchar, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3315; 
CREATE FUNCTION pg_catalog.set ( 
varchar, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'varchartoset';

DROP FUNCTION IF EXISTS pg_catalog.set(text, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3320; 
CREATE FUNCTION pg_catalog.set ( 
text, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'texttoset';

DROP FUNCTION IF EXISTS pg_catalog.set(nvarchar2, int4) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3321; 
CREATE FUNCTION pg_catalog.set ( 
nvarchar2, int4 
) RETURNS anyset LANGUAGE INTERNAL IMMUTABLE STRICT as 'nvarchar2toset';

DROP FUNCTION IF EXISTS pg_catalog.set_in(cstring, oid) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3352; 
CREATE FUNCTION pg_catalog.set_in ( 
cstring, oid 
) RETURNS varbit LANGUAGE INTERNAL STABLE STRICT as 'set_in';

DROP FUNCTION IF EXISTS pg_catalog.set_out(varbit) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3353; 
CREATE FUNCTION pg_catalog.set_out ( 
varbit 
) RETURNS cstring LANGUAGE INTERNAL STABLE STRICT as 'set_out';

DROP FUNCTION IF EXISTS pg_catalog.set_recv(cstring, oid) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3335; 
CREATE FUNCTION pg_catalog.set_recv ( 
cstring, oid 
) RETURNS varbit LANGUAGE INTERNAL STABLE STRICT as 'set_recv';

DROP FUNCTION IF EXISTS pg_catalog.set_send(varbit) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3336; 
CREATE FUNCTION pg_catalog.set_send ( 
varbit 
) RETURNS bytea LANGUAGE INTERNAL STABLE STRICT as 'set_send';

DROP FUNCTION IF EXISTS pg_catalog.settobpchar(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3322; 
CREATE FUNCTION pg_catalog.settobpchar ( 
anyset 
) RETURNS bpchar LANGUAGE INTERNAL IMMUTABLE STRICT as 'settobpchar';

DROP FUNCTION IF EXISTS pg_catalog.settonumber(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3306; 
CREATE FUNCTION pg_catalog.settonumber ( 
anyset 
) RETURNS numeric LANGUAGE INTERNAL IMMUTABLE STRICT as 'settonumber';

DROP FUNCTION IF EXISTS pg_catalog.settonvarchar2(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3323; 
CREATE FUNCTION pg_catalog.settonvarchar2 ( 
anyset 
) RETURNS nvarchar2 LANGUAGE INTERNAL IMMUTABLE STRICT as 'settonvarchar2';

DROP FUNCTION IF EXISTS pg_catalog.settotext(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3324; 
CREATE FUNCTION pg_catalog.settotext ( 
anyset 
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'settotext';

DROP FUNCTION IF EXISTS pg_catalog.settovarchar(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3325; 
CREATE FUNCTION pg_catalog.settovarchar ( 
anyset 
) RETURNS varchar LANGUAGE INTERNAL IMMUTABLE STRICT as 'settovarchar';

DROP FUNCTION IF EXISTS pg_catalog.textsetge(text, anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6571; 
CREATE FUNCTION pg_catalog.textsetge ( 
text, anyset 
) RETURNS bool LANGUAGE INTERNAL IMMUTABLE STRICT as 'textsetge';

DROP FUNCTION IF EXISTS pg_catalog.to_char(anyset) CASCADE; 
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 6550; 
CREATE FUNCTION pg_catalog.to_char ( 
anyset 
) RETURNS text LANGUAGE INTERNAL IMMUTABLE STRICT as 'to_char';

-- add comment for functions
COMMENT ON FUNCTION pg_catalog.anyset_in(cstring) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.anyset_out(anyset) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.setlt(anyset,anyset) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.setint2lt(anyset,int2) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.setint4lt(anyset,int4) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.setint8lt(anyset,int8) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.settextlt(anyset,text) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.int2setlt(int2,anyset) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.int4setlt(int4,anyset) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.int8setlt(int8,anyset) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.textsetlt(text,anyset) IS 'implementation of < operator';
COMMENT ON FUNCTION pg_catalog.setle(anyset,anyset) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.setint2le(anyset,int2) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.setint4le(anyset,int4) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.setint8le(anyset,int8) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.settextle(anyset,text) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.int2setle(int2,anyset) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.int4setle(int4,anyset) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.int8setle(int8,anyset) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.textsetle(text,anyset) IS 'implementation of <= operator';
COMMENT ON FUNCTION pg_catalog.setne(anyset,anyset) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.setint2ne(anyset,int2) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.setint4ne(anyset,int4) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.setint8ne(anyset,int8) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.settextne(anyset,text) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.int2setne(int2,anyset) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.int4setne(int4,anyset) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.int8setne(int8,anyset) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.textsetne(text,anyset) IS 'implementation of <> operator';
COMMENT ON FUNCTION pg_catalog.seteq(anyset,anyset) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.setint2eq(anyset,int2) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.setint4eq(anyset,int4) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.setint8eq(anyset,int8) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.settexteq(anyset,text) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.int2seteq(int2,anyset) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.int4seteq(int4,anyset) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.int8seteq(int8,anyset) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.textseteq(text,anyset) IS 'implementation of = operator';
COMMENT ON FUNCTION pg_catalog.setgt(anyset,anyset) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.setint2gt(anyset,int2) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.setint4gt(anyset,int4) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.setint8gt(anyset,int8) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.settextgt(anyset,text) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.int2setgt(int2,anyset) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.int4setgt(int4,anyset) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.int8setgt(int8,anyset) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.textsetgt(text,anyset) IS 'implementation of > operator';
COMMENT ON FUNCTION pg_catalog.setge(anyset,anyset) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.setint2ge(anyset,int2) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.setint4ge(anyset,int4) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.setint8ge(anyset,int8) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.settextge(anyset,text) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.int2setge(int2,anyset) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.int4setge(int4,anyset) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.int8setge(int8,anyset) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.btint2setcmp(int2,anyset) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btint4setcmp(int4,anyset) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btint8setcmp(int8,anyset) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btsetcmp(anyset,anyset) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btsetint2cmp(anyset,int2) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btsetint4cmp(anyset,int4) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.btsetint8cmp(anyset,int8) IS 'less-equal-greater';
COMMENT ON FUNCTION pg_catalog.hashsetint(anyset) IS 'hash set as integer';
COMMENT ON FUNCTION pg_catalog.hashsettext(anyset) IS 'hash set as integer';
COMMENT ON FUNCTION pg_catalog.btsetsortsupport(internal) IS 'sort support';
COMMENT ON FUNCTION pg_catalog.find_in_set(text,anyset) IS 'find position in set';
COMMENT ON FUNCTION pg_catalog.float4(anyset) IS 'convert set to float4';
COMMENT ON FUNCTION pg_catalog.float8(anyset) IS 'convert set to float8';
COMMENT ON FUNCTION pg_catalog.int2(anyset) IS 'convert set to int2';
COMMENT ON FUNCTION pg_catalog.int4(anyset) IS 'convert set to int4';
COMMENT ON FUNCTION pg_catalog.int8(anyset) IS 'convert set to int8';
COMMENT ON FUNCTION pg_catalog.set(int2,int4) IS 'convert int2 to set';
COMMENT ON FUNCTION pg_catalog.set(int4,int4) IS 'convert int4 to set';
COMMENT ON FUNCTION pg_catalog.set(int8,int4) IS 'convert int8 to set';
COMMENT ON FUNCTION pg_catalog.set(float8,int4) IS 'convert double to set';
COMMENT ON FUNCTION pg_catalog.set(float4,int4) IS 'convert float to set';
COMMENT ON FUNCTION pg_catalog.set(numeric,int4) IS 'convert number to set';
COMMENT ON FUNCTION pg_catalog.set(bpchar,int4) IS 'convert bpchar to set';
COMMENT ON FUNCTION pg_catalog.set(varchar,int4) IS 'convert varchar to set';
COMMENT ON FUNCTION pg_catalog.set(text,int4) IS 'convert text to set';
COMMENT ON FUNCTION pg_catalog.set(nvarchar2,int4) IS 'convert nvarchar2 to set';
COMMENT ON FUNCTION pg_catalog.set_in(cstring,oid) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.set_out(varbit) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.set_recv(cstring,oid) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.set_send(varbit) IS 'I/O';
COMMENT ON FUNCTION pg_catalog.settobpchar(anyset) IS 'convert set to bpchar';
COMMENT ON FUNCTION pg_catalog.settonumber(anyset) IS 'convert set to number';
COMMENT ON FUNCTION pg_catalog.settonvarchar2(anyset) IS 'convert set to nvarchar2';
COMMENT ON FUNCTION pg_catalog.settotext(anyset) IS 'convert set to text';
COMMENT ON FUNCTION pg_catalog.settovarchar(anyset) IS 'convert set to varchar';
COMMENT ON FUNCTION pg_catalog.textsetge(text,anyset) IS 'implementation of >= operator';
COMMENT ON FUNCTION pg_catalog.to_char(anyset) IS 'format set to text';

--------------------------------------------------------------------
-- add new operators
--------------------------------------------------------------------

DROP OPERATOR IF EXISTS pg_catalog.=(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(anyset, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(anyset, int8) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(anyset, int2) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(anyset, int4) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(int8, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(int2, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(int4, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(anyset, text) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.=(text, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<>(text, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<(text, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>(text, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.<=(text, anyset) CASCADE;
DROP OPERATOR IF EXISTS pg_catalog.>=(text, anyset) CASCADE;

-- create operator with oid auto-allocated
CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = seteq, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = setne, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = setlt, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = setgt, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = setle, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = setge, LEFTARG = anyset, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = setint8eq, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = setint8ne, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = setint8lt, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = setint8gt, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = setint8le, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = setint8ge, LEFTARG = anyset, RIGHTARG = int8, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = setint2eq, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = setint2ne, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = setint2lt, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = setint2gt, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = setint2le, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = setint2ge, LEFTARG = anyset, RIGHTARG = int2, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = setint4eq, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = setint4ne, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = setint4lt, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = setint4gt, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = setint4le, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = setint4ge, LEFTARG = anyset, RIGHTARG = int4, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = int8seteq, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = int8setne, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = int8setlt, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = int8setgt, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = int8setle, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = int8setge, LEFTARG = int8, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = int2seteq, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = int2setne, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = int2setlt, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = int2setgt, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = int2setle, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = int2setge, LEFTARG = int2, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = int4seteq, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES, MERGES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = int4setne, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = int4setlt, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = int4setgt, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = int4setle, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = int4setge, LEFTARG = int4, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = settexteq, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = settextne, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = settextlt, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = settextgt, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = settextle, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = settextge, LEFTARG = anyset, RIGHTARG = text, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.=( 
 PROCEDURE = textseteq, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.=), NEGATOR = OPERATOR(pg_catalog.<>), RESTRICT = eqsel, JOIN = eqjoinsel, HASHES
);

CREATE OPERATOR pg_catalog.<>( 
 PROCEDURE = textsetne, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<>), NEGATOR = OPERATOR(pg_catalog.=), RESTRICT = neqsel, JOIN = neqjoinsel
);

CREATE OPERATOR pg_catalog.<( 
 PROCEDURE = textsetlt, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>), NEGATOR = OPERATOR(pg_catalog.>=), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>( 
 PROCEDURE = textsetgt, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<), NEGATOR = OPERATOR(pg_catalog.<=), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR pg_catalog.<=( 
 PROCEDURE = textsetle, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.>=), NEGATOR = OPERATOR(pg_catalog.>), RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR pg_catalog.>=( 
 PROCEDURE = textsetge, LEFTARG = text, RIGHTARG = anyset, COMMUTATOR = OPERATOR(pg_catalog.<=), NEGATOR = OPERATOR(pg_catalog.<), RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

--------------------------------------------------------------
-- add new pg_cast
--------------------------------------------------------------
DROP CAST IF EXISTS (anyset AS int2) CASCADE;
CREATE CAST (anyset AS int2)
 WITH FUNCTION pg_catalog.int2(anyset)
 AS IMPLICIT;

DROP CAST IF EXISTS (int2 AS anyset) CASCADE;
CREATE CAST (int2 AS anyset)
 WITH FUNCTION pg_catalog.set(int2, int4)
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS int4) CASCADE; 
CREATE CAST (anyset AS int4) 
 WITH FUNCTION pg_catalog.int4(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (int4 AS anyset) CASCADE; 
CREATE CAST (int4 AS anyset) 
 WITH FUNCTION pg_catalog.set(int4, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS int8) CASCADE; 
CREATE CAST (anyset AS int8) 
 WITH FUNCTION pg_catalog.int8(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (int8 AS anyset) CASCADE; 
CREATE CAST (int8 AS anyset) 
 WITH FUNCTION pg_catalog.set(int8, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS float8) CASCADE; 
CREATE CAST (anyset AS float8) 
 WITH FUNCTION pg_catalog.float8(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (float8 AS anyset) CASCADE; 
CREATE CAST (float8 AS anyset) 
 WITH FUNCTION pg_catalog.set(float8, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS float4) CASCADE; 
CREATE CAST (anyset AS float4) 
 WITH FUNCTION pg_catalog.float4(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (float4 AS anyset) CASCADE; 
CREATE CAST (float4 AS anyset) 
 WITH FUNCTION pg_catalog.set(float4, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS numeric) CASCADE; 
CREATE CAST (anyset AS numeric) 
 WITH FUNCTION pg_catalog.settonumber(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (numeric AS anyset) CASCADE; 
CREATE CAST (numeric AS anyset) 
 WITH FUNCTION pg_catalog.set(numeric, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS text) CASCADE; 
CREATE CAST (anyset AS text) 
 WITH FUNCTION pg_catalog.settotext(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (text AS anyset) CASCADE; 
CREATE CAST (text AS anyset) 
 WITH FUNCTION pg_catalog.set(text, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS varchar) CASCADE; 
CREATE CAST (anyset AS varchar) 
 WITH FUNCTION pg_catalog.settovarchar(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (varchar AS anyset) CASCADE; 
CREATE CAST (varchar AS anyset) 
 WITH FUNCTION pg_catalog.set(varchar, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS bpchar) CASCADE; 
CREATE CAST (anyset AS bpchar) 
 WITH FUNCTION pg_catalog.settobpchar(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (bpchar AS anyset) CASCADE; 
CREATE CAST (bpchar AS anyset) 
 WITH FUNCTION pg_catalog.set(bpchar, int4) 
 AS IMPLICIT;

DROP CAST IF EXISTS (anyset AS nvarchar2) CASCADE;
CREATE CAST (anyset AS nvarchar2) 
 WITH FUNCTION pg_catalog.settonvarchar2(anyset) 
 AS IMPLICIT;

DROP CAST IF EXISTS (nvarchar2 AS anyset) CASCADE;
CREATE CAST (nvarchar2 AS anyset) 
 WITH FUNCTION pg_catalog.set(nvarchar2, int4) 
 AS IMPLICIT;

--------------------------------------------------------------------
-- add new pg_opfamily
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_opfamily_temp(
  IN imethod integer,
  IN iname text,
  IN inamespace integer,
  IN iowner integer
)
RETURNS void
AS $$
BEGIN
  insert into pg_catalog.pg_opfamily values (imethod, iname, inamespace, iowner);
  return;
END; $$
LANGUAGE 'plpgsql';

SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 8646;
select Insert_pg_opfamily_temp(405, 'set_ops', 11, 10);

DROP FUNCTION Insert_pg_opfamily_temp();

--------------------------------------------------------------
-- add new pg_opclass
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_opclass_temp(
  IN icmethod integer,
  IN icname text,
  IN icnamespace integer,
  IN icowner integer,
  IN icfamily integer,
  IN icintype integer,
  IN icdefault boolean,
  IN ickeytype integer
)
RETURNS void
AS $$
BEGIN
  insert into pg_catalog.pg_opclass values (icmethod, icname, icnamespace, icowner, icfamily, icintype, icdefault, ickeytype);
  return;
END; $$
LANGUAGE 'plpgsql';

select Insert_pg_opclass_temp(403, 'setasint_ops', 11, 10, 1976, 3272, true, 0);
select Insert_pg_opclass_temp(405, 'setasint_ops', 11, 10, 1977, 3272, false, 0);
select Insert_pg_opclass_temp(405, 'settext_ops', 11, 10, 1995, 3272, false, 0);
select Insert_pg_opclass_temp(4439, 'setasint_ops', 11, 10, 6976, 3272, true, 0);
select Insert_pg_opclass_temp(405, 'set_ops', 11, 10, 8646, 3272, true, 0);
	   
DROP FUNCTION Insert_pg_opclass_temp();

--------------------------------------------------------------
-- add pg_amproc
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_amproc_temp(
  IN iprocfamily    oid,
  IN iproclefttype  oid,
  IN iprocrighttype oid,
  IN iprocnum       smallint,
  IN iproc          regproc
)
RETURNS void
AS $$
BEGIN
  insert into pg_catalog.pg_amproc values (iprocfamily, iproclefttype, iprocrighttype, iprocnum, iproc);
  return;
END; $$
LANGUAGE 'plpgsql';

SELECT Insert_pg_amproc_temp(1976, 3272, 3272, 1, 6544);
SELECT Insert_pg_amproc_temp(1976, 3272, 3272, 2, 6538);
SELECT Insert_pg_amproc_temp(1976, 3272, 23, 1, 6545);
SELECT Insert_pg_amproc_temp(1976, 23, 3272, 1, 6543);
SELECT Insert_pg_amproc_temp(1976, 3272, 20, 1, 6546);
SELECT Insert_pg_amproc_temp(1976, 20, 3272, 1, 6547);
SELECT Insert_pg_amproc_temp(1976, 3272, 21, 1, 6549);
SELECT Insert_pg_amproc_temp(1976, 21, 3272, 1, 6548);
SELECT Insert_pg_amproc_temp(1977, 3272, 3272, 1, 3294);
SELECT Insert_pg_amproc_temp(1995, 3272, 3272, 1, 3297);
SELECT Insert_pg_amproc_temp(8646, 3272, 3272, 1, 3297);
SELECT Insert_pg_amproc_temp(6976, 3272, 3272, 1, 6544);
SELECT Insert_pg_amproc_temp(6976, 3272, 3272, 2, 6538);
SELECT Insert_pg_amproc_temp(6976, 3272, 23, 1, 6545);
SELECT Insert_pg_amproc_temp(6976, 23, 3272, 1, 6543);
SELECT Insert_pg_amproc_temp(6976, 3272, 20, 1, 6546);
SELECT Insert_pg_amproc_temp(6976, 20, 3272, 1, 6547);
SELECT Insert_pg_amproc_temp(6976, 3272, 21, 1, 6549);
SELECT Insert_pg_amproc_temp(6976, 21, 3272, 1, 6548);

DROP FUNCTION Insert_pg_amproc_temp();

--------------------------------------------------------------
-- add pg_amop
--------------------------------------------------------------
CREATE OR REPLACE FUNCTION Insert_pg_amop_temp(
  IN iopfamily     integer,
  IN ioplefttype   integer,
  IN ioprighttype  integer,
  IN iopstrategy   integer,
  IN ioppurpose    character,
  IN iopmethod     integer,
  IN iopsortfamily integer
)
RETURNS void
AS $$
DECLARE
  opopr oid;
BEGIN
  if iopfamily = 1977 or iopfamily = 1995 or iopfamily = 8646 then
    select oid into opopr from pg_catalog.pg_operator where oprleft = ioplefttype and oprright = ioprighttype and oprname = '=';
  else
    select oid into opopr from pg_catalog.pg_operator where oprleft = ioplefttype and oprright = ioprighttype and oprname = (case iopstrategy when 1 then '<' when 2 then '<=' when 3 then '=' when 4 then '>=' when 5 then '>' else '' end);
  end if;
  insert into pg_catalog.pg_amop values (iopfamily, ioplefttype, ioprighttype, iopstrategy, ioppurpose, opopr, iopmethod, iopsortfamily);
  return;
END; $$
LANGUAGE 'plpgsql';

SELECT Insert_pg_amop_temp(1995, 3272, 25, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1995, 25, 3272, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1995, 3272, 3272, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 3272, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 3272, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 3272, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 3272, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 3272, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 23, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 23, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 23, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 23, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 23, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 23, 3272, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 23, 3272, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 23, 3272, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 23, 3272, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 23, 3272, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 20, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 20, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 20, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 20, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 20, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 20, 3272, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 20, 3272, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 20, 3272, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 20, 3272, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 20, 3272, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 21, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 21, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 21, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 21, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 3272, 21, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 21, 3272, 1, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 21, 3272, 2, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 21, 3272, 3, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 21, 3272, 4, 's', 403, 0);
SELECT Insert_pg_amop_temp(1976, 21, 3272, 5, 's', 403, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 3272, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 3272, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 3272, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 3272, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 3272, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 23, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 23, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 23, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 23, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 23, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 23, 3272, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 23, 3272, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 23, 3272, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 23, 3272, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 23, 3272, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 20, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 20, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 20, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 20, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 20, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 20, 3272, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 20, 3272, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 20, 3272, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 20, 3272, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 20, 3272, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 21, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 21, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 21, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 21, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 3272, 21, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 21, 3272, 1, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 21, 3272, 2, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 21, 3272, 3, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 21, 3272, 4, 's', 4439, 0);
SELECT Insert_pg_amop_temp(6976, 21, 3272, 5, 's', 4439, 0);
SELECT Insert_pg_amop_temp(1977, 3272, 23, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1977, 23, 3272, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1977, 3272, 20, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1977, 20, 3272, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1977, 3272, 21, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(1977, 21, 3272, 1, 's', 405, 0);
SELECT Insert_pg_amop_temp(8646, 3272, 3272, 1, 's', 405, 0);

DROP FUNCTION Insert_pg_amop_temp();
