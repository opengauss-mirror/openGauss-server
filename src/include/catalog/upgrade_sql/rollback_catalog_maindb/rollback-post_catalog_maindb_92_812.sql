DO $upgrade$
BEGIN
IF working_version_num() < 92780 then
--------------------------------------------------------------
-- delete pg_amop
--------------------------------------------------------------
delete from pg_catalog.pg_amop where amoplefttype = 3272 or amoprighttype = 3272;


--------------------------------------------------------------
-- delete pg_amproc
--------------------------------------------------------------
delete from pg_catalog.pg_amproc where amproclefttype = 3272 or amprocrighttype = 3272;


--------------------------------------------------------------
-- delete pg_opclass
--------------------------------------------------------------
delete from pg_catalog.pg_opclass where opcintype = 3272;

--------------------------------------------------------------
-- delete pg_opfamily
--------------------------------------------------------------
delete from pg_catalog.pg_opfamily where oid = 8646;

--------------------------------------------------------------
-- delete pg_cast
--------------------------------------------------------------
DECLARE
cnt int;
BEGIN
    select count(*) into cnt from pg_type where oid = 3272;
    if cnt = 1 then
        DROP CAST IF EXISTS (anyset AS int2) CASCADE;
        DROP CAST IF EXISTS (int2 AS anyset) CASCADE;
        DROP CAST IF EXISTS (anyset AS int4) CASCADE; 
        DROP CAST IF EXISTS (int4 AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS int8) CASCADE; 
        DROP CAST IF EXISTS (int8 AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS float8) CASCADE; 
        DROP CAST IF EXISTS (float8 AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS float4) CASCADE; 
        DROP CAST IF EXISTS (float4 AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS numeric) CASCADE; 
        DROP CAST IF EXISTS (numeric AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS text) CASCADE; 
        DROP CAST IF EXISTS (text AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS varchar) CASCADE; 
        DROP CAST IF EXISTS (varchar AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS bpchar) CASCADE; 
        DROP CAST IF EXISTS (bpchar AS anyset) CASCADE; 
        DROP CAST IF EXISTS (anyset AS nvarchar2) CASCADE;
        DROP CAST IF EXISTS (nvarchar2 AS anyset) CASCADE;
    end if;
END;

--------------------------------------------------------------
-- delete pg_operator
--------------------------------------------------------------

DECLARE
cnt int;
BEGIN
    select count(*) into cnt from pg_type where oid = 3272;
    if cnt = 1 then
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
    end if;
END;


--------------------------------------------------------------
-- delete builtin funcs
--------------------------------------------------------------
DECLARE
cnt int;
BEGIN
    select count(*) into cnt from pg_type where oid = 3272;
    if cnt = 1 then
        DROP FUNCTION IF EXISTS pg_catalog.setlt(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2lt(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4lt(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8lt(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settextlt(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2setlt(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4setlt(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8setlt(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textsetlt(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setle(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2le(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4le(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8le(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settextle(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2setle(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4setle(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8setle(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textsetle(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setne(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2ne(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4ne(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8ne(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settextne(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2setne(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4setne(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8setne(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textsetne(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.seteq(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2eq(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4eq(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8eq(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settexteq(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2seteq(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4seteq(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8seteq(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textseteq(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setgt(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2gt(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4gt(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8gt(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settextgt(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2setgt(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4setgt(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8setgt(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textsetgt(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setge(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint2ge(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint4ge(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.setint8ge(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settextge(anyset, text) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2setge(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4setge(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8setge(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btint2setcmp(int2, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btint4setcmp(int4, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btint8setcmp(int8, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btsetcmp(anyset, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btsetint2cmp(anyset, int2) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btsetint4cmp(anyset, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btsetint8cmp(anyset, int8) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.hashsetint(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.hashsettext(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.btsetsortsupport(internal) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.find_in_set(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.float4(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.float8(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int2(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int4(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.int8(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(int2, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(int4, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(int8, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(float8, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(float4, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(numeric, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(bpchar, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(varchar, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(text, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set(nvarchar2, int4) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set_in(cstring, oid) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set_out(varbit) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set_recv(cstring, oid) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.set_send(varbit) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settobpchar(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settonumber(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settonvarchar2(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settotext(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.settovarchar(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.textsetge(text, anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.to_char(anyset) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.anyset_in(cstring) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.anyset_out(anyset) CASCADE;
    end if;
END;
--------------------------------------------------------------
-- delete all set types
--------------------------------------------------------------

DECLARE
  stmt text;
  cursor r is select typname from pg_type where typcategory = 'H';
  tname r%rowtype;
BEGIN
  for tname in r loop
    stmt := 'DROP TYPE IF EXISTS pg_catalog.' || tname.typname || ' CASCADE';
    execute immediate stmt;
  end loop;
END;

DROP TYPE IF EXISTS pg_catalog.anyset CASCADE;

--------------------------------------------------------------
-- delete pg_set
--------------------------------------------------------------
DROP INDEX IF EXISTS pg_catalog.pg_set_typid_order_index;
DROP INDEX IF EXISTS pg_catalog.pg_set_typid_label_index;
DROP INDEX IF EXISTS pg_catalog.pg_set_oid_index;
DROP TYPE IF EXISTS pg_catalog.pg_set;
DROP TABLE IF EXISTS pg_catalog.pg_set;

END IF;
END $upgrade$;
