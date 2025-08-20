--------------------------------------------------------------
-- delete pg_operator
--------------------------------------------------------------
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'simple_integer' limit 1) into ans;
    if ans = true then
        DROP OPERATOR IF EXISTS pg_catalog.+(simple_integer, simple_integer) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.-(simple_integer, simple_integer) CASCADE;
        DROP OPERATOR IF EXISTS pg_catalog.*(simple_integer, simple_integer) CASCADE;
    end if;
END$$;

--------------------------------------------------------------
-- delete pg_cast
--------------------------------------------------------------
DO $$
DECLARE
ans_simple_integer boolean;
ans_signtype boolean;
ans_positiven boolean;
ans_positive boolean;
ans_naturaln boolean;
ans_natural boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'simple_integer' limit 1) into ans_simple_integer;
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'signtype' limit 1) into ans_signtype;
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'positiven' limit 1) into ans_positiven;
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'positive' limit 1) into ans_positive;
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'naturaln' limit 1) into ans_naturaln;
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'natural' limit 1) into ans_natural;
    if ans_simple_integer = true then
        DROP CAST IF EXISTS (tinyint AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (smallint AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (integer AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (bigint AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (int16 AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (boolean AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (real AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (double precision AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (numeric AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (bit AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (text AS simple_integer) CASCADE;
        DROP CAST IF EXISTS ("char" AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (character varying AS simple_integer) CASCADE;
        DROP CAST IF EXISTS (bpchar AS simple_integer) CASCADE;

        DROP CAST IF EXISTS (simple_integer AS tinyint) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS smallint) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS integer) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS bigint) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS int16) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS boolean) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS real) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS double precision) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS numeric) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS bit) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS text) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS "char") CASCADE;
        DROP CAST IF EXISTS (simple_integer AS character varying) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS bpchar) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS clob) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS money) CASCADE;
        DROP CAST IF EXISTS (simple_integer AS interval) CASCADE;
    end if;

    if ans_signtype = true then
        DROP CAST IF EXISTS (tinyint AS signtype) CASCADE;
        DROP CAST IF EXISTS (smallint AS signtype) CASCADE;
        DROP CAST IF EXISTS (integer AS signtype) CASCADE;
        DROP CAST IF EXISTS (bigint AS signtype) CASCADE;
        DROP CAST IF EXISTS (int16 AS signtype) CASCADE;
        DROP CAST IF EXISTS (boolean AS signtype) CASCADE;
        DROP CAST IF EXISTS (real AS signtype) CASCADE;
        DROP CAST IF EXISTS (double precision AS signtype) CASCADE;
        DROP CAST IF EXISTS (numeric AS signtype) CASCADE;
        DROP CAST IF EXISTS (bit AS signtype) CASCADE;
        DROP CAST IF EXISTS (text AS signtype) CASCADE;
        DROP CAST IF EXISTS ("char" AS signtype) CASCADE;
        DROP CAST IF EXISTS (character varying AS signtype) CASCADE;
        DROP CAST IF EXISTS (bpchar AS signtype) CASCADE;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (simple_integer AS signtype) CASCADE;
        end if;

        DROP CAST IF EXISTS (signtype AS tinyint) CASCADE;
        DROP CAST IF EXISTS (signtype AS smallint) CASCADE;
        DROP CAST IF EXISTS (signtype AS integer) CASCADE;
        DROP CAST IF EXISTS (signtype AS bigint) CASCADE;
        DROP CAST IF EXISTS (signtype AS int16) CASCADE;
        DROP CAST IF EXISTS (signtype AS boolean) CASCADE;
        DROP CAST IF EXISTS (signtype AS real) CASCADE;
        DROP CAST IF EXISTS (signtype AS double precision) CASCADE;
        DROP CAST IF EXISTS (signtype AS numeric) CASCADE;
        DROP CAST IF EXISTS (signtype AS bit) CASCADE;
        DROP CAST IF EXISTS (signtype AS text) CASCADE;
        DROP CAST IF EXISTS (signtype AS "char") CASCADE;
        DROP CAST IF EXISTS (signtype AS character varying) CASCADE;
        DROP CAST IF EXISTS (signtype AS bpchar) CASCADE;
        DROP CAST IF EXISTS (signtype AS clob) CASCADE;
        DROP CAST IF EXISTS (signtype AS money) CASCADE;
        DROP CAST IF EXISTS (signtype AS interval) CASCADE;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (signtype AS simple_integer) CASCADE;
        end if;
    end if;

    if ans_positiven = true then
        DROP CAST IF EXISTS (tinyint AS positiven) CASCADE;
        DROP CAST IF EXISTS (smallint AS positiven) CASCADE;
        DROP CAST IF EXISTS (integer AS positiven) CASCADE;
        DROP CAST IF EXISTS (bigint AS positiven) CASCADE;
        DROP CAST IF EXISTS (int16 AS positiven) CASCADE;
        DROP CAST IF EXISTS (boolean AS positiven) CASCADE;
        DROP CAST IF EXISTS (real AS positiven) CASCADE;
        DROP CAST IF EXISTS (double precision AS positiven) CASCADE;
        DROP CAST IF EXISTS (numeric AS positiven) CASCADE;
        DROP CAST IF EXISTS (bit AS positiven) CASCADE;
        DROP CAST IF EXISTS (text AS positiven) CASCADE;
        DROP CAST IF EXISTS ("char" AS positiven) CASCADE;
        DROP CAST IF EXISTS (character varying AS positiven) CASCADE;
        DROP CAST IF EXISTS (bpchar AS positiven) CASCADE;
        if ans_signtype = true then
            DROP CAST IF EXISTS (signtype AS positiven) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (simple_integer AS positiven) CASCADE;
        end if;


        DROP CAST IF EXISTS (positiven AS tinyint) CASCADE;
        DROP CAST IF EXISTS (positiven AS smallint) CASCADE;
        DROP CAST IF EXISTS (positiven AS integer) CASCADE;
        DROP CAST IF EXISTS (positiven AS bigint) CASCADE;
        DROP CAST IF EXISTS (positiven AS int16) CASCADE;
        DROP CAST IF EXISTS (positiven AS boolean) CASCADE;
        DROP CAST IF EXISTS (positiven AS real) CASCADE;
        DROP CAST IF EXISTS (positiven AS double precision) CASCADE;
        DROP CAST IF EXISTS (positiven AS numeric) CASCADE;
        DROP CAST IF EXISTS (positiven AS bit) CASCADE;
        DROP CAST IF EXISTS (positiven AS text) CASCADE;
        DROP CAST IF EXISTS (positiven AS "char") CASCADE;
        DROP CAST IF EXISTS (positiven AS character varying) CASCADE;
        DROP CAST IF EXISTS (positiven AS bpchar) CASCADE;
        DROP CAST IF EXISTS (positiven AS clob) CASCADE;
        DROP CAST IF EXISTS (positiven AS money) CASCADE;
        DROP CAST IF EXISTS (positiven AS interval) CASCADE;
        if ans_signtype = true then
            DROP CAST IF EXISTS (positiven AS signtype) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (positiven AS simple_integer) CASCADE;
        end if;
    end if;

    if ans_positive = true then
        DROP CAST IF EXISTS (tinyint AS positive) CASCADE;
        DROP CAST IF EXISTS (smallint AS positive) CASCADE;
        DROP CAST IF EXISTS (integer AS positive) CASCADE;
        DROP CAST IF EXISTS (bigint AS positive) CASCADE;
        DROP CAST IF EXISTS (int16 AS positive) CASCADE;
        DROP CAST IF EXISTS (boolean AS positive) CASCADE;
        DROP CAST IF EXISTS (real AS positive) CASCADE;
        DROP CAST IF EXISTS (double precision AS positive) CASCADE;
        DROP CAST IF EXISTS (numeric AS positive) CASCADE;
        DROP CAST IF EXISTS (bit AS positive) CASCADE;
        DROP CAST IF EXISTS (text AS positive) CASCADE;
        DROP CAST IF EXISTS ("char" AS positive) CASCADE;
        DROP CAST IF EXISTS (character varying AS positive) CASCADE;
        DROP CAST IF EXISTS (bpchar AS positive) CASCADE;
        if ans_positiven = true then
            DROP CAST IF EXISTS (positiven AS positive) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (signtype AS positive) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (simple_integer AS positive) CASCADE;
        end if;

        DROP CAST IF EXISTS (positive AS tinyint) CASCADE;
        DROP CAST IF EXISTS (positive AS smallint) CASCADE;
        DROP CAST IF EXISTS (positive AS integer) CASCADE;
        DROP CAST IF EXISTS (positive AS bigint) CASCADE;
        DROP CAST IF EXISTS (positive AS int16) CASCADE;
        DROP CAST IF EXISTS (positive AS boolean) CASCADE;
        DROP CAST IF EXISTS (positive AS real) CASCADE;
        DROP CAST IF EXISTS (positive AS double precision) CASCADE;
        DROP CAST IF EXISTS (positive AS numeric) CASCADE;
        DROP CAST IF EXISTS (positive AS bit) CASCADE;
        DROP CAST IF EXISTS (positive AS text) CASCADE;
        DROP CAST IF EXISTS (positive AS "char") CASCADE;
        DROP CAST IF EXISTS (positive AS character varying) CASCADE;
        DROP CAST IF EXISTS (positive AS bpchar) CASCADE;
        DROP CAST IF EXISTS (positive AS clob) CASCADE;
        DROP CAST IF EXISTS (positive AS money) CASCADE;
        DROP CAST IF EXISTS (positive AS interval) CASCADE;
        if ans_positiven = true then
            DROP CAST IF EXISTS (positive AS positiven) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (positive AS signtype) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (positive AS simple_integer) CASCADE;
        end if;
    end if;

    if ans_naturaln = true then
        DROP CAST IF EXISTS (tinyint AS naturaln) CASCADE;
        DROP CAST IF EXISTS (smallint AS naturaln) CASCADE;
        DROP CAST IF EXISTS (integer AS naturaln) CASCADE;
        DROP CAST IF EXISTS (bigint AS naturaln) CASCADE;
        DROP CAST IF EXISTS (int16 AS naturaln) CASCADE;
        DROP CAST IF EXISTS (boolean AS naturaln) CASCADE;
        DROP CAST IF EXISTS (real AS naturaln) CASCADE;
        DROP CAST IF EXISTS (double precision AS naturaln) CASCADE;
        DROP CAST IF EXISTS (numeric AS naturaln) CASCADE;
        DROP CAST IF EXISTS (bit AS naturaln) CASCADE;
        DROP CAST IF EXISTS (text AS naturaln) CASCADE;
        DROP CAST IF EXISTS ("char" AS naturaln) CASCADE;
        DROP CAST IF EXISTS (character varying AS naturaln) CASCADE;
        DROP CAST IF EXISTS (bpchar AS naturaln) CASCADE;
        if ans_positive = true then
            DROP CAST IF EXISTS (positive AS naturaln) CASCADE;
        end if;
        if ans_positiven = true then
            DROP CAST IF EXISTS (positiven AS naturaln) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (signtype AS naturaln) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (simple_integer AS naturaln) CASCADE;
        end if;

        DROP CAST IF EXISTS (naturaln AS tinyint) CASCADE;
        DROP CAST IF EXISTS (naturaln AS smallint) CASCADE;
        DROP CAST IF EXISTS (naturaln AS integer) CASCADE;
        DROP CAST IF EXISTS (naturaln AS bigint) CASCADE;
        DROP CAST IF EXISTS (naturaln AS int16) CASCADE;
        DROP CAST IF EXISTS (naturaln AS boolean) CASCADE;
        DROP CAST IF EXISTS (naturaln AS real) CASCADE;
        DROP CAST IF EXISTS (naturaln AS double precision) CASCADE;
        DROP CAST IF EXISTS (naturaln AS numeric) CASCADE;
        DROP CAST IF EXISTS (naturaln AS bit) CASCADE;
        DROP CAST IF EXISTS (naturaln AS text) CASCADE;
        DROP CAST IF EXISTS (naturaln AS "char") CASCADE;
        DROP CAST IF EXISTS (naturaln AS character varying) CASCADE;
        DROP CAST IF EXISTS (naturaln AS bpchar) CASCADE;
        DROP CAST IF EXISTS (naturaln AS clob) CASCADE;
        DROP CAST IF EXISTS (naturaln AS money) CASCADE;
        DROP CAST IF EXISTS (naturaln AS interval) CASCADE;
        if ans_positive = true then
            DROP CAST IF EXISTS (naturaln AS positive) CASCADE;
        end if;
        if ans_positiven = true then
            DROP CAST IF EXISTS (naturaln AS positiven) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (naturaln AS signtype) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (naturaln AS simple_integer) CASCADE;
        end if;
    end if;

    if ans_natural = true then
        DROP CAST IF EXISTS (tinyint AS natural) CASCADE;
        DROP CAST IF EXISTS (smallint AS natural) CASCADE;
        DROP CAST IF EXISTS (integer AS natural) CASCADE;
        DROP CAST IF EXISTS (bigint AS natural) CASCADE;
        DROP CAST IF EXISTS (int16 AS natural) CASCADE;
        DROP CAST IF EXISTS (boolean AS natural) CASCADE;
        DROP CAST IF EXISTS (real AS natural) CASCADE;
        DROP CAST IF EXISTS (double precision AS natural) CASCADE;
        DROP CAST IF EXISTS (numeric AS natural) CASCADE;
        DROP CAST IF EXISTS (bit AS natural) CASCADE;
        DROP CAST IF EXISTS (text AS natural) CASCADE;
        DROP CAST IF EXISTS ("char" AS natural) CASCADE;
        DROP CAST IF EXISTS (character varying AS natural) CASCADE;
        DROP CAST IF EXISTS (bpchar AS natural) CASCADE;
        if ans_naturaln = true then
            DROP CAST IF EXISTS (naturaln AS natural) CASCADE;
        end if;
        if ans_positive = true then
            DROP CAST IF EXISTS (positive AS natural) CASCADE;
        end if;
        if ans_positiven = true then
            DROP CAST IF EXISTS (positiven AS natural) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (signtype AS natural) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (simple_integer AS natural) CASCADE;
        end if;

        DROP CAST IF EXISTS (natural AS tinyint) CASCADE;
        DROP CAST IF EXISTS (natural AS smallint) CASCADE;
        DROP CAST IF EXISTS (natural AS integer) CASCADE;
        DROP CAST IF EXISTS (natural AS bigint) CASCADE;
        DROP CAST IF EXISTS (natural AS int16) CASCADE;
        DROP CAST IF EXISTS (natural AS boolean) CASCADE;
        DROP CAST IF EXISTS (natural AS real) CASCADE;
        DROP CAST IF EXISTS (natural AS double precision) CASCADE;
        DROP CAST IF EXISTS (natural AS numeric) CASCADE;
        DROP CAST IF EXISTS (natural AS bit) CASCADE;
        DROP CAST IF EXISTS (natural AS text) CASCADE;
        DROP CAST IF EXISTS (natural AS "char") CASCADE;
        DROP CAST IF EXISTS (natural AS character varying) CASCADE;
        DROP CAST IF EXISTS (natural AS bpchar) CASCADE;
        DROP CAST IF EXISTS (natural AS clob) CASCADE;
        DROP CAST IF EXISTS (natural AS money) CASCADE;
        DROP CAST IF EXISTS (natural AS interval) CASCADE;
        if ans_naturaln = true then
            DROP CAST IF EXISTS (natural AS naturaln) CASCADE;
        end if;
        if ans_positive = true then
            DROP CAST IF EXISTS (natural AS positive) CASCADE;
        end if;
        if ans_positiven = true then
            DROP CAST IF EXISTS (natural AS positiven) CASCADE;
        end if;
        if ans_signtype = true then
            DROP CAST IF EXISTS (natural AS signtype) CASCADE;
        end if;
        if ans_simple_integer = true then
            DROP CAST IF EXISTS (natural AS simple_integer) CASCADE;
        end if;
    end if;
END$$;

--------------------------------------------------------------
-- delete builtin funcs
--------------------------------------------------------------
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'simple_integer' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.simple_integer_sub(simple_integer, simple_integer);
        DROP FUNCTION IF EXISTS pg_catalog.simple_integer_plus(simple_integer, simple_integer);
        DROP FUNCTION IF EXISTS pg_catalog.simple_integer_mul(simple_integer,simple_integer);
    end if;
END$$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.simple_integer(bpchar) CASCADE;

DROP FUNCTION if EXISTS pg_catalog.signtype(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.signtype(bpchar) CASCADE;

DROP FUNCTION if EXISTS pg_catalog.positiven(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positiven(bpchar) CASCADE;

DROP FUNCTION if EXISTS pg_catalog.positive(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.positive(bpchar) CASCADE;

DROP FUNCTION if EXISTS pg_catalog.naturaln(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.naturaln(bpchar) CASCADE;

DROP FUNCTION if EXISTS pg_catalog.natural(smallint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(integer) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(boolean) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(tinyint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(bigint) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(int16) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(real) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(double precision) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(numeric) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(bit) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(text) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural("char") CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(character varying) CASCADE;
DROP FUNCTION if EXISTS pg_catalog.natural(bpchar) CASCADE;

--------------------------------------------------------------
-- delete type
--------------------------------------------------------------
DROP FUNCTION IF EXISTS pg_catalog.simple_integer_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.simple_integer_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'simple_integer' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.simple_integer_out(simple_integer) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.simple_integer_send(simple_integer) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._simple_integer;
DROP TYPE IF EXISTS pg_catalog.simple_integer;

DROP FUNCTION IF EXISTS pg_catalog.signtype_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.signtype_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'signtype' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.signtype_out(signtype) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.signtype_send(signtype) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._signtype;
DROP TYPE IF EXISTS pg_catalog.signtype;

DROP FUNCTION IF EXISTS pg_catalog.positiven_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.positiven_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'positiven' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.positiven_out(positiven) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.positiven_send(positiven) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._positiven;
DROP TYPE IF EXISTS pg_catalog.positiven;

DROP FUNCTION IF EXISTS pg_catalog.positive_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.positive_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'positive' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.positive_out(positive) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.positive_send(positive) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._positive;
DROP TYPE IF EXISTS pg_catalog.positive;

DROP FUNCTION IF EXISTS pg_catalog.naturaln_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.naturaln_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'naturaln' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.naturaln_out(naturaln) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.naturaln_send(naturaln) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._naturaln;
DROP TYPE IF EXISTS pg_catalog.naturaln;

DROP FUNCTION IF EXISTS pg_catalog.natural_in(cstring) CASCADE;
DROP FUNCTION IF EXISTS pg_catalog.natural_recv(internal) CASCADE;
DO $$
DECLARE
ans boolean;
BEGIN
    select case when count(*)=1 then true else false end as ans from (select * from pg_type where typname = 'natural' limit 1) into ans;
    if ans = true then
        DROP FUNCTION IF EXISTS pg_catalog.natural_out(natural) CASCADE;
        DROP FUNCTION IF EXISTS pg_catalog.natural_send(natural) CASCADE;
    end if;
END$$;
DROP TYPE IF EXISTS pg_catalog._natural;
DROP TYPE IF EXISTS pg_catalog.natural;