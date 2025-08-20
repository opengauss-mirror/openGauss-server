--------------------------------------------------------------
-- add new type
--------------------------------------------------------------
-- natural
DROP TYPE IF EXISTS pg_catalog._natural;
DROP TYPE IF EXISTS pg_catalog.natural;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5850, 5851, b;
CREATE TYPE pg_catalog.natural;

DROP FUNCTION IF EXISTS pg_catalog.natural_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5847;
CREATE FUNCTION pg_catalog.natural_in (
cstring
) RETURNS natural LANGUAGE INTERNAL IMMUTABLE STRICT as 'natural_in';

DROP FUNCTION IF EXISTS pg_catalog.natural_out(natural) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5848;
CREATE FUNCTION pg_catalog.natural_out (
natural
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'natural_out';

DROP FUNCTION IF EXISTS pg_catalog.natural_send(natural) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5850;
CREATE FUNCTION pg_catalog.natural_send (
natural
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'natural_send';

DROP FUNCTION IF EXISTS pg_catalog.natural_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5849;
CREATE FUNCTION pg_catalog.natural_recv (
internal
) RETURNS natural LANGUAGE INTERNAL IMMUTABLE STRICT as 'natural_recv';

CREATE TYPE pg_catalog.natural (
    INPUT=natural_in,
    OUTPUT=natural_out,
    RECEIVE=natural_recv,
    SEND=natural_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');

-- naturaln
DROP TYPE IF EXISTS pg_catalog._naturaln;
DROP TYPE IF EXISTS pg_catalog.naturaln;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5852, 5853, b;
CREATE TYPE pg_catalog.naturaln;

DROP FUNCTION IF EXISTS pg_catalog.naturaln_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5866;
CREATE FUNCTION pg_catalog.naturaln_in (
cstring
) RETURNS naturaln LANGUAGE INTERNAL IMMUTABLE as 'naturaln_in';

DROP FUNCTION IF EXISTS pg_catalog.naturaln_out(naturaln) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5867;
CREATE FUNCTION pg_catalog.naturaln_out (
naturaln
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'naturaln_out';

DROP FUNCTION IF EXISTS pg_catalog.naturaln_send(naturaln) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5869;
CREATE FUNCTION pg_catalog.naturaln_send (
naturaln
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'naturaln_send';

DROP FUNCTION IF EXISTS pg_catalog.naturaln_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5868;
CREATE FUNCTION pg_catalog.naturaln_recv (
internal
) RETURNS naturaln LANGUAGE INTERNAL IMMUTABLE as 'naturaln_recv';

CREATE TYPE pg_catalog.naturaln (
    INPUT=naturaln_in,
    OUTPUT=naturaln_out,
    RECEIVE=naturaln_recv,
    SEND=naturaln_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');

-- positive
DROP TYPE IF EXISTS pg_catalog._positive;
DROP TYPE IF EXISTS pg_catalog.positive;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5854, 5855, b;
CREATE TYPE pg_catalog.positive;

DROP FUNCTION IF EXISTS pg_catalog.positive_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5885;
CREATE FUNCTION pg_catalog.positive_in (
cstring
) RETURNS positive LANGUAGE INTERNAL IMMUTABLE STRICT as 'positive_in';

DROP FUNCTION IF EXISTS pg_catalog.positive_out(positive) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5886;
CREATE FUNCTION pg_catalog.positive_out (
positive
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'positive_out';

DROP FUNCTION IF EXISTS pg_catalog.positive_send(positive) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5888;
CREATE FUNCTION pg_catalog.positive_send (
positive
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'positive_send';

DROP FUNCTION IF EXISTS pg_catalog.positive_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5887;
CREATE FUNCTION pg_catalog.positive_recv (
internal
) RETURNS positive LANGUAGE INTERNAL IMMUTABLE STRICT as 'positive_recv';

CREATE TYPE pg_catalog.positive (
    INPUT=positive_in,
    OUTPUT=positive_out,
    RECEIVE=positive_recv,
    SEND=positive_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');

-- positiven
DROP TYPE IF EXISTS pg_catalog._positiven;
DROP TYPE IF EXISTS pg_catalog.positiven;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5856, 5857, b;
CREATE TYPE pg_catalog.positiven;

DROP FUNCTION IF EXISTS pg_catalog.positiven_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5904;
CREATE FUNCTION pg_catalog.positiven_in (
cstring
) RETURNS positiven LANGUAGE INTERNAL IMMUTABLE as 'positiven_in';

DROP FUNCTION IF EXISTS pg_catalog.positiven_out(positiven) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5905;
CREATE FUNCTION pg_catalog.positiven_out (
positiven
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'positiven_out';

DROP FUNCTION IF EXISTS pg_catalog.positiven_send(positiven) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5907;
CREATE FUNCTION pg_catalog.positiven_send (
positiven
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'positiven_send';

DROP FUNCTION IF EXISTS pg_catalog.positiven_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5906;
CREATE FUNCTION pg_catalog.positiven_recv (
internal
) RETURNS positiven LANGUAGE INTERNAL IMMUTABLE as 'positiven_recv';

CREATE TYPE pg_catalog.positiven (
    INPUT=positiven_in,
    OUTPUT=positiven_out,
    RECEIVE=positiven_recv,
    SEND=positiven_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');

-- signtype
DROP TYPE IF EXISTS pg_catalog._signtype;
DROP TYPE IF EXISTS pg_catalog.signtype;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5858, 5859, b;
CREATE TYPE pg_catalog.signtype;

DROP FUNCTION IF EXISTS pg_catalog.signtype_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5923;
CREATE FUNCTION pg_catalog.signtype_in (
cstring
) RETURNS signtype LANGUAGE INTERNAL IMMUTABLE STRICT as 'signtype_in';

DROP FUNCTION IF EXISTS pg_catalog.signtype_out(signtype) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5924;
CREATE FUNCTION pg_catalog.signtype_out (
signtype
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'signtype_out';

DROP FUNCTION IF EXISTS pg_catalog.signtype_send(signtype) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5926;
CREATE FUNCTION pg_catalog.signtype_send (
signtype
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'signtype_send';

DROP FUNCTION IF EXISTS pg_catalog.signtype_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5925;
CREATE FUNCTION pg_catalog.signtype_recv (
internal
) RETURNS signtype LANGUAGE INTERNAL IMMUTABLE STRICT as 'signtype_recv';

CREATE TYPE pg_catalog.signtype (
    INPUT=signtype_in,
    OUTPUT=signtype_out,
    RECEIVE=signtype_recv,
    SEND=signtype_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');

-- simple_integer
DROP TYPE IF EXISTS pg_catalog._simple_integer;
DROP TYPE IF EXISTS pg_catalog.simple_integer;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_TYPE, 5860, 5861, b;
CREATE TYPE pg_catalog.simple_integer;

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_in(cstring) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5959;
CREATE FUNCTION pg_catalog.simple_integer_in (
cstring
) RETURNS simple_integer LANGUAGE INTERNAL IMMUTABLE as 'simple_integer_in';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_out(simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5960;
CREATE FUNCTION pg_catalog.simple_integer_out (
simple_integer
) RETURNS cstring LANGUAGE INTERNAL IMMUTABLE STRICT as 'simple_integer_out';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_send(simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5943;
CREATE FUNCTION pg_catalog.simple_integer_send (
simple_integer
) RETURNS bytea LANGUAGE INTERNAL IMMUTABLE STRICT as 'simple_integer_send';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_recv(internal) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 5942;
CREATE FUNCTION pg_catalog.simple_integer_recv (
internal
) RETURNS simple_integer LANGUAGE INTERNAL IMMUTABLE as 'simple_integer_recv';

CREATE TYPE pg_catalog.simple_integer (
    INPUT=simple_integer_in,
    OUTPUT=simple_integer_out,
    RECEIVE=simple_integer_recv,
    SEND=simple_integer_send,
    INTERNALLENGTH=4,
    PASSEDBYVALUE=true,
    STORAGE=PLAIN,
    CATEGORY='N');
--------------------------------------------------------------
-- add new functions
--------------------------------------------------------------
-- natural casts
DROP FUNCTION if EXISTS pg_catalog.natural(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5851;
CREATE OR REPLACE FUNCTION pg_catalog.natural(smallint)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i2_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5852;
CREATE OR REPLACE FUNCTION pg_catalog.natural(integer)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i4_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5853;
CREATE OR REPLACE FUNCTION pg_catalog.natural(boolean)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bool_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5854;
CREATE OR REPLACE FUNCTION pg_catalog.natural(tinyint)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i1_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5855;
CREATE OR REPLACE FUNCTION pg_catalog.natural(bigint)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i8_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5856;
CREATE OR REPLACE FUNCTION pg_catalog.natural(int16)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5857;
CREATE OR REPLACE FUNCTION pg_catalog.natural(real)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$float_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5858;
CREATE OR REPLACE FUNCTION pg_catalog.natural(double precision)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$double_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5859;
CREATE OR REPLACE FUNCTION pg_catalog.natural(numeric)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5860;
CREATE OR REPLACE FUNCTION pg_catalog.natural(bit)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bit_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5861;
CREATE OR REPLACE FUNCTION pg_catalog.natural(text)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$text_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5862;
CREATE OR REPLACE FUNCTION pg_catalog.natural("char")
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$char_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5863;
CREATE OR REPLACE FUNCTION pg_catalog.natural(character varying)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_natural$function$;

DROP FUNCTION if EXISTS pg_catalog.natural(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5864;
CREATE OR REPLACE FUNCTION pg_catalog.natural(bpchar)
 RETURNS natural
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_natural$function$;

comment on function pg_catalog.natural(smallint) is 'convert int2 to natural';
comment on function pg_catalog.natural(integer) is 'convert int4 to natural';
comment on function pg_catalog.natural(boolean) is 'convert bool to natural';
comment on function pg_catalog.natural(tinyint) is 'convert int1 to natural';
comment on function pg_catalog.natural(bigint) is 'convert int8 to natural';
comment on function pg_catalog.natural(int16) is 'convert int16 to natural';
comment on function pg_catalog.natural(real) is 'convert float to natural';
comment on function pg_catalog.natural(double precision) is 'convert double to natural';
comment on function pg_catalog.natural(numeric) is 'convert numeric to natural';
comment on function pg_catalog.natural(bit) is 'convert bit to natural';
comment on function pg_catalog.natural(text) is 'convert text to natural';
comment on function pg_catalog.natural("char") is 'convert char to natural';
comment on function pg_catalog.natural(character varying) is 'convert varchar to natural';
comment on function pg_catalog.natural(bpchar) is 'convert bpchar to natural';

comment on function pg_catalog.natural_in(cstring) is 'I/O';
comment on function pg_catalog.natural_out(natural) is 'I/O';
comment on function pg_catalog.natural_send(natural) is 'I/O';
comment on function pg_catalog.natural_recv(internal) is 'I/O';

-- naturaln casts
DROP FUNCTION if EXISTS pg_catalog.naturaln(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5870;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(smallint)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i2_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5871;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(integer)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i4_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5872;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(boolean)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bool_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5873;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(tinyint)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i1_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5874;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(bigint)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i8_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5875;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(int16)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i16_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5876;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(real)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$float_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5877;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(double precision)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$double_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5878;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(numeric)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5879;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(bit)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bit_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5880;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(text)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$text_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5881;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln("char")
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$char_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5882;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(character varying)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_naturaln$function$;

DROP FUNCTION if EXISTS pg_catalog.naturaln(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5883;
CREATE OR REPLACE FUNCTION pg_catalog.naturaln(bpchar)
 RETURNS naturaln
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_naturaln$function$;

comment on function pg_catalog.naturaln(smallint) is 'convert int2 to naturaln';
comment on function pg_catalog.naturaln(integer) is 'convert int4 to naturaln';
comment on function pg_catalog.naturaln(boolean) is 'convert bool to naturaln';
comment on function pg_catalog.naturaln(tinyint) is 'convert int1 to naturaln';
comment on function pg_catalog.naturaln(bigint) is 'convert int8 to naturaln';
comment on function pg_catalog.naturaln(int16) is 'convert int16 to naturaln';
comment on function pg_catalog.naturaln(real) is 'convert float to naturaln';
comment on function pg_catalog.naturaln(double precision) is 'convert double to naturaln';
comment on function pg_catalog.naturaln(numeric) is 'convert numeric to naturaln';
comment on function pg_catalog.naturaln(bit) is 'convert bit to naturaln';
comment on function pg_catalog.naturaln(text) is 'convert text to naturaln';
comment on function pg_catalog.naturaln("char") is 'convert char to naturaln';
comment on function pg_catalog.naturaln(character varying) is 'convert varchar to naturaln';
comment on function pg_catalog.naturaln(bpchar) is 'convert bpchar to naturaln';

comment on function pg_catalog.naturaln_in(cstring) is 'I/O';
comment on function pg_catalog.naturaln_out(naturaln) is 'I/O';
comment on function pg_catalog.naturaln_send(naturaln) is 'I/O';
comment on function pg_catalog.naturaln_recv(internal) is 'I/O';

-- positive casts
DROP FUNCTION if EXISTS pg_catalog.positive(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5889;
CREATE OR REPLACE FUNCTION pg_catalog.positive(smallint)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i2_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5890;
CREATE OR REPLACE FUNCTION pg_catalog.positive(integer)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i4_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5891;
CREATE OR REPLACE FUNCTION pg_catalog.positive(boolean)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bool_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5892;
CREATE OR REPLACE FUNCTION pg_catalog.positive(tinyint)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i1_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5893;
CREATE OR REPLACE FUNCTION pg_catalog.positive(bigint)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i8_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5894;
CREATE OR REPLACE FUNCTION pg_catalog.positive(int16)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5895;
CREATE OR REPLACE FUNCTION pg_catalog.positive(real)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$float_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5896;
CREATE OR REPLACE FUNCTION pg_catalog.positive(double precision)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$double_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5897;
CREATE OR REPLACE FUNCTION pg_catalog.positive(numeric)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5898;
CREATE OR REPLACE FUNCTION pg_catalog.positive(bit)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bit_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5899;
CREATE OR REPLACE FUNCTION pg_catalog.positive(text)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$text_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5900;
CREATE OR REPLACE FUNCTION pg_catalog.positive("char")
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$char_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5901;
CREATE OR REPLACE FUNCTION pg_catalog.positive(character varying)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_positive$function$;

DROP FUNCTION if EXISTS pg_catalog.positive(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5902;
CREATE OR REPLACE FUNCTION pg_catalog.positive(bpchar)
 RETURNS positive
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_positive$function$;

comment on function pg_catalog.positive(smallint) is 'convert int2 to positive';
comment on function pg_catalog.positive(integer) is 'convert int4 to positive';
comment on function pg_catalog.positive(boolean) is 'convert bool to positive';
comment on function pg_catalog.positive(tinyint) is 'convert int1 to positive';
comment on function pg_catalog.positive(bigint) is 'convert int8 to positive';
comment on function pg_catalog.positive(int16) is 'convert int16 to positive';
comment on function pg_catalog.positive(real) is 'convert float to positive';
comment on function pg_catalog.positive(double precision) is 'convert double to positive';
comment on function pg_catalog.positive(numeric) is 'convert numeric to positive';
comment on function pg_catalog.positive(bit) is 'convert bit to positive';
comment on function pg_catalog.positive(text) is 'convert text to positive';
comment on function pg_catalog.positive("char") is 'convert char to positive';
comment on function pg_catalog.positive(character varying) is 'convert varchar to positive';
comment on function pg_catalog.positive(bpchar) is 'convert bpchar to positive';

comment on function pg_catalog.positive_in(cstring) is 'I/O';
comment on function pg_catalog.positive_out(positive) is 'I/O';
comment on function pg_catalog.positive_send(positive) is 'I/O';
comment on function pg_catalog.positive_recv(internal) is 'I/O';

-- positiven casts
DROP FUNCTION if EXISTS pg_catalog.positiven(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5908;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(smallint)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i2_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5909;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(integer)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i4_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5910;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(boolean)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bool_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5911;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(tinyint)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i1_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5912;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(bigint)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i8_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5913;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(int16)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i16_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5914;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(real)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$float_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5915;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(double precision)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$double_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5916;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(numeric)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5917;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(bit)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bit_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5918;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(text)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$text_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5919;
CREATE OR REPLACE FUNCTION pg_catalog.positiven("char")
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$char_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5920;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(character varying)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_positiven$function$;

DROP FUNCTION if EXISTS pg_catalog.positiven(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5921;
CREATE OR REPLACE FUNCTION pg_catalog.positiven(bpchar)
 RETURNS positiven
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_positiven$function$;

comment on function pg_catalog.positiven(smallint) is 'convert int2 to positiven';
comment on function pg_catalog.positiven(integer) is 'convert int4 to positiven';
comment on function pg_catalog.positiven(boolean) is 'convert bool to positiven';
comment on function pg_catalog.positiven(tinyint) is 'convert int1 to positiven';
comment on function pg_catalog.positiven(bigint) is 'convert int8 to positiven';
comment on function pg_catalog.positiven(int16) is 'convert int16 to positiven';
comment on function pg_catalog.positiven(real) is 'convert float to positiven';
comment on function pg_catalog.positiven(double precision) is 'convert double to positiven';
comment on function pg_catalog.positiven(numeric) is 'convert numeric to positiven';
comment on function pg_catalog.positiven(bit) is 'convert bit to positiven';
comment on function pg_catalog.positiven(text) is 'convert text to positiven';
comment on function pg_catalog.positiven("char") is 'convert char to positiven';
comment on function pg_catalog.positiven(character varying) is 'convert varchar to positiven';
comment on function pg_catalog.positiven(bpchar) is 'convert bpchar to positiven';

comment on function pg_catalog.positiven_in(cstring) is 'I/O';
comment on function pg_catalog.positiven_out(positiven) is 'I/O';
comment on function pg_catalog.positiven_send(positiven) is 'I/O';
comment on function pg_catalog.positiven_recv(internal) is 'I/O';

-- signtype casts
DROP FUNCTION if EXISTS pg_catalog.signtype(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5927;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(smallint)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i2_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5928;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(integer)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i4_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5929;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(boolean)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bool_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5930;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(tinyint)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i1_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5931;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(bigint)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i8_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5932;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(int16)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$i16_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5933;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(real)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$float_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5934;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(double precision)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$double_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5935;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(numeric)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5936;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(bit)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bit_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5937;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(text)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$text_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5938;
CREATE OR REPLACE FUNCTION pg_catalog.signtype("char")
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$char_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5939;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(character varying)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_signtype$function$;

DROP FUNCTION if EXISTS pg_catalog.signtype(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5940;
CREATE OR REPLACE FUNCTION pg_catalog.signtype(bpchar)
 RETURNS signtype
 LANGUAGE internal
 IMMUTABLE STRICT NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_signtype$function$;

comment on function pg_catalog.signtype(smallint) is 'convert int2 to signtype';
comment on function pg_catalog.signtype(integer) is 'convert int4 to signtype';
comment on function pg_catalog.signtype(boolean) is 'convert bool to signtype';
comment on function pg_catalog.signtype(tinyint) is 'convert int1 to signtype';
comment on function pg_catalog.signtype(bigint) is 'convert int8 to signtype';
comment on function pg_catalog.signtype(int16) is 'convert int16 to signtype';
comment on function pg_catalog.signtype(real) is 'convert float to signtype';
comment on function pg_catalog.signtype(double precision) is 'convert double to signtype';
comment on function pg_catalog.signtype(numeric) is 'convert numeric to signtype';
comment on function pg_catalog.signtype(bit) is 'convert bit to signtype';
comment on function pg_catalog.signtype(text) is 'convert text to signtype';
comment on function pg_catalog.signtype("char") is 'convert char to signtype';
comment on function pg_catalog.signtype(character varying) is 'convert varchar to signtype';
comment on function pg_catalog.signtype(bpchar) is 'convert bpchar to signtype';

comment on function pg_catalog.signtype_in(cstring) is 'I/O';
comment on function pg_catalog.signtype_out(signtype) is 'I/O';
comment on function pg_catalog.signtype_send(signtype) is 'I/O';
comment on function pg_catalog.signtype_recv(internal) is 'I/O';

-- simple_integer casts
DROP FUNCTION if EXISTS pg_catalog.simple_integer(smallint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5944;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(smallint)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i2_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5945;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(integer)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i4_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(boolean) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5946;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(boolean)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bool_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(tinyint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5947;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(tinyint)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i1_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(bigint) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5948;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(bigint)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i8_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(int16) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5949;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(int16)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$i16_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(real) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5950;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(real)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$float_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(double precision) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5951;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(double precision)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$double_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(numeric) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5952;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(numeric)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$numeric_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(bit) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5953;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(bit)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bit_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(text) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5954;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(text)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$text_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer("char") CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5955;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer("char")
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$char_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(character varying) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5956;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(character varying)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$varchar_to_simple_integer$function$;

DROP FUNCTION if EXISTS pg_catalog.simple_integer(bpchar) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_PROC, 5957;
CREATE OR REPLACE FUNCTION pg_catalog.simple_integer(bpchar)
 RETURNS simple_integer
 LANGUAGE internal
 IMMUTABLE NOT FENCED NOT SHIPPABLE
AS $function$bpchar_to_simple_integer$function$;

comment on function pg_catalog.simple_integer(smallint) is 'convert int2 to simple_integer';
comment on function pg_catalog.simple_integer(integer) is 'convert int4 to simple_integer';
comment on function pg_catalog.simple_integer(boolean) is 'convert bool to simple_integer';
comment on function pg_catalog.simple_integer(tinyint) is 'convert int1 to simple_integer';
comment on function pg_catalog.simple_integer(bigint) is 'convert int8 to simple_integer';
comment on function pg_catalog.simple_integer(int16) is 'convert int16 to simple_integer';
comment on function pg_catalog.simple_integer(real) is 'convert float to simple_integer';
comment on function pg_catalog.simple_integer(double precision) is 'convert double to simple_integer';
comment on function pg_catalog.simple_integer(numeric) is 'convert numeric to simple_integer';
comment on function pg_catalog.simple_integer(bit) is 'convert bit to simple_integer';
comment on function pg_catalog.simple_integer(text) is 'convert text to simple_integer';
comment on function pg_catalog.simple_integer("char") is 'convert char to simple_integer';
comment on function pg_catalog.simple_integer(character varying) is 'convert varchar to simple_integer';
comment on function pg_catalog.simple_integer(bpchar) is 'convert bpchar to simple_integer';

comment on function pg_catalog.simple_integer_in(cstring) is 'I/O';
comment on function pg_catalog.simple_integer_out(simple_integer) is 'I/O';
comment on function pg_catalog.simple_integer_send(simple_integer) is 'I/O';
comment on function pg_catalog.simple_integer_recv(internal) is 'I/O';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_mul(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3361;
CREATE FUNCTION pg_catalog.simple_integer_mul(simple_integer, simple_integer)
RETURNS simple_integer LANGUAGE INTERNAL IMMUTABLE as 'simple_integer_mul';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_plus(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3362;
CREATE FUNCTION pg_catalog.simple_integer_plus(simple_integer, simple_integer)
RETURNS simple_integer LANGUAGE INTERNAL IMMUTABLE as 'simple_integer_plus';

DROP FUNCTION IF EXISTS pg_catalog.simple_integer_sub(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_PROC, 3364;
CREATE FUNCTION pg_catalog.simple_integer_sub(simple_integer, simple_integer)
RETURNS simple_integer LANGUAGE INTERNAL IMMUTABLE as 'simple_integer_sub';

comment on function pg_catalog.simple_integer_mul(simple_integer, simple_integer) is 'simple_integer multiply';
comment on function pg_catalog.simple_integer_plus(simple_integer, simple_integer) is 'simple_integer add';
comment on function pg_catalog.simple_integer_sub(simple_integer, simple_integer) is 'simple_integer substract';
--------------------------------------------------------------
-- add pg_cast
--------------------------------------------------------------
SET LOCAL inplace_upgrade_next_system_object_oids=IUO_GENERAL, 0;

DROP CAST IF EXISTS (natural AS integer) CASCADE;
DROP CAST IF EXISTS (naturaln AS integer) CASCADE;
DROP CAST IF EXISTS (positive AS integer) CASCADE;
DROP CAST IF EXISTS (positiven AS integer) CASCADE;
DROP CAST IF EXISTS (signtype AS integer) CASCADE;
DROP CAST IF EXISTS (simple_integer AS integer) CASCADE;

CREATE CAST (natural AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (naturaln AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (positive AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (positiven AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (signtype AS integer) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (simple_integer AS integer) WITHOUT FUNCTION AS IMPLICIT;

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
DROP CAST IF EXISTS (naturaln AS natural) CASCADE;
DROP CAST IF EXISTS (positive AS natural) CASCADE;
DROP CAST IF EXISTS (positiven AS natural) CASCADE;
DROP CAST IF EXISTS (signtype AS natural) CASCADE;
DROP CAST IF EXISTS (simple_integer AS natural) CASCADE;

DROP CAST IF EXISTS (natural AS tinyint) CASCADE;
DROP CAST IF EXISTS (natural AS smallint) CASCADE;
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
DROP CAST IF EXISTS (natural AS naturaln) CASCADE;
DROP CAST IF EXISTS (natural AS positive) CASCADE;
DROP CAST IF EXISTS (natural AS positiven) CASCADE;
DROP CAST IF EXISTS (natural AS signtype) CASCADE;
DROP CAST IF EXISTS (natural AS simple_integer) CASCADE;

CREATE CAST (tinyint AS natural) WITH FUNCTION pg_catalog.natural(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS natural) WITH FUNCTION pg_catalog.natural(smallint) AS IMPLICIT;
CREATE CAST (integer AS natural) WITH FUNCTION pg_catalog.natural(integer) AS IMPLICIT;
CREATE CAST (bigint AS natural) WITH FUNCTION pg_catalog.natural(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS natural) WITH FUNCTION pg_catalog.natural(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS natural) WITH FUNCTION pg_catalog.natural(boolean) AS IMPLICIT;
CREATE CAST (real AS natural) WITH FUNCTION pg_catalog.natural(real) AS IMPLICIT;
CREATE CAST (double precision AS natural) WITH FUNCTION pg_catalog.natural(double precision) AS IMPLICIT;
CREATE CAST (numeric AS natural) WITH FUNCTION pg_catalog.natural(numeric) AS IMPLICIT;
CREATE CAST (bit AS natural) WITH FUNCTION pg_catalog.natural(bit);
CREATE CAST (text AS natural) WITH FUNCTION pg_catalog.natural(text) AS IMPLICIT;
CREATE CAST ("char" AS natural) WITH FUNCTION pg_catalog.natural("char");
CREATE CAST (character varying AS natural) WITH FUNCTION pg_catalog.natural(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS natural) WITH FUNCTION pg_catalog.natural(bpchar) AS IMPLICIT;
CREATE CAST (naturaln AS natural) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (positive AS natural) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (positiven AS natural) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (signtype AS natural) WITH FUNCTION pg_catalog.natural(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS natural) WITH FUNCTION pg_catalog.natural(integer) AS IMPLICIT;

CREATE CAST (natural AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (natural AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (natural AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (natural AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (natural AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (natural AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (natural AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (natural AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (natural AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (natural AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (natural AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (natural AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (natural AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (natural AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (natural AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (natural AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
CREATE CAST (natural AS naturaln) WITH FUNCTION pg_catalog.naturaln(integer) AS IMPLICIT;
CREATE CAST (natural AS positive) WITH FUNCTION pg_catalog.positive(integer) AS IMPLICIT;
CREATE CAST (natural AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;
CREATE CAST (natural AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;
CREATE CAST (natural AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(integer) AS IMPLICIT;

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
DROP CAST IF EXISTS (positive AS naturaln) CASCADE;
DROP CAST IF EXISTS (positiven AS naturaln) CASCADE;
DROP CAST IF EXISTS (signtype AS naturaln) CASCADE;
DROP CAST IF EXISTS (simple_integer AS naturaln) CASCADE;

DROP CAST IF EXISTS (naturaln AS tinyint) CASCADE;
DROP CAST IF EXISTS (naturaln AS smallint) CASCADE;
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
DROP CAST IF EXISTS (naturaln AS positive) CASCADE;
DROP CAST IF EXISTS (naturaln AS positiven) CASCADE;
DROP CAST IF EXISTS (naturaln AS signtype) CASCADE;
DROP CAST IF EXISTS (naturaln AS simple_integer) CASCADE;

CREATE CAST (tinyint AS naturaln) WITH FUNCTION pg_catalog.naturaln(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS naturaln) WITH FUNCTION pg_catalog.naturaln(smallint) AS IMPLICIT;
CREATE CAST (integer AS naturaln) WITH FUNCTION pg_catalog.naturaln(integer) AS IMPLICIT;
CREATE CAST (bigint AS naturaln) WITH FUNCTION pg_catalog.naturaln(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS naturaln) WITH FUNCTION pg_catalog.naturaln(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS naturaln) WITH FUNCTION pg_catalog.naturaln(boolean) AS IMPLICIT;
CREATE CAST (real AS naturaln) WITH FUNCTION pg_catalog.naturaln(real) AS IMPLICIT;
CREATE CAST (double precision AS naturaln) WITH FUNCTION pg_catalog.naturaln(double precision) AS IMPLICIT;
CREATE CAST (numeric AS naturaln) WITH FUNCTION pg_catalog.naturaln(numeric) AS IMPLICIT;
CREATE CAST (bit AS naturaln) WITH FUNCTION pg_catalog.naturaln(bit);
CREATE CAST (text AS naturaln) WITH FUNCTION pg_catalog.naturaln(text) AS IMPLICIT;
CREATE CAST ("char" AS naturaln) WITH FUNCTION pg_catalog.naturaln("char");
CREATE CAST (character varying AS naturaln) WITH FUNCTION pg_catalog.naturaln(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS naturaln) WITH FUNCTION pg_catalog.naturaln(bpchar) AS IMPLICIT;
CREATE CAST (positive AS naturaln) WITH FUNCTION pg_catalog.naturaln(integer) AS IMPLICIT;
CREATE CAST (positiven AS naturaln) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (signtype AS naturaln) WITH FUNCTION pg_catalog.naturaln(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS naturaln) WITH FUNCTION pg_catalog.naturaln(integer) AS IMPLICIT;

CREATE CAST (naturaln AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (naturaln AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (naturaln AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (naturaln AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (naturaln AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (naturaln AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (naturaln AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (naturaln AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (naturaln AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (naturaln AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (naturaln AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (naturaln AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (naturaln AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (naturaln AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (naturaln AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (naturaln AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
CREATE CAST (naturaln AS positive) WITH FUNCTION pg_catalog.positive(integer) AS IMPLICIT;
CREATE CAST (naturaln AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;
CREATE CAST (naturaln AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;
CREATE CAST (naturaln AS simple_integer) WITHOUT FUNCTION AS IMPLICIT;

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
DROP CAST IF EXISTS (positiven AS positive) CASCADE;
DROP CAST IF EXISTS (signtype AS positive) CASCADE;
DROP CAST IF EXISTS (simple_integer AS positive) CASCADE;

DROP CAST IF EXISTS (positive AS tinyint) CASCADE;
DROP CAST IF EXISTS (positive AS smallint) CASCADE;
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
DROP CAST IF EXISTS (positive AS positiven) CASCADE;
DROP CAST IF EXISTS (positive AS signtype) CASCADE;
DROP CAST IF EXISTS (positive AS simple_integer) CASCADE;

CREATE CAST (tinyint AS positive) WITH FUNCTION pg_catalog.positive(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS positive) WITH FUNCTION pg_catalog.positive(smallint) AS IMPLICIT;
CREATE CAST (integer AS positive) WITH FUNCTION pg_catalog.positive(integer) AS IMPLICIT;
CREATE CAST (bigint AS positive) WITH FUNCTION pg_catalog.positive(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS positive) WITH FUNCTION pg_catalog.positive(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS positive) WITH FUNCTION pg_catalog.positive(boolean) AS IMPLICIT;
CREATE CAST (real AS positive) WITH FUNCTION pg_catalog.positive(real) AS IMPLICIT;
CREATE CAST (double precision AS positive) WITH FUNCTION pg_catalog.positive(double precision) AS IMPLICIT;
CREATE CAST (numeric AS positive) WITH FUNCTION pg_catalog.positive(numeric) AS IMPLICIT;
CREATE CAST (bit AS positive) WITH FUNCTION pg_catalog.positive(bit);
CREATE CAST (text AS positive) WITH FUNCTION pg_catalog.positive(text) AS IMPLICIT;
CREATE CAST ("char" AS positive) WITH FUNCTION pg_catalog.positive("char");
CREATE CAST (character varying AS positive) WITH FUNCTION pg_catalog.positive(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS positive) WITH FUNCTION pg_catalog.positive(bpchar) AS IMPLICIT;
CREATE CAST (positiven AS positive) WITHOUT FUNCTION AS IMPLICIT;
CREATE CAST (signtype AS positive) WITH FUNCTION pg_catalog.positive(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS positive) WITH FUNCTION pg_catalog.positive(integer) AS IMPLICIT;

CREATE CAST (positive AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (positive AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (positive AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (positive AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (positive AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (positive AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (positive AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (positive AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (positive AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (positive AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (positive AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (positive AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (positive AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (positive AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (positive AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (positive AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
CREATE CAST (positive AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;
CREATE CAST (positive AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;
CREATE CAST (positive AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(integer) AS IMPLICIT;

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
DROP CAST IF EXISTS (signtype AS positiven) CASCADE;
DROP CAST IF EXISTS (simple_integer AS positiven) CASCADE;

DROP CAST IF EXISTS (positiven AS tinyint) CASCADE;
DROP CAST IF EXISTS (positiven AS smallint) CASCADE;
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
DROP CAST IF EXISTS (positiven AS signtype) CASCADE;
DROP CAST IF EXISTS (positiven AS simple_integer) CASCADE;

CREATE CAST (tinyint AS positiven) WITH FUNCTION pg_catalog.positiven(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS positiven) WITH FUNCTION pg_catalog.positiven(smallint) AS IMPLICIT;
CREATE CAST (integer AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;
CREATE CAST (bigint AS positiven) WITH FUNCTION pg_catalog.positiven(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS positiven) WITH FUNCTION pg_catalog.positiven(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS positiven) WITH FUNCTION pg_catalog.positiven(boolean) AS IMPLICIT;
CREATE CAST (real AS positiven) WITH FUNCTION pg_catalog.positiven(real) AS IMPLICIT;
CREATE CAST (double precision AS positiven) WITH FUNCTION pg_catalog.positiven(double precision) AS IMPLICIT;
CREATE CAST (numeric AS positiven) WITH FUNCTION pg_catalog.positiven(numeric) AS IMPLICIT;
CREATE CAST (bit AS positiven) WITH FUNCTION pg_catalog.positiven(bit);
CREATE CAST (text AS positiven) WITH FUNCTION pg_catalog.positiven(text) AS IMPLICIT;
CREATE CAST ("char" AS positiven) WITH FUNCTION pg_catalog.positiven("char");
CREATE CAST (character varying AS positiven) WITH FUNCTION pg_catalog.positiven(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS positiven) WITH FUNCTION pg_catalog.positiven(bpchar) AS IMPLICIT;
CREATE CAST (signtype AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS positiven) WITH FUNCTION pg_catalog.positiven(integer) AS IMPLICIT;

CREATE CAST (positiven AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (positiven AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (positiven AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (positiven AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (positiven AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (positiven AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (positiven AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (positiven AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (positiven AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (positiven AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (positiven AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (positiven AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (positiven AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (positiven AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (positiven AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (positiven AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
CREATE CAST (positiven AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;
CREATE CAST (positiven AS simple_integer) WITHOUT FUNCTION AS IMPLICIT;

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
DROP CAST IF EXISTS (simple_integer AS signtype) CASCADE;

DROP CAST IF EXISTS (signtype AS tinyint) CASCADE;
DROP CAST IF EXISTS (signtype AS smallint) CASCADE;
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
DROP CAST IF EXISTS (signtype AS simple_integer) CASCADE;

CREATE CAST (tinyint AS signtype) WITH FUNCTION pg_catalog.signtype(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS signtype) WITH FUNCTION pg_catalog.signtype(smallint) AS IMPLICIT;
CREATE CAST (integer AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;
CREATE CAST (bigint AS signtype) WITH FUNCTION pg_catalog.signtype(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS signtype) WITH FUNCTION pg_catalog.signtype(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS signtype) WITH FUNCTION pg_catalog.signtype(boolean) AS IMPLICIT;
CREATE CAST (real AS signtype) WITH FUNCTION pg_catalog.signtype(real) AS IMPLICIT;
CREATE CAST (double precision AS signtype) WITH FUNCTION pg_catalog.signtype(double precision) AS IMPLICIT;
CREATE CAST (numeric AS signtype) WITH FUNCTION pg_catalog.signtype(numeric) AS IMPLICIT;
CREATE CAST (bit AS signtype) WITH FUNCTION pg_catalog.signtype(bit);
CREATE CAST (text AS signtype) WITH FUNCTION pg_catalog.signtype(text) AS IMPLICIT;
CREATE CAST ("char" AS signtype) WITH FUNCTION pg_catalog.signtype("char");
CREATE CAST (character varying AS signtype) WITH FUNCTION pg_catalog.signtype(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS signtype) WITH FUNCTION pg_catalog.signtype(bpchar) AS IMPLICIT;
CREATE CAST (simple_integer AS signtype) WITH FUNCTION pg_catalog.signtype(integer) AS IMPLICIT;

CREATE CAST (signtype AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (signtype AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (signtype AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (signtype AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (signtype AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (signtype AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (signtype AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (signtype AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (signtype AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (signtype AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (signtype AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (signtype AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (signtype AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (signtype AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (signtype AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (signtype AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
CREATE CAST (signtype AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(integer) AS IMPLICIT;

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

CREATE CAST (tinyint AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(tinyint) AS IMPLICIT;
CREATE CAST (smallint AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(smallint) AS IMPLICIT;
CREATE CAST (integer AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(integer) AS IMPLICIT;
CREATE CAST (bigint AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(bigint) AS ASSIGNMENT;
CREATE CAST (int16 AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(int16) AS ASSIGNMENT;
CREATE CAST (boolean AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(boolean) AS IMPLICIT;
CREATE CAST (real AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(real) AS IMPLICIT;
CREATE CAST (double precision AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(double precision) AS IMPLICIT;
CREATE CAST (numeric AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(numeric) AS IMPLICIT;
CREATE CAST (bit AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(bit);
CREATE CAST (text AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(text) AS IMPLICIT;
CREATE CAST ("char" AS simple_integer) WITH FUNCTION pg_catalog.simple_integer("char");
CREATE CAST (character varying AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(character varying) AS IMPLICIT;
CREATE CAST (bpchar AS simple_integer) WITH FUNCTION pg_catalog.simple_integer(bpchar) AS IMPLICIT;

CREATE CAST (simple_integer AS tinyint) WITH FUNCTION pg_catalog.i4toi1(integer) AS ASSIGNMENT;
CREATE CAST (simple_integer AS smallint) WITH FUNCTION pg_catalog.int2(integer) AS ASSIGNMENT;
CREATE CAST (simple_integer AS bigint) WITH FUNCTION pg_catalog.int8(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS int16) WITH FUNCTION pg_catalog.int16(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS boolean) WITH FUNCTION pg_catalog.bool(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS real) WITH FUNCTION pg_catalog.float4(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS double precision) WITH FUNCTION pg_catalog.float8(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS numeric) WITH FUNCTION pg_catalog.numeric(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS bit) WITH FUNCTION pg_catalog.bit(integer, integer);
CREATE CAST (simple_integer AS text) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS "char") WITH FUNCTION pg_catalog.char(integer);
CREATE CAST (simple_integer AS character varying) WITH FUNCTION pg_catalog.int4_varchar(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS bpchar) WITH FUNCTION pg_catalog.int4_bpchar(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS clob) WITH FUNCTION pg_catalog.int4_text(integer) AS IMPLICIT;
CREATE CAST (simple_integer AS money) WITH FUNCTION pg_catalog.money(integer) AS ASSIGNMENT;
CREATE CAST (simple_integer AS interval) WITH FUNCTION pg_catalog.num_to_interval(integer, integer) AS IMPLICIT;
--------------------------------------------------------------
-- add new operator
--------------------------------------------------------------
DROP OPERATOR IF EXISTS pg_catalog.+(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 2200;
CREATE OPERATOR pg_catalog.+(
    leftarg = simple_integer,
    rightarg = simple_integer,
    procedure = simple_integer_plus,
    commutator=operator(pg_catalog.+)
);
COMMENT ON OPERATOR pg_catalog.+(simple_integer, simple_integer) IS 'add';

DROP OPERATOR IF EXISTS pg_catalog.-(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 2201;
CREATE OPERATOR pg_catalog.-(leftarg = simple_integer, rightarg = simple_integer, procedure = simple_integer_sub);
COMMENT ON OPERATOR pg_catalog.-(simple_integer, simple_integer) IS 'substract';

DROP OPERATOR IF EXISTS pg_catalog.*(simple_integer, simple_integer) CASCADE;
SET LOCAL inplace_upgrade_next_system_object_oids = IUO_GENERAL, 2202;
CREATE OPERATOR pg_catalog.*(
    leftarg = simple_integer,
    rightarg = simple_integer,
    procedure = simple_integer_mul,
    commutator=operator(pg_catalog.*)
);
COMMENT ON OPERATOR pg_catalog.*(simple_integer, simple_integer) IS 'multiply';