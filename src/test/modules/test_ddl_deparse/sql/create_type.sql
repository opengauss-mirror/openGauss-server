---
--- CREATE_TYPE
---

CREATE FUNCTION text_w_default_in(cstring)
   RETURNS text_w_default
   AS 'textin'
   LANGUAGE internal STABLE STRICT;

CREATE FUNCTION text_w_default_out(text_w_default)
   RETURNS cstring
   AS 'textout'
   LANGUAGE internal STABLE STRICT ;

CREATE TYPE employee_type AS (name TEXT, salary NUMERIC);

CREATE TYPE enum_test AS ENUM ('foo', 'bar', 'baz');

CREATE TYPE int2range AS RANGE (
  SUBTYPE = int2
);

-- TYPEMOD_GIVEN with typmod -1 must not call printTypmod.
SELECT test_format_type_extended('text'::regtype::oid, -1, 5);

-- Positive typmods are preserved, and ignored when TYPEMOD_GIVEN is absent.
SELECT test_format_type_extended('varchar'::regtype::oid, 104, 5);
SELECT test_format_type_extended('varchar'::regtype::oid, 104, 4);

-- Array formatting must switch the OID and syscache tuple together.
SELECT test_format_type_extended('text[]'::regtype::oid, -1, 5);

-- INVALID_AS_NULL keeps its documented behavior.
SELECT test_format_type_extended(0::oid, -1, 8) IS NULL AS invalid_is_null;
