create schema hll_hash;
set current_schema = hll_hash;

--------------CONTENTS--------------------
-- hyperloglog hash function test cases
------------------------------------------
--1. boolean
--2. smallint integer bigint
--3. bytea text
--4. hll_hash_any
------------------------------------------

------------------------------------------
-- 1. boolean
------------------------------------------
SELECT hll_hash_boolean(FALSE);
SELECT hll_hash_boolean(0);
SELECT hll_hash_boolean('f');
SELECT hll_hash_boolean(FALSE,123);
SELECT hll_hash_boolean(TRUE);
SELECT hll_hash_boolean('t');
SELECT hll_hash_boolean(1);
SELECT hll_hash_boolean(10);
SELECT hll_hash_boolean(TRUE,123);

------------------------------------------
-- 2. smallint integer bigint
------------------------------------------
SELECT hll_hash_smallint(0::smallint);
SELECT hll_hash_smallint(100::smallint);
SELECT hll_hash_smallint(-100::smallint);
SELECT hll_hash_smallint(-100::smallint,123);

SELECT hll_hash_integer(0);
SELECT hll_hash_integer(100);
SELECT hll_hash_integer(-100);
SELECT hll_hash_integer(21474836,123);

SELECT hll_hash_bigint(0);
SELECT hll_hash_bigint(100);
SELECT hll_hash_bigint(-100);
SELECT hll_hash_bigint(223372036854775808,123);
------------------------------------------
-- 3. bytea text
------------------------------------------

SELECT hll_hash_bytea(E'\\x');
SELECT hll_hash_bytea(E'\\x41');
SELECT hll_hash_bytea(E'\\x42');
SELECT hll_hash_bytea(E'\\x4142');
SELECT hll_hash_bytea(E'\\x4142',123);

SELECT hll_hash_text('');
SELECT hll_hash_text('A');
SELECT hll_hash_text('B');
SELECT hll_hash_text('AB');
SELECT hll_hash_text('AB',123);
SELECT hll_hash_text(
'过零丁洋 - 百度汉语
作者：文天祥
辛苦遭逢起一经，干戈寥落四周星。
山河破碎风飘絮，身世浮沉雨打萍。
惶恐滩头说惶恐，零丁洋里叹零丁。
人生自古谁无死？留取丹心照汗青。');

------------------------------------------
-- 4. hll_hash_any
------------------------------------------
--- Check hash and hash_any function results match

SELECT hll_hash_boolean(FALSE) = hll_hash_any(FALSE);
SELECT hll_hash_boolean(FALSE) = hll_hash_any(FALSE,123);
SELECT hll_hash_boolean(TRUE) = hll_hash_any(TRUE);
SELECT hll_hash_boolean(TRUE) = hll_hash_any(TRUE,123);

SELECT hll_hash_smallint(0::smallint) = hll_hash_any(0::smallint);
SELECT hll_hash_smallint(0::smallint) = hll_hash_any(0::smallint,123);
SELECT hll_hash_integer(100) = hll_hash_any(100);
SELECT hll_hash_integer(100) = hll_hash_any(100,123);
SELECT hll_hash_bigint(-100) = hll_hash_any(-100::bigint);
SELECT hll_hash_bigint(-100) = hll_hash_any(-100::bigint,123);

SELECT hll_hash_bytea(E'\\x') = hll_hash_any(E'\\x'::bytea);
SELECT hll_hash_bytea(E'\\x4142') = hll_hash_any(E'\\x4142'::bytea);
SELECT hll_hash_bytea(E'\\x4142') = hll_hash_any(E'\\x4142'::bytea,123);
SELECT hll_hash_text('') = hll_hash_any(''::text);
SELECT hll_hash_text('AB') = hll_hash_any('AB'::text);
SELECT hll_hash_text('AB') = hll_hash_any('AB'::text,123);

--- Check several types not handled by default hash functions
--- macaddr
SELECT hll_hash_any('08:00:2b:01:02:03'::macaddr);
SELECT hll_hash_any('08002b010203'::macaddr);
SELECT hll_hash_any('01-23-45-67-89-ab'::macaddr);
SELECT hll_hash_any('012345-6789ab'::macaddr);

--- interval
SELECT hll_hash_any('1 year 2 months 3 days 4 hours 5 minutes 6seconds'::interval);
SELECT hll_hash_any('P1Y2M3DT4H5M6S'::interval);
SELECT hll_hash_any('1997-06 20 12:00:00'::interval);
SELECT hll_hash_any('P1997-06-20T12:00:00'::interval);

-- test negative seed value
SELECT hll_hash_any(1,-1);
SELECT hll_hash_smallint(1,-1);
SELECT hll_hash_bigint(1,-1);
SELECT hll_hash_integer(1,-1);
SELECT hll_hash_boolean(FALSE,-1);
SELECT hll_hash_bytea(E'\\x',-1);
SELECT hll_hash_text('AB',-1);

-- final cleaning up
drop schema hll_hash cascade;
reset current_schema;
