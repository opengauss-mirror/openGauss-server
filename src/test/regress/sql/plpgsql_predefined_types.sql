CREATE DATABASE test_predefined_pltype;
\c test_predefined_pltype;

--
-- NATURAL
--

CREATE TABLE NATURAL_TBL(f1 natural);

INSERT INTO NATURAL_TBL(f1) VALUES ('   0  ');

INSERT INTO NATURAL_TBL(f1) VALUES ('123456     ');
INSERT INTO NATURAL_TBL(f1) VALUES ('123457 ');

INSERT INTO NATURAL_TBL(f1) VALUES ('');

-- largest and smallest values
INSERT INTO NATURAL_TBL(f1) VALUES ('2147483647');

INSERT INTO NATURAL_TBL(f1) VALUES ('0');

-- bad input values -- should give errors
INSERT INTO NATURAL_TBL(f1) VALUES ('34.5');
INSERT INTO NATURAL_TBL(f1) VALUES ('1000000000000');
INSERT INTO NATURAL_TBL(f1) VALUES ('     ');
INSERT INTO NATURAL_TBL(f1) VALUES ('   asdf   ');
INSERT INTO NATURAL_TBL(f1) VALUES ('- 1234');
INSERT INTO NATURAL_TBL(f1) VALUES ('-1234');
INSERT INTO NATURAL_TBL(f1) VALUES ('123       5');

INSERT INTO NATURAL_TBL (SELECT f1+1 FROM NATURAL_TBL);
INSERT INTO NATURAL_TBL (SELECT f1-1 FROM NATURAL_TBL);

CREATE INDEX idx ON NATURAL_TBL(f1);

SELECT '' AS five, * FROM NATURAL_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM NATURAL_TBL i WHERE i.f1 <> natural '0' ORDER BY f1;

SELECT '' AS one, i.* FROM NATURAL_TBL i WHERE i.f1 = natural '0';

SELECT '' AS two, i.* FROM NATURAL_TBL i WHERE i.f1 < natural '0' ORDER BY f1;

SELECT '' AS three, i.* FROM NATURAL_TBL i WHERE i.f1 <= natural '0' ORDER BY f1;

SELECT '' AS two, i.* FROM NATURAL_TBL i WHERE i.f1 > natural '0' ORDER BY f1;

SELECT '' AS three, i.* FROM NATURAL_TBL i WHERE i.f1 >= natural '0' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM NATURAL_TBL i WHERE (i.f1 % natural '2') = int4 '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM NATURAL_TBL i WHERE (i.f1 % natural '2') = int4 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * natural '2' AS x FROM NATURAL_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + natural '2' AS x FROM NATURAL_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - natural '2' AS x FROM NATURAL_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / natural '2' AS x FROM NATURAL_TBL i ORDER BY f1;

SELECT natural '1000' < natural '999' AS false;

-- divide zero
SELECT (0)::natural / (-2)::int2;
SELECT (0)::natural / (-2)::int8;

-- corner case
SELECT (-1::natural<<31)::text;
SELECT ((-1::natural<<31)+1)::text;

-- cast to natural
SELECT '11'::tinyint::natural;
SELECT '1122'::int2::natural;
SELECT '-1122'::int2::natural;
SELECT '0'::integer::natural;
SELECT '-1'::integer::natural;
SELECT '11100'::int8::natural;
SELECT '-11100'::int8::natural;
SELECT '123'::int16::natural;
SELECT '-123'::int16::natural;
SELECT '32767.4'::float4::natural;
SELECT '-32767.4'::float4::natural;
SELECT '2147483647.4'::float8::natural;
SELECT '-2147483647.4'::float8::natural;
SELECT '34338492.215397047'::numeric(210,10)::natural;
SELECT '-34338492.215397047'::numeric(210,10)::natural;
SELECT B'1101100000000000'::BIT(16)::natural;
SELECT true::natural;
SELECT false::natural;
SELECT '2147483647'::text::natural;
SELECT '-2147483647'::text::natural;
SELECT '7'::"char"::natural;
SELECT '1234'::varchar(4)::natural;
SELECT '-234'::varchar(4)::natural;
SELECT '1234'::bpchar(4)::natural;
SELECT '-234'::bpchar(4)::natural;
SELECT '1234'::nvarchar2(4)::natural;
SELECT '-234'::nvarchar2(4)::natural;

DROP TABLE NATURAL_TBL;
--
-- NATURALN
--

CREATE TABLE NATURALN_TBL(f1 naturaln);

INSERT INTO NATURALN_TBL(f1) VALUES ('   0  ');

INSERT INTO NATURALN_TBL(f1) VALUES ('123456     ');
INSERT INTO NATURALN_TBL(f1) VALUES ('123457 ');


-- largest and smallest values
INSERT INTO NATURALN_TBL(f1) VALUES ('2147483647');

INSERT INTO NATURALN_TBL(f1) VALUES ('0');

-- bad input values -- should give errors
INSERT INTO NATURALN_TBL(f1) VALUES (NULL);
INSERT INTO NATURALN_TBL(f1) VALUES ('34.5');
INSERT INTO NATURALN_TBL(f1) VALUES ('1000000000000');
INSERT INTO NATURALN_TBL(f1) VALUES ('asdf');
INSERT INTO NATURALN_TBL(f1) VALUES ('     ');
INSERT INTO NATURALN_TBL(f1) VALUES ('   asdf   ');
INSERT INTO NATURALN_TBL(f1) VALUES ('- 1234');
INSERT INTO NATURALN_TBL(f1) VALUES ('-1234');
INSERT INTO NATURALN_TBL(f1) VALUES ('123       5');

INSERT INTO NATURALN_TBL (SELECT f1+1 FROM NATURALN_TBL);
INSERT INTO NATURALN_TBL (SELECT f1-1 FROM NATURALN_TBL);

CREATE INDEX idx ON NATURALN_TBL(f1);

SELECT '' AS five, * FROM NATURALN_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM NATURALN_TBL i WHERE i.f1 <> naturaln '0' ORDER BY f1;

SELECT '' AS one, i.* FROM NATURALN_TBL i WHERE i.f1 = naturaln '0';

SELECT '' AS two, i.* FROM NATURALN_TBL i WHERE i.f1 < naturaln '0' ORDER BY f1;

SELECT '' AS three, i.* FROM NATURALN_TBL i WHERE i.f1 <= naturaln '0' ORDER BY f1;

SELECT '' AS two, i.* FROM NATURALN_TBL i WHERE i.f1 > naturaln '0' ORDER BY f1;

SELECT '' AS three, i.* FROM NATURALN_TBL i WHERE i.f1 >= naturaln '0' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM NATURALN_TBL i WHERE (i.f1 % naturaln '2') = int4 '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM NATURALN_TBL i WHERE (i.f1 % naturaln '2') = int4 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * naturaln '2' AS x FROM NATURALN_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + naturaln '2' AS x FROM NATURALN_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - naturaln '2' AS x FROM NATURALN_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / naturaln '2' AS x FROM NATURALN_TBL i ORDER BY f1;

SELECT naturaln '1000' < naturaln '999' AS false;

-- divide zero
SELECT (0)::naturaln / (-2)::int2;
SELECT (0)::naturaln / (-2)::int8;

-- corner case
SELECT (-1::naturaln<<31)::text;
SELECT ((-1::naturaln<<31)+1)::text;

-- cast to naturaln
SELECT NULL::tinyint::naturaln;
SELECT '11'::tinyint::naturaln;
SELECT '1122'::int2::naturaln;
SELECT '-1122'::int2::naturaln;
SELECT '0'::integer::naturaln;
SELECT '-1'::integer::naturaln;
SELECT '11100'::int8::naturaln;
SELECT '-11100'::int8::naturaln;
SELECT '123'::int16::naturaln;
SELECT '-123'::int16::naturaln;
SELECT '32767.4'::float4::naturaln;
SELECT '-32767.4'::float4::naturaln;
SELECT '2147483647.4'::float8::naturaln;
SELECT '-2147483647.4'::float8::naturaln;
SELECT '34338492.215397047'::numeric(210,10)::naturaln;
SELECT '-34338492.215397047'::numeric(210,10)::naturaln;
SELECT B'1101100000000000'::BIT(16)::naturaln;
SELECT true::naturaln;
SELECT false::naturaln;
SELECT '2147483647'::text::naturaln;
SELECT '-2147483647'::text::naturaln;
SELECT '7'::"char"::naturaln;
SELECT '1234'::varchar(4)::naturaln;
SELECT '-234'::varchar(4)::naturaln;
SELECT '1234'::bpchar(4)::naturaln;
SELECT '-234'::bpchar(4)::naturaln;
SELECT '1234'::nvarchar2(4)::naturaln;
SELECT '-234'::nvarchar2(4)::naturaln;

DROP TABLE NATURALN_TBL;

--
-- POSITIVE
--

CREATE TABLE POSITIVE_TBL(f1 positive);

INSERT INTO POSITIVE_TBL(f1) VALUES ('   1  ');

INSERT INTO POSITIVE_TBL(f1) VALUES ('123456     ');
INSERT INTO POSITIVE_TBL(f1) VALUES ('123457 ');

INSERT INTO POSITIVE_TBL(f1) VALUES ('');

-- largest and smallest values
INSERT INTO POSITIVE_TBL(f1) VALUES ('2147483647');

INSERT INTO POSITIVE_TBL(f1) VALUES ('1');

-- bad input values -- should give errors
INSERT INTO POSITIVE_TBL(f1) VALUES ('0');
INSERT INTO POSITIVE_TBL(f1) VALUES ('34.5');
INSERT INTO POSITIVE_TBL(f1) VALUES ('1000000000000');
INSERT INTO POSITIVE_TBL(f1) VALUES ('     ');
INSERT INTO POSITIVE_TBL(f1) VALUES ('   asdf   ');
INSERT INTO POSITIVE_TBL(f1) VALUES ('- 1234');
INSERT INTO POSITIVE_TBL(f1) VALUES ('-1234');
INSERT INTO POSITIVE_TBL(f1) VALUES ('123       5');

INSERT INTO POSITIVE_TBL (SELECT f1+1 FROM POSITIVE_TBL);
INSERT INTO POSITIVE_TBL (SELECT f1-1 FROM POSITIVE_TBL);

CREATE INDEX idx ON POSITIVE_TBL(f1);

SELECT '' AS five, * FROM POSITIVE_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM POSITIVE_TBL i WHERE i.f1 <> positive '1' ORDER BY f1;

SELECT '' AS one, i.* FROM POSITIVE_TBL i WHERE i.f1 = positive '1';

SELECT '' AS two, i.* FROM POSITIVE_TBL i WHERE i.f1 < positive '1' ORDER BY f1;

SELECT '' AS three, i.* FROM POSITIVE_TBL i WHERE i.f1 <= positive '1' ORDER BY f1;

SELECT '' AS two, i.* FROM POSITIVE_TBL i WHERE i.f1 > positive '1' ORDER BY f1;

SELECT '' AS three, i.* FROM POSITIVE_TBL i WHERE i.f1 >= positive '1' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM POSITIVE_TBL i WHERE (i.f1 % positive '2') = int4 '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM POSITIVE_TBL i WHERE (i.f1 % positive '2') = int4 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * positive '2' AS x FROM POSITIVE_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + positive '2' AS x FROM POSITIVE_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - positive '2' AS x FROM POSITIVE_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / positive '2' AS x FROM POSITIVE_TBL i ORDER BY f1;

SELECT positive '1000' < positive '999' AS false;

-- corner case
SELECT (-1::positive<<31)::text;
SELECT ((-1::positive<<31)+1)::text;

-- cast to positive
SELECT '11'::tinyint::positive;
SELECT '1122'::int2::positive;
SELECT '-1122'::int2::positive;
SELECT '1'::integer::positive;
SELECT '0'::integer::positive;
SELECT '11100'::int8::positive;
SELECT '-11100'::int8::positive;
SELECT '123'::int16::positive;
SELECT '-123'::int16::positive;
SELECT '32767.4'::float4::positive;
SELECT '-32767.4'::float4::positive;
SELECT '2147483647.4'::float8::positive;
SELECT '-2147483647.4'::float8::positive;
SELECT '34338492.215397047'::numeric(210,10)::positive;
SELECT '-34338492.215397047'::numeric(210,10)::positive;
SELECT B'1101100000000000'::BIT(16)::positive;
SELECT true::positive;
SELECT false::positive;
SELECT '2147483647'::text::positive;
SELECT '-2147483647'::text::positive;
SELECT '7'::"char"::positive;
SELECT '1234'::varchar(4)::positive;
SELECT '-234'::varchar(4)::positive;
SELECT '1234'::bpchar(4)::positive;
SELECT '-234'::bpchar(4)::positive;
SELECT '1234'::nvarchar2(4)::positive;
SELECT '-234'::nvarchar2(4)::positive;

DROP TABLE POSITIVE_TBL;

--
-- POSITIVEN
--

CREATE TABLE POSITIVEN_TBL(f1 positiven);

INSERT INTO POSITIVEN_TBL(f1) VALUES ('   1  ');

INSERT INTO POSITIVEN_TBL(f1) VALUES ('123456     ');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('123457 ');

-- largest and smallest values
INSERT INTO POSITIVEN_TBL(f1) VALUES ('2147483647');

INSERT INTO POSITIVEN_TBL(f1) VALUES ('1');

-- bad input values -- should give errors
INSERT INTO POSITIVEN_TBL(f1) VALUES (NULL);
INSERT INTO POSITIVEN_TBL(f1) VALUES ('34.5');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('1000000000000');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('asdf');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('     ');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('   asdf   ');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('- 1234');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('-1234');
INSERT INTO POSITIVEN_TBL(f1) VALUES ('123       5');

INSERT INTO POSITIVEN_TBL (SELECT f1+1 FROM POSITIVEN_TBL);
INSERT INTO POSITIVEN_TBL (SELECT f1-1 FROM POSITIVEN_TBL);

CREATE INDEX idx ON POSITIVEN_TBL(f1);

SELECT '' AS five, * FROM POSITIVEN_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM POSITIVEN_TBL i WHERE i.f1 <> positiven '1' ORDER BY f1;

SELECT '' AS one, i.* FROM POSITIVEN_TBL i WHERE i.f1 = positiven '1';

SELECT '' AS two, i.* FROM POSITIVEN_TBL i WHERE i.f1 < positiven '1' ORDER BY f1;

SELECT '' AS three, i.* FROM POSITIVEN_TBL i WHERE i.f1 <= positiven '1' ORDER BY f1;

SELECT '' AS two, i.* FROM POSITIVEN_TBL i WHERE i.f1 > positiven '1' ORDER BY f1;

SELECT '' AS three, i.* FROM POSITIVEN_TBL i WHERE i.f1 >= positiven '1' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM POSITIVEN_TBL i WHERE (i.f1 % positiven '2') = int4 '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM POSITIVEN_TBL i WHERE (i.f1 % positiven '2') = int4 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * positiven '2' AS x FROM POSITIVEN_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + positiven '2' AS x FROM POSITIVEN_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - positiven '2' AS x FROM POSITIVEN_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / positiven '2' AS x FROM POSITIVEN_TBL i ORDER BY f1;

SELECT positiven '1000' < positiven '999' AS false;

-- corner case
SELECT (-1::positiven<<31)::text;
SELECT ((-1::positiven<<31)+1)::text;

-- cast to positiven
SELECT NULL::tinyint::positiven;
SELECT '11'::tinyint::positiven;
SELECT '1122'::int2::positiven;
SELECT '-1122'::int2::positiven;
SELECT '0'::integer::positiven;
SELECT '-1'::integer::positiven;
SELECT '11100'::int8::positiven;
SELECT '-11100'::int8::positiven;
SELECT '123'::int16::positiven;
SELECT '-123'::int16::positiven;
SELECT '32767.4'::float4::positiven;
SELECT '-32767.4'::float4::positiven;
SELECT '2147483647.4'::float8::positiven;
SELECT '-2147483647.4'::float8::positiven;
SELECT '34338492.215397047'::numeric(210,10)::positiven;
SELECT '-34338492.215397047'::numeric(210,10)::positiven;
SELECT B'1101100000000000'::BIT(16)::positiven;
SELECT true::positiven;
SELECT false::positiven;
SELECT '2147483647'::text::positiven;
SELECT '-2147483647'::text::positiven;
SELECT '7'::"char"::positiven;
SELECT '1234'::varchar(4)::positiven;
SELECT '-234'::varchar(4)::positiven;
SELECT '1234'::bpchar(4)::positiven;
SELECT '-234'::bpchar(4)::positiven;
SELECT '1234'::nvarchar2(4)::positiven;
SELECT '-234'::nvarchar2(4)::positiven;

DROP TABLE POSITIVEN_TBL;

--
-- SIGNTYPE
--

CREATE TABLE SIGNTYPE_TBL(f1 signtype);

INSERT INTO SIGNTYPE_TBL(f1) VALUES ('   1  ');

INSERT INTO SIGNTYPE_TBL(f1) VALUES ('-1     ');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('0 ');
INSERT INTO SIGNTYPE_TBL(f1) VALUES (NULL);


-- largest and smallest values
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('1');

INSERT INTO SIGNTYPE_TBL(f1) VALUES ('-1');

-- bad input values -- should give errors
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('3');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('34.5');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('1000000000000');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('asdf');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('     ');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('   asdf   ');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('- 1234');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('-1234');
INSERT INTO SIGNTYPE_TBL(f1) VALUES ('123       5');

INSERT INTO SIGNTYPE_TBL (SELECT f1+1 FROM SIGNTYPE_TBL);
INSERT INTO SIGNTYPE_TBL (SELECT f1-1 FROM SIGNTYPE_TBL);

CREATE INDEX idx ON SIGNTYPE_TBL(f1);

SELECT '' AS five, * FROM SIGNTYPE_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM SIGNTYPE_TBL i WHERE i.f1 <> signtype '1' ORDER BY f1;

SELECT '' AS one, i.* FROM SIGNTYPE_TBL i WHERE i.f1 = signtype '1';

SELECT '' AS two, i.* FROM SIGNTYPE_TBL i WHERE i.f1 < signtype '1' ORDER BY f1;

SELECT '' AS three, i.* FROM SIGNTYPE_TBL i WHERE i.f1 <= signtype '1' ORDER BY f1;

SELECT '' AS two, i.* FROM SIGNTYPE_TBL i WHERE i.f1 > signtype '1' ORDER BY f1;

SELECT '' AS three, i.* FROM SIGNTYPE_TBL i WHERE i.f1 >= signtype '1' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM SIGNTYPE_TBL i WHERE (i.f1 % int4 '2') = signtype '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM SIGNTYPE_TBL i WHERE (i.f1 % int4 '2') = signtype '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * int4 '2' AS x FROM SIGNTYPE_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + int4 '2' AS x FROM SIGNTYPE_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - int4 '2' AS x FROM SIGNTYPE_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / int4 '2' AS x FROM SIGNTYPE_TBL i ORDER BY f1;

-- divide zero
SELECT (0)::signtype / (-2)::int2;
SELECT (0)::signtype / (-2)::int8;

-- cast to signtype
SELECT NULL::tinyint::signtype;
SELECT '1'::tinyint::signtype;
SELECT '2'::tinyint::signtype;
SELECT '-1'::int2::signtype;
SELECT '-2'::int2::signtype;
SELECT '0'::integer::signtype;
SELECT '2'::integer::signtype;
SELECT '1'::int8::signtype;
SELECT '-11'::int8::signtype;
SELECT '-1'::int16::signtype;
SELECT '-123'::int16::signtype;
SELECT '0.4'::float4::signtype;
SELECT '-1.6'::float4::signtype;
SELECT '1.4'::float8::signtype;
SELECT '1.6'::float8::signtype;
SELECT '0.7'::numeric(210,10)::signtype;
SELECT '-1.7'::numeric(210,10)::signtype;
SELECT B'01'::BIT(2)::signtype;
SELECT true::signtype;
SELECT false::signtype;
SELECT '-1'::text::signtype;
SELECT '-2'::text::signtype;
SELECT '1'::varchar(4)::signtype;
SELECT '-2'::varchar(4)::signtype;
SELECT '1'::bpchar(4)::signtype;
SELECT '-2'::bpchar(4)::signtype;
SELECT '1'::nvarchar2(4)::signtype;
SELECT '-2'::nvarchar2(4)::signtype;

DROP TABLE SIGNTYPE_TBL;

CREATE TABLE SIMPLE_INTEGER_TBL(f1 simple_integer);

INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('   0  ');

INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('123456     ');

INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('    -123456');


-- largest and smallest values
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('2147483647');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('-2147483648');

-- bad input values -- should give errors
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('34.5');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('2147483648');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('-2147483649');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('     ');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('   asdf   ');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('- 1234');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES ('123       5');
INSERT INTO SIMPLE_INTEGER_TBL(f1) VALUES (NULL);

CREATE INDEX idx ON SIMPLE_INTEGER_TBL(f1);

SELECT '' AS five, * FROM SIMPLE_INTEGER_TBL ORDER BY f1;

SELECT '' AS four, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 <> simple_integer '0' ORDER BY f1;

SELECT '' AS one, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 = simple_integer '0';

SELECT '' AS two, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 < simple_integer '0' ORDER BY f1;

SELECT '' AS three, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 <= simple_integer '0' ORDER BY f1;

SELECT '' AS two, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 > simple_integer '0' ORDER BY f1;

SELECT '' AS three, i.* FROM SIMPLE_INTEGER_TBL i WHERE i.f1 >= simple_integer '0' ORDER BY f1;

-- positive odds
SELECT '' AS one, i.* FROM SIMPLE_INTEGER_TBL i WHERE (i.f1 % simple_integer '2') = int4 '1' ORDER BY f1;

-- any evens
SELECT '' AS three, i.* FROM SIMPLE_INTEGER_TBL i WHERE (i.f1 % simple_integer '2') = int4 '0' ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 * simple_integer '2' AS x FROM SIMPLE_INTEGER_TBL i
WHERE abs(f1) < 1073741824 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 + simple_integer '2' AS x FROM SIMPLE_INTEGER_TBL i
WHERE f1 < 2147483646 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 - simple_integer '2' AS x FROM SIMPLE_INTEGER_TBL i
WHERE f1 > -2147483647 ORDER BY f1;

SELECT '' AS five, i.f1, i.f1 / simple_integer '2' AS x FROM SIMPLE_INTEGER_TBL i ORDER BY f1;

SELECT (-2147483647)::simple_integer * (-2)::simple_integer;
SELECT (-2147483648)::simple_integer / (-1)::simple_integer;
SELECT (-2147483648)::simple_integer % (-1)::simple_integer;
DROP TABLE SIMPLE_INTEGER_TBL;


-- cast to simple_integer
SELECT '11'::tinyint::simple_integer;
SELECT NULL::tinyint::simple_integer;
SELECT '1122'::int2::simple_integer;
SELECT NULL::int2::simple_integer;
SELECT '0'::integer::simple_integer;
SELECT NULL::integer::simple_integer;
SELECT '11100'::int8::simple_integer;
SELECT NULL::int8::simple_integer;
SELECT '123'::int16::simple_integer;
SELECT NULL::int16::simple_integer;
SELECT '32767.4'::float4::simple_integer;
SELECT NULL::float4::simple_integer;
SELECT '2147483647.4'::float8::simple_integer;
SELECT NULL::float8::simple_integer;
SELECT '34338492.215397047'::numeric(210,10)::simple_integer;
SELECT NULL::numeric(210,10)::simple_integer;
SELECT B'1101100000000000'::BIT(16)::simple_integer;
SELECT NULL::BIT(16)::simple_integer;
SELECT true::simple_integer;
SELECT false::simple_integer;
SELECT '2147483647'::text::simple_integer;
SELECT NULL::text::simple_integer;
SELECT '7'::"char"::simple_integer;
SELECT NULL::"char"::simple_integer;
SELECT '1234'::varchar(4)::simple_integer;
SELECT NULL::varchar(4)::simple_integer;
SELECT '1234'::bpchar(4)::simple_integer;
SELECT NULL::bpchar(4)::simple_integer;
SELECT '1234'::nvarchar2(4)::simple_integer;
SELECT NULL::nvarchar2(4)::simple_integer;

DROP TABLE simple_integer_TBL;

SELECT '-1'::signtype::natural;
select null::positiven;

SELECT 94::natural::naturaln;
SELECT NULL::natural::naturaln;
SELECT 46::natural::positive;
SELECT 0::natural::positive;
SELECT 58::natural::positiven;
SELECT 0::natural::positiven;
SELECT NULL::natural::positiven;
SELECT 1::natural::signtype;
SELECT '-2'::natural::signtype;
SELECT 55::natural::simple_integer;
SELECT NULL::natural::simple_integer;

SELECT 2::naturaln::natural;
SELECT 2::positive::natural;
SELECT 3::positiven::natural;
SELECT 0::signtype::natural;
SELECT '-1'::signtype::natural;
SELECT 1::simple_integer::natural;
SELECT '-2'::simple_integer::natural;

SELECT 10::naturaln::positive;
SELECT 0::naturaln::positive;
SELECT 10::naturaln::positiven;
SELECT 0::naturaln::positiven;
SELECT 1::naturaln::signtype;
SELECT 2::naturaln::signtype;
SELECT 5::naturaln::simple_integer;

SELECT 1::positive::naturaln;
SELECT NULL::positive::naturaln;
SELECT 1::positiven::naturaln;
SELECT 1::signtype::naturaln;
SELECT '-1'::signtype::naturaln;
SELECT NULL::signtype::naturaln;
SELECT 1::simple_integer::naturaln;
SELECT '-3'::simple_integer::naturaln;

SELECT 6::positiven::positive;
SELECT NULL::signtype::positive;
SELECT 0::signtype::positive;
SELECT 7::simple_integer::positive;
SELECT '-7'::simple_integer::positive;

SELECT 65::positive::positiven;
SELECT NULL::positive::positiven;
SELECT 1::positive::signtype;
SELECT 10::positive::signtype;
SELECT 25::positive::simple_integer;
SELECT NULL::positive::simple_integer;

SELECT 1::positiven::signtype;
SELECT 6::positiven::signtype;
SELECT 81::positiven::simple_integer;

SELECT 1::signtype::positiven;
SELECT NULL::signtype::positiven;
SELECT 0::signtype::positiven;
SELECT 5::simple_integer::positiven;
SELECT '-8'::simple_integer::positiven;

SELECT '-1'::signtype::simple_integer;
SELECT NULL::signtype::simple_integer;
SELECT 0::simple_integer::signtype;
SELECT 2::simple_integer::signtype;

DECLARE
    a natural := 5;
    b naturaln := 4;
    c positive := 3;
    d positiven := 2;
    e signtype := 1;
    f simple_integer := 0;
    g integer := 7;
BEGIN
    b := d + c - a;
    c := (b + a + g) / d ;
    d := e * g;
    e := sign(a % f);
    f := g - c * d;
    g := a;
    a := b + NULL;
    raise info 'a: %, b: %, c: %, d: %, e: %, f: %, g: %',
    a, b, c, d, e, f, g;
END;
/

DECLARE
    TYPE parr IS varray(5) of natural;
    TYPE tbl IS table of signtype;
    arr parr;
    t tbl;
BEGIN
    arr(1) := 10;
    t(1) := 1;
    raise info 't(1): %, arr(1): %', t(1), arr(1);
END;
/

CREATE OR REPLACE FUNCTION func1(a positiven) RETURNS positiven AS 
$$
BEGIN
    a := a + 1;
    RETURN a;
END
$$
LANGUAGE 'plpgsql';
SELECT func1(3);
SELECT func1(0);
SELECT func1(NULL);

DROP FUNCTION func1;

\c regression
DROP DATABASE test_predefined_pltype;