/*
 * This file is used to test the function of JavaUDF
 * on parameter types and boundaries.
 */
DROP SCHEMA javaudf_parameters;
CREATE SCHEMA javaudf_parameters;
SET CURRENT_SCHEMA = javaudf_parameters;

--test type: basic type
CREATE FUNCTION dummy(BOOL)
	RETURNS BOOL
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match bool
CREATE FUNCTION dummy(BOOLEAN)
	RETURNS BOOLEAN
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return true when passing null
CREATE FUNCTION dummyBoolean(BOOLEAN)
	RETURNS BOOLEAN
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyBoolean(java.lang.Boolean)'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy("char")
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(BYTEA)
	RETURNS BYTEA
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return java error when passing null
CREATE FUNCTION dummyByte("char")
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyByte(java.lang.Byte)'
	IMMUTABLE LANGUAGE java;

----should return value
CREATE FUNCTION dummy(INT2)
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match int2
CREATE FUNCTION dummy(SMALLINT)
	RETURNS SMALLINT
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyShort(SMALLINT)
	RETURNS SMALLINT
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyShort(java.lang.Short)'
	IMMUTABLE LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(INT4)
	RETURNS INT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match int4
CREATE FUNCTION dummy(INTEGER)
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyInteger(INTEGER)
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyInteger(java.lang.Integer)'
	IMMUTABLE LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(INT8)
	RETURNS INT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match int8
CREATE FUNCTION dummy(BIGINT)
	RETURNS BIGINT
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyLong(BIGINT)
	RETURNS BIGINT
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyLong(java.lang.Long)'
	IMMUTABLE LANGUAGE java;

----should return value
CREATE FUNCTION dummy(FLOAT4)
	RETURNS FLOAT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match float4
CREATE FUNCTION dummy(REAL)
	RETURNS REAL
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyFloat(REAL)
	RETURNS REAL
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyFloat(java.lang.Float)'
	IMMUTABLE LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(FLOAT8)
	RETURNS FLOAT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match float8
CREATE FUNCTION dummy(DOUBLE PRECISION)
	RETURNS DOUBLE PRECISION
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match float8
CREATE FUNCTION dummyDouble(DOUBLE PRECISION)
	RETURNS DOUBLE PRECISION
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyDouble(java.lang.Double)'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(TIME)
	RETURNS TIME
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(TIMETZ)
	RETURNS TIMETZ
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(TIMESTAMP)
	RETURNS TIMESTAMP
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match timestamp
CREATE FUNCTION dummy(DATE)
	RETURNS DATE
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(TIMESTAMPTZ)
	RETURNS TIMESTAMPTZ
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----the match sequence should be char, text, varchar.
CREATE FUNCTION dummy(TEXT)
	RETURNS TEXT
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(VARCHAR)
	RETURNS VARCHAR
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match varchar
CREATE FUNCTION dummy(CHARACTER VARYING)
	RETURNS CHARACTER VARYING
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

CREATE FUNCTION dummy(CHAR)
	RETURNS CHAR
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match char
CREATE FUNCTION dummy(CHARACTER)
	RETURNS CHARACTER
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

--on target list
SELECT dummy(boolCol), dummy(charCol), dummy(int2Col), dummy(int4Col), dummy(int8Col), dummy(floatCol), dummy(varcharCol), dummy(textCol), dummy(textCol), dummy(timestampCol)
FROM javaudf_basic.typeTable
WHERE boolCol = true
ORDER BY 3;

SELECT dummyBoolean(boolCol), dummyByte(charCol::"char"), dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM javaudf_basic.typeTable
WHERE boolCol = true
ORDER BY 3;

SELECT dummy(to_date('08 Jun 2018', 'DD Mon YYYY'));
SELECT dummy(to_timestamp('08 Jun 2018', 'DD Mon YYYY'));
SELECT dummy(to_timestamp(200120400));
SELECT dummy('b'::BYTEA);

----test passing as a map to Java container
SELECT dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM javaudf_basic.boundaryTable
WHERE textCol = 'null';
SELECT dummyByte(null);

-- test mppdb boundary
SELECT  dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM javaudf_basic.boundaryTable
WHERE textCol = 'min';

SELECT dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM javaudf_basic.boundaryTable
WHERE textCol = 'max';

-- try type mismatch
--- only has dummy(int2) in mppdb, all other integer should be regarded as int2 and call dummy(int2) in java
DROP FUNCTION dummy(BOOLEAN);
DROP FUNCTION dummy(INTEGER);
DROP FUNCTION dummy(BIGINT);
SELECT dummy(int4Col::INT4)
FROM javaudf_basic.typeTable
WHERE dummy(int4col::INT4) < 3
ORDER BY 1;

SELECT dummy(int8Col::INT8)
FROM javaudf_basic.typeTable
WHERE dummy(int8col::INT8) < 3
ORDER BY 1;

--on WHERE condition
SELECT dummy(charCol)
FROM javaudf_basic.typeTable
WHERE dummy(int4Col) = 3;

SELECT dummy(charCol)
FROM javaudf_basic.typeTable
WHERE dummy(int4Col)+dummy(floatCol) < 5
ORDER BY 1;

SELECT charCol
FROM javaudf_basic.typeTable
WHERE dummy(charCol) = 'a';

--try volatile
explain verbose
SELECT charCol
FROM javaudf_basic.typeTable
WHERE dummy(int4Col) = 3;

--try immutable
CREATE FUNCTION dummy_immutable(INT4)
	RETURNS INT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java IMMUTABLE;
explain verbose
SELECT charCol
FROM javaudf_basic.typeTable
WHERE dummy_immutable(int4Col) = 3;

-- clear up
DROP SCHEMA javaudf_parameters CASCADE;
