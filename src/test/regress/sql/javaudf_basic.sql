--support limitation
DROP SCHEMA JAVAUDF_BASIC CASCADE;
CREATE SCHEMA JAVAUDF_BASIC;
SET CURRENT_SCHEMA = JAVAUDF_BASIC;

CREATE FUNCTION dummy(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java not fenced;

CREATE FUNCTION dummy(int4) 
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

ALTER FUNCTION dummy(int4) NOT FENCED;

--test type
---test basic type
CREATE FUNCTION dummy(bool)
	RETURNS bool
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match bool
CREATE FUNCTION dummy(boolean)
	RETURNS boolean
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return true when passing null
CREATE FUNCTION dummyBoolean(boolean)
	RETURNS boolean
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyBoolean(java.lang.Boolean)'
	LANGUAGE java;

CREATE FUNCTION dummy("char")
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(bytea)
	RETURNS bytea
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return java error when passing null
CREATE FUNCTION dummyByte("char")
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyByte(java.lang.Byte)'
	LANGUAGE java;

----should return value
CREATE FUNCTION dummy(int2)
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match int2
CREATE FUNCTION dummy(smallint)
	RETURNS smallint
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyShort(smallint)
	RETURNS smallint
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyShort(java.lang.Short)'
	LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	IMMUTABLE LANGUAGE java;

----default match int4
CREATE FUNCTION dummy(integer)
	RETURNS integer
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyInteger(integer)
	RETURNS integer
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyInteger(java.lang.Integer)'
	LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(int8)
	RETURNS int8
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match int8
CREATE FUNCTION dummy(bigint)
	RETURNS bigint
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyLong(bigint)
	RETURNS bigint
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyLong(java.lang.Long)'
	LANGUAGE java;

----should return value
CREATE FUNCTION dummy(float4)
	RETURNS float4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match float4
CREATE FUNCTION dummy(real)
	RETURNS real
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----return 0 when passing null
CREATE FUNCTION dummyFloat(real)
	RETURNS real
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyFloat(java.lang.Float)'
	LANGUAGE java;

----should return value+1
CREATE FUNCTION dummy(float8)
	RETURNS float8
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match float8
CREATE FUNCTION dummy(double precision)
	RETURNS double precision
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match float8
CREATE FUNCTION dummyDouble(double precision)
	RETURNS double precision
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyDouble(java.lang.Double)'
	LANGUAGE java;

CREATE FUNCTION dummy(time)
	RETURNS time
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(timetz)
	RETURNS timetz
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(timestamp)
	RETURNS timestamp
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match timestamp
CREATE FUNCTION dummy(date)
	RETURNS date
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(timestamptz)
	RETURNS timestamptz
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----the match sequence should be char, text, varchar.
CREATE FUNCTION dummy(text)
	RETURNS text
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(varchar)
	RETURNS varchar
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match varchar
CREATE FUNCTION dummy(character varying)
	RETURNS character varying
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE FUNCTION dummy(char)
	RETURNS char
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

----default match char
CREATE FUNCTION dummy(character)
	RETURNS character
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

CREATE TABLE typeTable(boolCol bool, charCol "char", int2Col int2, int4Col int4, int8Col int8, floatCol float4, doubleCol float8, varcharCol varchar(10), textCol text, timeCol time, timestampCol timestamp)
distribute by hash (int4Col)
;
INSERT INTO typeTable VALUES ( 1, 'a', 1,1,1, 1.0, 1.0, 'abc','abc','1:00:00', '2018-06-01 1:00:00');
INSERT INTO typeTable VALUES ( 0, 'b', 2,2,2, 2.0, 2.0, 'bcd','bcd','2:00:00', '2018-06-02 2:00:00');
INSERT INTO typeTable VALUES ( 1, 'c', 3,3,3, 3.0, 3.0, 'cde','cde','3:00:00', '2018-06-03 3:00:00');
INSERT INTO typeTable VALUES ( 0, 'd', 4,4,4, 4.0, 4.0, 'def','def','4:00:00', '2018-06-04 4:00:00');
INSERT INTO typeTable VALUES ( 1, 'e', 5,5,5, 5.0, 5.0, 'efg','efg','5:00:00', '2018-06-05 5:00:00');

CREATE TABLE boundaryTable(textCol text, charCol "char", int2Col int2, int4Col int4, int8Col int8, floatCol float4, doubleCol float8)
distribute by hash (int4Col);
INSERT INTO boundaryTable VALUES ('null', null, null, null, null, null, null);
INSERT INTO boundaryTable VALUES ('min', 'aa' , -32768, -2147483648 , -9223372036854775808, 10.9999, 20.9999);
INSERT INTO boundaryTable VALUES ('max', 'bb' , 32767, 2147483647, 9223372036854775807, 10.9999, 20.9999);

-- on target list
SELECT  dummy(boolCol), dummy(charCol), dummy(int2Col), dummy(int4Col), dummy(int8Col), dummy(floatCol),dummy(varcharCol), dummy(textCol),dummy(textCol),dummy(timestampCol)
FROM typeTable
WHERE boolCol = true
ORDER BY 3;

SELECT  dummyBoolean(boolCol), dummyByte(charCol::"char"), dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM typeTable
WHERE boolCol = true
ORDER BY 3;

SELECT dummy(to_date('08 Jun 2018', 'DD Mon YYYY'));
SELECT dummy(to_timestamp('08 Jun 2018', 'DD Mon YYYY'));
SELECT dummy(to_timestamp(200120400));
SELECT dummy('b'::bytea);

-- test passing null to Java container through basic and 
----test passing as a basic type

----test passing as a map to Java container
SELECT  dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM boundaryTable
WHERE textCol = 'null';
SELECT  dummyByte(null);

-- test mppdb boundary
SELECT  dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM boundaryTable
WHERE textCol = 'min';

SELECT  dummyShort(int2Col), dummyInteger(int4Col), dummyLong(int8Col), dummyFloat(floatCol), dummyDouble(doubleCol)
FROM boundaryTable
WHERE textCol = 'max';

--on WHERE condition
SELECT  dummy(charCol)
FROM typeTable
WHERE dummy(int4Col) = 3;

SELECT  dummy(charCol)
FROM typeTable
WHERE dummy(int4Col)+dummy(floatCol) < 5
ORDER BY 1;

SELECT  charCol
FROM typeTable
WHERE dummy(charCol) = 'a'
;

-- try type mismatch
--- only has dummy(int2) in mppdb, all other integer should be regarded as int2 and call dummy(int2) in java
DROP FUNCTION dummy(boolean);
DROP FUNCTION dummy(integer);
DROP FUNCTION dummy(bigint);
SELECT  dummy(int4Col::int4)
FROM typeTable
WHERE dummy(int4col::int4) < 3
ORDER BY 1
;
SELECT  dummy(int8Col::int8)
FROM typeTable
WHERE dummy(int8col::int8) < 3
ORDER BY 1
;
--- only has dummy_int2_only(int2) in java, all other should return cannot find java method
CREATE FUNCTION dummy_int2_only(int2)
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyShortOnly'
	LANGUAGE java;
SELECT  dummy_int2_only(int4Col::int4)
FROM typeTable
WHERE dummy_int2_only(int4Col::int4) < 3
ORDER BY 1
;
SELECT  dummy_int2_only(int8Col::int8)
FROM typeTable
WHERE dummy_int2_only(int8Col::int8) < 3
ORDER BY 1
;
DROP FUNCTION dummy_int2_only;

--try transaction
start transaction;
CREATE FUNCTION dummy_int2(int2)
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
rollback;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

start transaction;
CREATE FUNCTION dummy_int2(int2)
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
commit;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

start transaction;
DROP FUNCTION dummy_int2;
rollback;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

start transaction;
DROP FUNCTION dummy_int2;
commit;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

--try volatile
explain verbose
SELECT  charCol
FROM typeTable
WHERE dummy(int4Col) = 3;

--try immutable 
CREATE FUNCTION dummy_immutable(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java IMMUTABLE;
explain verbose
SELECT  charCol
FROM typeTable
WHERE dummy_immutable(int4Col) = 3;

DROP FUNCTION dummy_immutable;

--clear up
---cleared before, so show error
DROP FUNCTION dummy(bool);
DROP FUNCTION dummy(int4);
DROP FUNCTION dummy(int8);
---clear others
DROP FUNCTION dummy(timestamptz);
DROP FUNCTION dummy(timestamp);
DROP FUNCTION dummy(timetz);
DROP FUNCTION dummy(time);
DROP FUNCTION dummy(text);
DROP FUNCTION dummy(varchar);
DROP FUNCTION dummy(bytea);
DROP FUNCTION dummy(char);
DROP FUNCTION dummy("char");
DROP FUNCTION dummy(float4);
DROP FUNCTION dummy(float8);
DROP FUNCTION dummy(int2);
DROP FUNCTION dummyByte("char");
DROP FUNCTION dummyFloat(float4);
DROP FUNCTION dummyDouble(float8);
DROP FUNCTION dummyBoolean(bool);
DROP FUNCTION dummyShort(int2);
DROP FUNCTION dummyInteger(int4);
DROP FUNCTION dummyLong(int8);

--test void
CREATE FUNCTION java_retrunVoid()
	RETURNS void
	AS 'huawei.javaudf.basic.BasicJavaUDF.returnVoid'
	LANGUAGE java;
SELECT java_retrunVoid();
DROP FUNCTION java_retrunVoid;

--test array
CREATE FUNCTION dummy(int2[])
	RETURNS int2[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[2,3,4]::int2[]);
DROP FUNCTION dummy(int2[]);

CREATE FUNCTION dummy(int4[])
	RETURNS int4[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[4,5,6]::int4[]);
DROP FUNCTION dummy(int4[]);

CREATE FUNCTION dummy(int8[])
	RETURNS int8[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[4,5,6]::int8[]);
DROP FUNCTION dummy(int8[]);

CREATE FUNCTION dummy(float4[])
	RETURNS float4[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[0.8,0.9,1.0]::float4[]);
DROP FUNCTION dummy(float4[]);

CREATE FUNCTION dummy(float8[])
	RETURNS float8[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[0.8,0.9,1.0]::float8[]);
DROP FUNCTION dummy(float8[]);

--test Java container value boundary
CREATE FUNCTION getShortMin()
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.getShortMin'
	LANGUAGE java;

CREATE FUNCTION getIntegerMin()
	RETURNS integer
	AS 'huawei.javaudf.basic.BasicJavaUDF.getIntegerMin'
	LANGUAGE java;

CREATE FUNCTION getLongMin()
	RETURNS int8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getLongMin'
	LANGUAGE java;

CREATE FUNCTION getFloatMin()
	RETURNS float4
	AS 'huawei.javaudf.basic.BasicJavaUDF.getFloatMin'
	LANGUAGE java;

CREATE FUNCTION getDoubleMin()
	RETURNS float8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getDoubleMin'
	LANGUAGE java;

CREATE FUNCTION getByteMin()
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.getByteMin'
	LANGUAGE java;

CREATE FUNCTION getShortMax()
	RETURNS int2
	AS 'huawei.javaudf.basic.BasicJavaUDF.getShortMax'
	LANGUAGE java;

CREATE FUNCTION getIntegerMax()
	RETURNS integer
	AS 'huawei.javaudf.basic.BasicJavaUDF.getIntegerMax'
	LANGUAGE java;

CREATE FUNCTION getLongMax()
	RETURNS int8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getLongMax'
	LANGUAGE java;

CREATE FUNCTION getFloatMax()
	RETURNS float4
	AS 'huawei.javaudf.basic.BasicJavaUDF.getFloatMax'
	LANGUAGE java;

CREATE FUNCTION getDoubleMax()
	RETURNS float8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getDoubleMax'
	LANGUAGE java;

CREATE FUNCTION getByteMax()
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.getByteMax'
	LANGUAGE java;

SELECT getShortMin(), getIntegerMin(), getLongMin(), getFloatMin(), getDoubleMin(), getByteMin();
SELECT getShortMax(), getIntegerMax(), getLongMax(), getFloatMax(), getDoubleMax(), getByteMax();

DROP FUNCTION getIntegerMin;
DROP FUNCTION getShortMin;
DROP FUNCTION getLongMin;
DROP FUNCTION getFloatMin;
DROP FUNCTION getDoubleMin;
DROP FUNCTION getByteMin;
DROP FUNCTION getIntegerMax;
DROP FUNCTION getShortMax;
DROP FUNCTION getLongMax;
DROP FUNCTION getFloatMax;
DROP FUNCTION getDoubleMax;
DROP FUNCTION getByteMax;

--test guc UDFWorkerMemHardLimit
show UDFWorkerMemHardLimit;
set FencedUDFMemoryLimit = 1048577;

--test on write file
CREATE FUNCTION write_file(TEXT)
	RETURNS bool
	AS 'huawei.javaudf.basic.BasicJavaUDF.write_file'
	LANGUAGE java;
SELECT  write_file('hello.txt');
DROP FUNCTION write_file;

--test on showing error massage
CREATE FUNCTION test_error()
	RETURNS text
	AS 'huawei.javaudf.basic.BasicJavaUDF.get_error_msg'
	LANGUAGE java;
SELECT test_error();
DROP FUNCTION test_error;

-- Result should be consistant with your $GAUSSHOME/lib/postgresql/java
CREATE FUNCTION java_getSystemProperty(varchar)
	RETURNS varchar
	AS 'java.lang.System.getProperty'
	LANGUAGE java;
--SELECT java_getSystemProperty('javaudf.file.path');
DROP FUNCTION java_getSystemProperty(varchar);

CREATE FUNCTION java_addOne_int(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.addOne'
	IMMUTABLE LANGUAGE java;
SELECT java_addOne_int(10);
DROP FUNCTION java_addOne_int(int4);

CREATE FUNCTION java_addOne_Integer(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.addOne(java.lang.Integer)'
	IMMUTABLE LANGUAGE java;
SELECT java_addOne_Integer(10);
DROP FUNCTION java_addOne_Integer(int4);

CREATE FUNCTION java_addOne_long(int8)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.addOneLong'
	IMMUTABLE LANGUAGE java;
SELECT java_addOne_long(10);
DROP FUNCTION java_addOne_long(int8);

CREATE FUNCTION nullOnEven(int4)
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.nullOnEven'
	IMMUTABLE LANGUAGE java;
SELECT nullOnEven(1);
SELECT nullOnEven(2);
DROP FUNCTION nullOnEven(int4);

CREATE FUNCTION countNulls(int4[])
	RETURNS int4
	AS 'huawei.javaudf.basic.BasicJavaUDF.countNulls(java.lang.Integer[])'
	LANGUAGE java;
SELECT countNulls(ARRAY[null,1,null,2, null]);
DROP FUNCTION countNulls;

--clean up
DROP TABLE typetable;
DROP TABLE boundaryTable;
DROP SCHEMA JAVAUDF_BASIC CASCADE;
