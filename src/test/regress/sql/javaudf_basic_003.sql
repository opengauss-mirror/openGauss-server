/*
 * This file is used to test the function of JavaUDF
 * on support limitations, authority limitations, related guc,
 * java type boundary and etc.
 */

---- test autority limitations
---test: normal user can not create javaudf
CREATE USER udf_usr PASSWORD 'GAUSS@123';
GRANT ALL PRIVILEGES ON LANGUAGE java TO udf_usr;

SET SESSION SESSION AUTHORIZATION udf_usr PASSWORD 'GAUSS@123';
CREATE FUNCTION java_addOne_Integer(INTEGER)
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.addOne(java.lang.Integer)'
	IMMUTABLE LANGUAGE java;

---test: sysadmin user can create and use javaudf */
RESET SESSION AUTHORIZATION;
CREATE USER udf_admin PASSWORD 'GAUSS@123' SYSADMIN;
SET SESSION SESSION AUTHORIZATION udf_admin PASSWORD 'GAUSS@123';

CREATE FUNCTION nullOnEven(INTEGER)
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.nullOnEven'
	IMMUTABLE LANGUAGE java;

SELECT nullOnEven(2);

---test: normal user can use granted udf */
GRANT ALL PRIVILEGES ON SCHEMA udf_admin TO udf_usr;
SET SESSION SESSION AUTHORIZATION udf_usr PASSWORD 'GAUSS@123';

SELECT udf_admin.nullOnEven(2);

RESET SESSION AUTHORIZATION;

DROP USER udf_admin CASCADE;
DROP USER udf_usr CASCADE;

DROP SCHEMA javaudf_basic_002 CASCADE;
CREATE SCHEMA javaudf_basic_002;
SET CURRENT_SCHEMA = javaudf_basic_002;

-- support limitation: do not support not fenced JavaUDF
CREATE FUNCTION dummy(INT4)
	RETURNS INT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java NOT FENCED;

CREATE FUNCTION dummy(INT4)
	RETURNS INT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;

ALTER FUNCTION dummy(INT4) NOT FENCED;

-- only has dummy_int2_only(int2) in java, all other should return cannot find java method
CREATE FUNCTION dummy_int2_only(INT2)
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummyShortOnly'
	LANGUAGE java;
SELECT  dummy_int2_only(int4Col::INT4)
FROM javaudf_basic.typeTable
WHERE dummy_int2_only(int4Col::INT4) < 3
ORDER BY 1;

SELECT dummy_int2_only(int8Col::INT8)
FROM javaudf_basic.typeTable
WHERE dummy_int2_only(int8Col::INT8) < 3
ORDER BY 1;

--try transaction
START TRANSACTION;
CREATE FUNCTION dummy_int2(INT2)
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
ROLLBACK;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

START TRANSACTION;
CREATE FUNCTION dummy_int2(INT2)
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
COMMIT;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

START TRANSACTION;
DROP FUNCTION dummy_int2;
ROLLBACK;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2'; 

START TRANSACTION;
DROP FUNCTION dummy_int2;
COMMIT;
SELECT  proname FROM pg_proc WHERE proname = 'dummy_int2';

--test guc UDFWorkerMemHardLimit: should not over 1G
SHOW UDFWorkerMemHardLimit;
SET FencedUDFMemoryLimit = 1048577;

--test on write file: not allowed
CREATE FUNCTION write_file(TEXT)
	RETURNS BOOL
	AS 'huawei.javaudf.basic.BasicJavaUDF.write_file'
	LANGUAGE java;
SELECT  write_file('hello.txt');

--test on showing error massage
CREATE FUNCTION test_error()
	RETURNS TEXT
	AS 'huawei.javaudf.basic.BasicJavaUDF.get_error_msg'
	LANGUAGE java;
SELECT test_error();

--test Java container value boundary
CREATE FUNCTION getShortMin()
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.getShortMin'
	LANGUAGE java;

CREATE FUNCTION getIntegerMin()
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.getIntegerMin'
	LANGUAGE java;

CREATE FUNCTION getLongMin()
	RETURNS INT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getLongMin'
	LANGUAGE java;

CREATE FUNCTION getFloatMin()
	RETURNS FLOAT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.getFloatMin'
	LANGUAGE java;

CREATE FUNCTION getDoubleMin()
	RETURNS FLOAT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getDoubleMin'
	LANGUAGE java;

CREATE FUNCTION getByteMin()
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.getByteMin'
	LANGUAGE java;

CREATE FUNCTION getShortMax()
	RETURNS INT2
	AS 'huawei.javaudf.basic.BasicJavaUDF.getShortMax'
	LANGUAGE java;

CREATE FUNCTION getIntegerMax()
	RETURNS INTEGER
	AS 'huawei.javaudf.basic.BasicJavaUDF.getIntegerMax'
	LANGUAGE java;

CREATE FUNCTION getLongMax()
	RETURNS INT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getLongMax'
	LANGUAGE java;

CREATE FUNCTION getFloatMax()
	RETURNS FLOAT4
	AS 'huawei.javaudf.basic.BasicJavaUDF.getFloatMax'
	LANGUAGE java;

CREATE FUNCTION getDoubleMax()
	RETURNS FLOAT8
	AS 'huawei.javaudf.basic.BasicJavaUDF.getDoubleMax'
	LANGUAGE java;

CREATE FUNCTION getByteMax()
	RETURNS "char"
	AS 'huawei.javaudf.basic.BasicJavaUDF.getByteMax'
	LANGUAGE java;

SELECT getShortMin(), getIntegerMin(), getLongMin(), getFloatMin(), getDoubleMin(), getByteMin();
SELECT getShortMax(), getIntegerMax(), getLongMax(), getFloatMax(), getDoubleMax(), getByteMax();

-- test type: void
CREATE FUNCTION java_retrunVoid()
	RETURNS void
	AS 'huawei.javaudf.basic.BasicJavaUDF.returnVoid'
	LANGUAGE java;
SELECT java_retrunVoid();

--test type: array
CREATE FUNCTION dummy(INT2[])
	RETURNS INT2[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[2,3,4]::INT2[]);

CREATE FUNCTION dummy(INT4[])
	RETURNS INT4[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[4,5,6]::INT4[]);

CREATE FUNCTION dummy(INT8[])
	RETURNS INT8[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[4,5,6]::INT8[]);

CREATE FUNCTION dummy(FLOAT4[])
	RETURNS FLOAT4[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[0.8,0.9,1.0]::FLOAT4[]);

CREATE FUNCTION dummy(FLOAT8[])
	RETURNS FLOAT8[]
	AS 'huawei.javaudf.basic.BasicJavaUDF.dummy'
	LANGUAGE java;
SELECT dummy(ARRAY[0.8,0.9,1.0]::FLOAT8[]);

DROP SCHEMA javaudf_basic_002 CASCADE;
