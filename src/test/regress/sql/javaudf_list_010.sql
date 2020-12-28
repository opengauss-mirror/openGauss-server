DROP SCHEMA bicoredata_010 CASCADE;
CREATE SCHEMA bicoredata_010;
SET CURRENT_SCHEMA = bicoredata_010;

--!FUNC_141 GetOperators dependent file

--FUNC_142
CREATE FUNCTION userId(int8)
	RETURNS int4
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.Long)'
	LANGUAGE java;
SELECT userId (1243401);

CREATE FUNCTION userId(text)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate'
	LANGUAGE java;
SELECT userId ('a124343');

CREATE FUNCTION userId(text, text)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate'
	LANGUAGE java;
SELECT userId ('ASD123','a124343');

CREATE FUNCTION userId(text, text, int4)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT userId ('ASD123','a124343',2);

CREATE FUNCTION userId(text, int4)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT userId ('a124343',2);

--FUNC_143
CREATE FUNCTION GetHiCloudAppId(varchar)
	RETURNS varchar
	AS 'com.huawei.platform.bi.udf.service.hispace.GetHiCloudAppId.evaluate'
	LANGUAGE java;
SELECT getHiCloudAppId('1000123');

--FUNC_144
CREATE FUNCTION GetPhoneBrandUDF(text)
	RETURNS text
	AS 'com.huawei.udf.GetPhoneBrandUDF.evaluate'
	LANGUAGE java;
SELECT GetPhoneBrandUDF('HUAWEI P10') ;

--FUNC_145
CREATE FUNCTION CloudPlusAes(text)
	RETURNS text
	AS 'com.huawei.udf.CloudPlusAes.evaluate'
	LANGUAGE java;
SELECT CloudPlusAes('ad9baacdefaa91afd8eae7cef52e39d3');

--FUNC_146
CREATE FUNCTION QUOTA(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfQuota.evaluate'
	LANGUAGE java;
SELECT QUOTA(ARRAY['{a:"1"}']);

--FUNC_147
CREATE FUNCTION GetDistanceByGPS(float8,float8,float8,float8)
	RETURNS float8
	AS 'com.huawei.udf.GetDistanceByGPS.evaluate(java.lang.Double, java.lang.Double,java.lang.Double, java.lang.Double)'
	LANGUAGE java;
SELECT GetDistanceByGPS(0.1, 0.2, 0.3, 0.4);
SELECT GetDistanceByGPS(NULL,NULL,NULL,NULL) IS NULL;

--FUNC148
CREATE FUNCTION InsertSQLFactory(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.InsertSQLFactoryUDF.evaluate'
	LANGUAGE java;
SELECT insertSQLFactory(ARRAY['dual','value1']);

--FUNC_149
CREATE FUNCTION AESDeCryptUtil(text)
	RETURNS text
	AS 'com.huawei.udf.AESDeCryptUtilUDF.evaluate'
	LANGUAGE java;
SELECT AESDeCryptUtil('E00BED28C373A83F9EBC5FD565A561A0');

--!FUNC_150 explodeString UDTF

--FUNC_151
CREATE FUNCTION udf_md5(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfMd5.evaluate'
	LANGUAGE java;
SELECT udf_md5(ARRAY['abc']);
SELECT udf_md5(ARRAY['a,b,c',',']);
SELECT udf_md5(array['a,b,c',',','-']);

DROP SCHEMA bicoredata_010 CASCADE;
